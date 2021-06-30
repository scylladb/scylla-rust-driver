use crate::frame::response::event::Event;
use crate::routing::Token;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig, PoolSize};
use crate::transport::errors::QueryError;
use crate::transport::session::IntoTypedRows;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

/// Allows to read current topology info from the cluster
pub struct TopologyReader {
    connection_config: ConnectionConfig,
    control_connection_address: SocketAddr,
    control_connection: NodeConnectionPool,

    // when control connection fails, TopologyReader tries to connect to one of known_peers
    known_peers: Vec<SocketAddr>,
}

/// Describes all topology information retrieved from the cluster
pub struct TopologyInfo {
    pub peers: Vec<Peer>,
    pub keyspaces: HashMap<String, Keyspace>,
}

pub struct Peer {
    pub address: SocketAddr,
    pub tokens: Vec<Token>,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
    pub strategy: Strategy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum Strategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        // Replication factors of datacenters with given names
        datacenter_repfactors: HashMap<String, usize>,
    },
    LocalStrategy, // replication_factor == 1
    Other {
        name: String,
        data: HashMap<String, String>,
    },
}

impl TopologyReader {
    /// Creates new TopologyReader, which connects to known_peers in the background
    pub fn new(
        known_peers: &[SocketAddr],
        mut connection_config: ConnectionConfig,
        server_event_sender: mpsc::Sender<Event>,
    ) -> Self {
        let control_connection_address = *known_peers
            .choose(&mut thread_rng())
            .expect("Tried to initialize TopologyReader with empty known_peers list!");

        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        connection_config.event_sender = Some(server_event_sender);

        let control_connection = Self::make_control_connection_pool(
            control_connection_address,
            connection_config.clone(),
        );

        TopologyReader {
            control_connection_address,
            control_connection,
            connection_config,
            known_peers: known_peers.into(),
        }
    }

    /// Fetches current topology info from the cluster
    pub async fn read_topology_info(&mut self) -> Result<TopologyInfo, QueryError> {
        let mut result = self.fetch_topology_info().await;
        if let Ok(topology_info) = result {
            self.update_known_peers(&topology_info);
            return Ok(topology_info);
        }

        // shuffle known_peers to iterate through them in random order later
        self.known_peers.shuffle(&mut thread_rng());

        let address_of_failed_control_connection = self.control_connection_address;
        let filtered_known_peers = self
            .known_peers
            .iter()
            .filter(|&peer| peer != &address_of_failed_control_connection);

        // if fetching topology info on current control connection failed,
        // try to fetch topology info from other known peer
        for peer in filtered_known_peers {
            let err = match result {
                Ok(_) => break,
                Err(err) => err,
            };

            warn!(
                control_connection_address = self.control_connection_address.to_string().as_str(),
                error = err.to_string().as_str(),
                "Falied to fetch topology info using current control connection"
            );

            self.control_connection_address = *peer;
            self.control_connection = Self::make_control_connection_pool(
                self.control_connection_address,
                self.connection_config.clone(),
            );

            result = self.fetch_topology_info().await;
        }

        match &result {
            Ok(topology_info) => {
                self.update_known_peers(topology_info);
                debug!("Fetched new topology info");
            }
            Err(error) => error!(
                error = error.to_string().as_str(),
                "Could not fetch topology info"
            ),
        }

        result
    }

    async fn fetch_topology_info(&self) -> Result<TopologyInfo, QueryError> {
        // TODO: Timeouts?

        query_topology_info(
            &self.control_connection,
            self.control_connection_address.port(),
        )
        .await
    }

    fn update_known_peers(&mut self, topology_info: &TopologyInfo) {
        self.known_peers = topology_info
            .peers
            .iter()
            .map(|peer| peer.address)
            .collect();
    }

    fn make_control_connection_pool(
        addr: SocketAddr,
        connection_config: ConnectionConfig,
    ) -> NodeConnectionPool {
        let pool_config = PoolConfig {
            connection_config,

            // We want to have only one connection to receive events from
            pool_size: PoolSize::PerHost(NonZeroUsize::new(1).unwrap()),

            // The shard-aware port won't be used with PerHost pool size anyway,
            // so explicitly disable it here
            can_use_shard_aware_port: false,
        };

        NodeConnectionPool::new(addr.ip(), addr.port(), pool_config, None)
    }
}

async fn query_topology_info(
    pool: &NodeConnectionPool,
    connect_port: u16,
) -> Result<TopologyInfo, QueryError> {
    pool.wait_until_initialized().await;
    let conn: &Connection = &*pool.random_connection()?;

    let peers_query = query_peers(conn, connect_port);
    let keyspaces_query = query_keyspaces(conn);

    let (peers, keyspaces) = tokio::try_join!(peers_query, keyspaces_query)?;

    // There must be at least one peer
    if peers.is_empty() {
        return Err(QueryError::ProtocolError(
            "Bad TopologyInfo: peers list is empty",
        ));
    }

    // At least one peer has to have some tokens
    if peers.iter().all(|peer| peer.tokens.is_empty()) {
        return Err(QueryError::ProtocolError(
            "Bad TopoologyInfo: All peers have empty token list",
        ));
    }

    Ok(TopologyInfo { peers, keyspaces })
}

async fn query_peers(conn: &Connection, connect_port: u16) -> Result<Vec<Peer>, QueryError> {
    // There shouldn't be more peers than a single page capacity
    let peers_query = conn.query_single_page(
        "select peer, data_center, rack, tokens from system.peers",
        &[],
    );
    let local_query = conn.query_single_page(
        "select rpc_address, data_center, rack, tokens from system.local",
        &[],
    );

    let (peers_res, local_res) = tokio::try_join!(peers_query, local_query)?;

    let peers_rows = peers_res.rows.ok_or(QueryError::ProtocolError(
        "system.peers query response was not Rows",
    ))?;

    let local_rows = local_res.rows.ok_or(QueryError::ProtocolError(
        "system.local query response was not Rows",
    ))?;

    let mut result: Vec<Peer> = Vec::with_capacity(peers_rows.len() + 1);

    let typed_peers_rows =
        peers_rows.into_typed::<(IpAddr, Option<String>, Option<String>, Option<Vec<String>>)>();

    // For the local node we should use connection's address instead of rpc_address unless SNI is enabled (TODO)
    // Replace address in local_rows with connection's address
    let local_address: IpAddr = conn.get_connect_address().ip();
    let typed_local_rows = local_rows
        .into_typed::<(IpAddr, Option<String>, Option<String>, Option<Vec<String>>)>()
        .map(|res| res.map(|(_addr, dc, rack, tokens)| (local_address, dc, rack, tokens)));

    for row in typed_peers_rows.chain(typed_local_rows) {
        let (ip_address, datacenter, rack, tokens) = row.map_err(|_| {
            QueryError::ProtocolError("system.peers or system.local has invalid column type")
        })?;

        let tokens_str: Vec<String> = tokens.unwrap_or_default();

        let address = SocketAddr::new(ip_address, connect_port);

        // Parse string representation of tokens as integer values
        let tokens: Vec<Token> = tokens_str
            .iter()
            .map(|s| Token::from_str(s))
            .collect::<Result<Vec<Token>, _>>()
            .map_err(|_| QueryError::ProtocolError("Couldn't parse tokens as integer values"))?;

        result.push(Peer {
            address,
            tokens,
            datacenter,
            rack,
        });
    }

    Ok(result)
}

async fn query_keyspaces(conn: &Connection) -> Result<HashMap<String, Keyspace>, QueryError> {
    let rows = conn
        .query_single_page(
            "select keyspace_name, toJson(replication) from system_schema.keyspaces",
            &[],
        )
        .await?
        .rows
        .ok_or(QueryError::ProtocolError(
            "system_schema.keyspaces query response was not Rows",
        ))?;

    let mut result = HashMap::with_capacity(rows.len());

    for row in rows.into_typed::<(String, String)>() {
        let (keyspace_name, keyspace_json_text) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.keyspaces has invalid column type")
        })?;

        let strategy_map: HashMap<String, String> = json_to_string_map(&keyspace_json_text)?;

        let strategy: Strategy = strategy_from_string_map(strategy_map)?;

        result.insert(keyspace_name, Keyspace { strategy });
    }

    Ok(result)
}

fn json_to_string_map(json_text: &str) -> Result<HashMap<String, String>, QueryError> {
    use serde_json::Value;

    let json: Value = serde_json::from_str(json_text)
        .map_err(|_| QueryError::ProtocolError("Couldn't parse keyspaces as json"))?;

    let object_map = match json {
        Value::Object(map) => map,
        _ => {
            return Err(QueryError::ProtocolError(
                "keyspaces map json is not a json object",
            ))
        }
    };

    let mut result = HashMap::with_capacity(object_map.len());

    for (key, val) in object_map.into_iter() {
        match val {
            Value::String(string) => result.insert(key, string),
            _ => {
                return Err(QueryError::ProtocolError(
                    "json keyspaces map does not contain strings",
                ))
            }
        };
    }

    Ok(result)
}

fn strategy_from_string_map(
    mut strategy_map: HashMap<String, String>,
) -> Result<Strategy, QueryError> {
    let strategy_name: String = strategy_map
        .remove("class")
        .ok_or(QueryError::ProtocolError(
            "strategy map should have a 'class' field",
        ))?;

    let strategy: Strategy = match strategy_name.as_str() {
        "org.apache.cassandra.locator.SimpleStrategy" => {
            let rep_factor_str: String =
                strategy_map
                    .remove("replication_factor")
                    .ok_or(QueryError::ProtocolError(
                        "SimpleStrategy in strategy map does not have a replication factor",
                    ))?;

            let replication_factor: usize = usize::from_str(&rep_factor_str).map_err(|_| {
                QueryError::ProtocolError("Could not parse replication factor as an integer")
            })?;

            Strategy::SimpleStrategy { replication_factor }
        }
        "org.apache.cassandra.locator.NetworkTopologyStrategy" => {
            let mut datacenter_repfactors: HashMap<String, usize> =
                HashMap::with_capacity(strategy_map.len());

            for (datacenter, rep_factor_str) in strategy_map.drain() {
                let rep_factor: usize = match usize::from_str(&rep_factor_str) {
                    Ok(number) => number,
                    Err(_) => continue, // There might be other things in the map, we care only about rep_factors
                };

                datacenter_repfactors.insert(datacenter, rep_factor);
            }

            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }
        }
        "org.apache.cassandra.locator.LocalStrategy" => Strategy::LocalStrategy,
        _ => Strategy::Other {
            name: strategy_name,
            data: strategy_map,
        },
    };

    Ok(strategy)
}
