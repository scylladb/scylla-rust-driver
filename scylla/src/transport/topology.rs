use crate::routing::Token;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_keeper::ConnectionKeeper;
use crate::transport::errors::QueryError;
use crate::transport::session::IntoTypedRows;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

/// Allows to read current topology info from the cluster
pub struct TopologyReader {
    control_connections: HashMap<SocketAddr, ConnectionKeeper>,
    connect_port: u16,
    connection_config: ConnectionConfig,
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
    pub fn new(known_peers: &[SocketAddr], connection_config: ConnectionConfig) -> Self {
        let connect_port: u16 = known_peers
            .first()
            .expect("Tried to initialize TopologyReader with empty known_peers list!")
            .port();

        let mut control_connections: HashMap<SocketAddr, ConnectionKeeper> =
            HashMap::with_capacity(known_peers.len());

        for address in known_peers {
            control_connections.insert(
                *address,
                ConnectionKeeper::new(*address, connection_config.clone(), None, None),
            );
        }

        TopologyReader {
            control_connections,
            connect_port,
            connection_config,
        }
    }

    /// Fetches current topology info from the cluster
    pub async fn read_topology_info(&mut self) -> Result<TopologyInfo, QueryError> {
        let result = self.fetch_topology_info().await;

        if let Ok(topology_info) = &result {
            self.update_control_connections(topology_info);
        }

        result
    }

    async fn fetch_topology_info(&self) -> Result<TopologyInfo, QueryError> {
        // TODO: Timeouts? New attempts in parallel?

        let mut last_error: QueryError = QueryError::ProtocolError(
            "Invariant broken - TopologyReader control_connections not empty",
        );

        for conn in self.control_connections.values() {
            match query_topology_info(conn, self.connect_port).await {
                Ok(info) => return Ok(info),
                Err(e) => last_error = e,
            };
        }

        Err(last_error)
    }

    fn update_control_connections(&mut self, topology_info: &TopologyInfo) {
        let mut new_control_connections: HashMap<SocketAddr, ConnectionKeeper> =
            HashMap::with_capacity(topology_info.peers.len());

        for peer in &topology_info.peers {
            let cur_connection = self
                .control_connections
                .remove(&peer.address)
                .unwrap_or_else(|| {
                    ConnectionKeeper::new(peer.address, self.connection_config.clone(), None, None)
                });

            new_control_connections.insert(peer.address, cur_connection);
        }

        self.control_connections = new_control_connections;
    }
}

async fn query_topology_info(
    conn_keeper: &ConnectionKeeper,
    connect_port: u16,
) -> Result<TopologyInfo, QueryError> {
    let conn: &Connection = &*conn_keeper.get_connection().await?;

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

    let peers_rows = peers_res.ok_or(QueryError::ProtocolError(
        "system.peers query response was not Rows",
    ))?;

    let local_rows = local_res.ok_or(QueryError::ProtocolError(
        "system.local query response was not Rows",
    ))?;

    let mut result: Vec<Peer> = Vec::with_capacity(peers_rows.len() + 1);

    let typed_peers_rows =
        peers_rows.into_typed::<(IpAddr, Option<String>, Option<String>, Option<Vec<String>>)>();
    let typed_local_rows =
        local_rows.into_typed::<(IpAddr, Option<String>, Option<String>, Option<Vec<String>>)>();

    // Here we are considering the local node to be just a regular peer, but datastax claims that unless
    // you have SNI enabled, you shouldn't trust rpc_address (So they just use the initial connection's address)
    // we could expose this from conn & use it directly instead of system.local.rpc_address

    for row in typed_peers_rows.chain(typed_local_rows) {
        let (ip_address, datacenter, rack, tokens) = row.map_err(|_| {
            QueryError::ProtocolError("system.peers or system.local has invalid column type")
        })?;

        let tokens_str: Vec<String> = tokens.unwrap_or_default();

        let address = SocketAddr::new(ip_address, connect_port);

        // Parse string representation of tokens as integer values
        let tokens: Vec<Token> = tokens_str
            .iter()
            .map(|s| Token::from_str(&s))
            .collect::<Result<Vec<Token>, _>>()
            .map_err(|_| QueryError::ProtocolError("Couldn't parse tokens as integer values"))?;

        result.push(Peer {
            address,
            datacenter,
            rack,
            tokens,
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
