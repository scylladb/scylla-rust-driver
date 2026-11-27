//! Layout of a cluster's nodes across datacenters and racks.

use std::collections::HashSet;

use anyhow::Error;

/// Datacenters of a cluster, each with its racks and the number of nodes in them.
///
/// ### Example
/// ```
/// # use scylla_ccm_bridge::topology::{Datacenter, Rack, Topology};
/// // `dc1` with 2 nodes in `rack1` and 1 node in `rack2`, and `dc2` with 1 node in `rack1`.
/// let topology = Topology::new([
///     Datacenter::new("dc1", [Rack::new("rack1", 2), Rack::new("rack2", 1)]),
///     Datacenter::new("dc2", [Rack::new("rack1", 1)]),
/// ]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Topology {
    datacenters: Vec<Datacenter>,
}

impl Topology {
    /// Creates a topology made of the given datacenters.
    pub fn new(datacenters: impl IntoIterator<Item = Datacenter>) -> Self {
        Self {
            datacenters: datacenters.into_iter().collect(),
        }
    }

    /// Creates a topology with datacenters `dc1`, `dc2`, … with the given numbers of nodes.
    /// Each node gets a rack of its own: `rack1`, `rack2`, … in every datacenter.
    ///
    /// ```
    /// # use scylla_ccm_bridge::topology::{Datacenter, Rack, Topology};
    /// assert_eq!(
    ///     Topology::simple([2, 1]),
    ///     Topology::new([
    ///         Datacenter::new("dc1", [Rack::new("rack1", 1), Rack::new("rack2", 1)]),
    ///         Datacenter::new("dc2", [Rack::new("rack1", 1)]),
    ///     ]),
    /// );
    /// ```
    pub fn simple(nodes_per_dc: impl IntoIterator<Item = u8>) -> Self {
        Self::new(nodes_per_dc.into_iter().enumerate().map(|(dc, nodes)| {
            Datacenter::new(
                format!("dc{}", dc + 1),
                (1..=nodes).map(|rack| Rack::new(format!("rack{rack}"), 1)),
            )
        }))
    }

    /// Returns the datacenters of this topology.
    pub fn datacenters(&self) -> &[Datacenter] {
        &self.datacenters
    }

    /// Checks that this topology can be used to create a cluster.
    ///
    /// Every datacenter and rack must have a name that is unique among its siblings and contain
    /// at least one node: a datacenter or rack without nodes wouldn't exist.
    pub(crate) fn validate_initial(&self) -> Result<(), Error> {
        anyhow::ensure!(
            !self.datacenters.is_empty(),
            "the topology has no datacenters"
        );

        let mut datacenter_names = HashSet::new();
        let mut total_nodes = 0_usize;
        for datacenter in &self.datacenters {
            let dc_name = datacenter.name();
            anyhow::ensure!(!dc_name.is_empty(), "a datacenter has an empty name");
            anyhow::ensure!(
                datacenter_names.insert(dc_name),
                "datacenter {dc_name} is declared more than once"
            );
            anyhow::ensure!(
                !datacenter.racks().is_empty(),
                "datacenter {dc_name} has no racks"
            );

            let mut rack_names = HashSet::new();
            for rack in datacenter.racks() {
                let rack_name = rack.name();
                anyhow::ensure!(
                    !rack_name.is_empty(),
                    "a rack in datacenter {dc_name} has an empty name"
                );
                anyhow::ensure!(
                    rack_names.insert(rack_name),
                    "rack {rack_name} is declared more than once in datacenter {dc_name}"
                );
                anyhow::ensure!(
                    rack.nodes() > 0,
                    "rack {rack_name} in datacenter {dc_name} has no nodes"
                );
                total_nodes += usize::from(rack.nodes());
            }
        }

        // Node IDs, which also determine the nodes' addresses, go from 1 to 255.
        anyhow::ensure!(
            total_nodes <= 255,
            "the topology has {total_nodes} nodes, but at most 255 are supported"
        );

        Ok(())
    }
}

/// A datacenter of a [`Topology`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datacenter {
    name: String,
    racks: Vec<Rack>,
}

impl Datacenter {
    /// Creates a datacenter with the given name, made of the given racks.
    pub fn new(name: impl Into<String>, racks: impl IntoIterator<Item = Rack>) -> Self {
        Self {
            name: name.into(),
            racks: racks.into_iter().collect(),
        }
    }

    /// Returns the name of this datacenter.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the racks of this datacenter.
    pub fn racks(&self) -> &[Rack] {
        &self.racks
    }
}

/// A rack of a [`Datacenter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rack {
    name: String,
    nodes: u8,
}

impl Rack {
    /// Creates a rack with the given name, containing the given number of nodes.
    pub fn new(name: impl Into<String>, nodes: u8) -> Self {
        Self {
            name: name.into(),
            nodes,
        }
    }

    /// Returns the name of this rack.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of nodes in this rack.
    pub fn nodes(&self) -> u8 {
        self.nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_topologies() {
        let topologies = [
            Topology::new([Datacenter::new("dc1", [Rack::new("rack1", 1)])]),
            Topology::new([
                Datacenter::new("dc1", [Rack::new("rack1", 2), Rack::new("rack2", 1)]),
                // Rack names only need to be unique within their datacenter.
                Datacenter::new("dc2", [Rack::new("rack1", 1)]),
            ]),
            Topology::new([Datacenter::new("dc1", [Rack::new("rack1", 255)])]),
        ];
        for topology in topologies {
            topology
                .validate_initial()
                .unwrap_or_else(|err| panic!("{topology:?} was rejected: {err}"));
        }
    }

    #[test]
    fn rejects_invalid_topologies() {
        let topologies = [
            // No datacenters.
            Topology::default(),
            // A datacenter without racks.
            Topology::new([Datacenter::new("dc1", [])]),
            // A rack without nodes.
            Topology::new([Datacenter::new(
                "dc1",
                [Rack::new("rack1", 1), Rack::new("rack2", 0)],
            )]),
            // Empty names.
            Topology::new([Datacenter::new("", [Rack::new("rack1", 1)])]),
            Topology::new([Datacenter::new("dc1", [Rack::new("", 1)])]),
            // Duplicate names.
            Topology::new([
                Datacenter::new("dc1", [Rack::new("rack1", 1)]),
                Datacenter::new("dc1", [Rack::new("rack2", 1)]),
            ]),
            Topology::new([Datacenter::new(
                "dc1",
                [Rack::new("rack1", 1), Rack::new("rack1", 1)],
            )]),
            // Too many nodes.
            Topology::new([Datacenter::new(
                "dc1",
                [Rack::new("rack1", 255), Rack::new("rack2", 1)],
            )]),
        ];
        for topology in topologies {
            assert!(
                topology.validate_initial().is_err(),
                "{topology:?} was accepted"
            );
        }
    }
}
