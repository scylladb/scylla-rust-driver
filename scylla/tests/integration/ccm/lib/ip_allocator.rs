use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use anyhow::{Context, Error};

/// A subnet prefix for local network (127.x.x.x/24).
#[derive(Debug, Clone, Copy)]
pub(crate) struct NetPrefix(IpAddr);

impl NetPrefix {
    pub(super) fn empty() -> Self {
        NetPrefix(IpAddr::V6(Ipv6Addr::UNSPECIFIED))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_unspecified()
    }

    pub(super) fn to_ipaddress(self, id: u16) -> IpAddr {
        match self.0 {
            IpAddr::V4(v4) => {
                let mut octets = v4.octets();
                octets[3] = id as u8;
                IpAddr::V4(Ipv4Addr::from(octets))
            }
            IpAddr::V6(v6) => {
                let mut segments = v6.segments();
                segments[7] = id;
                IpAddr::V6(Ipv6Addr::from(segments))
            }
        }
    }
}

impl std::fmt::Display for NetPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            IpAddr::V4(v4) => {
                let octets = v4.octets();
                write!(f, "{}.{}.{}.", octets[0], octets[1], octets[2])
            }
            IpAddr::V6(v6) => {
                let mut segments = v6.segments();
                segments[7] = 0; // Set last segment to 0
                let new_ip = Ipv6Addr::from(segments);
                let formatted = new_ip.to_string();
                write!(
                    f,
                    "{}:",
                    formatted
                        .rsplit_once(':')
                        .map(|(prefix, _suffix)| prefix)
                        .unwrap()
                )
            }
        }
    }
}

impl From<IpAddr> for NetPrefix {
    fn from(ip: IpAddr) -> Self {
        NetPrefix(ip)
    }
}

impl FromStr for NetPrefix {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(IpAddr::from_str(s)?.into())
    }
}

impl Default for NetPrefix {
    fn default() -> Self {
        NetPrefix::empty()
    }
}

/// A local subnet identifier (127.x.y.0/24).
/// The local subnet is identified by two octets x and y.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct LocalSubnetIdentifier(u8, u8);

impl From<LocalSubnetIdentifier> for Ipv4Addr {
    fn from(subnet_id: LocalSubnetIdentifier) -> Self {
        Ipv4Addr::new(127, subnet_id.0, subnet_id.1, 0)
    }
}

impl From<LocalSubnetIdentifier> for NetPrefix {
    fn from(subnet_id: LocalSubnetIdentifier) -> Self {
        NetPrefix(Ipv4Addr::from(subnet_id).into())
    }
}

impl From<Ipv4Addr> for LocalSubnetIdentifier {
    fn from(ip: Ipv4Addr) -> Self {
        LocalSubnetIdentifier(ip.octets()[1], ip.octets()[2])
    }
}

pub(super) struct IpAllocator {
    used_ips: BTreeSet<LocalSubnetIdentifier>,
}

impl IpAllocator {
    /// The constructor scans /proc/net/tcp for busy local subnets (127.x.y.z/24). The subnet is busy,
    /// if there is at least one listener on any port and any address in this network.
    /// Example fragment of the file:
    ///   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
    ///   0: 3500007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000   193        0 7487 1 000000007e427786 100 0 0 10 5
    pub(super) fn new() -> Result<Self, Error> {
        let mut used_ips: BTreeSet<LocalSubnetIdentifier> = BTreeSet::new();
        let file = File::open("/proc/net/tcp").context("Failed to open /proc/net/tcp file")?;
        let mut lines = BufReader::new(file).lines();
        let _header_line = lines
            .next()
            .context("Failed to read the header line from /proc/net/tcp")?;
        for line_res in lines {
            let line = line_res.context("Failed to read a line from /proc/net/tcp")?;
            line.split_whitespace()
                .nth(1) // Skip ordinal number (first column), and get local address (second column)
                .and_then(|ip_addr_hex| ip_addr_hex.split_once(':'))
                .and_then(|(addr_hex, _port_hex)| u32::from_str_radix(addr_hex, 16).ok())
                .inspect(|&ip| {
                    let first_octet = ip as u8;
                    if first_octet == 127 {
                        used_ips.insert(LocalSubnetIdentifier((ip >> 8) as u8, (ip >> 16) as u8));
                    }
                });
        }
        Ok(Self { used_ips })
    }

    /// Removes a free IP prefix from the pool of local subnets (127.x.x.x/24) and returns it to the caller.
    /// The IP prefix should be later returned via [`IpAllocator::return_ip_prefix`].
    pub(super) fn alloc_ip_prefix(&mut self) -> Result<NetPrefix, Error> {
        for a in 0..=255 {
            for b in 0..=255 {
                if a == 0 && b == 0 {
                    continue;
                }
                let subnet_id = LocalSubnetIdentifier(a, b);
                if !self.used_ips.contains(&subnet_id) {
                    self.used_ips.insert(subnet_id);
                    return Ok(subnet_id.into());
                }
            }
        }

        Err(anyhow::anyhow!("No free IP prefixes available"))
    }

    /// Returns the IP prefix back to the pool of local subnets (127.x.x.x/24).
    pub(super) fn free_ip_prefix(&mut self, ip_prefix: &NetPrefix) -> Result<(), Error> {
        let ipv4 = match ip_prefix.0 {
            IpAddr::V4(v4) => v4,
            _ => return Err(anyhow::anyhow!("Ipv6 addresses are not yet supported!")),
        };
        let subnet_id: LocalSubnetIdentifier = ipv4.into();

        if !self.used_ips.remove(&subnet_id) {
            return Err(anyhow::anyhow!(
                "IP prefix {} was not allocated - something gone wrong!",
                ip_prefix
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subnet_identifier_to_ipv4() {
        let subnet_id = LocalSubnetIdentifier(21, 37);
        let ipv4 = Ipv4Addr::from(subnet_id);

        assert_eq!(ipv4, Ipv4Addr::new(127, 21, 37, 0));
    }

    #[test]
    fn test_ipv4_to_subnet_identifier() {
        let ipv4 = Ipv4Addr::new(127, 21, 37, 0);
        let subnet_id = LocalSubnetIdentifier::from(ipv4);

        assert_eq!(subnet_id, LocalSubnetIdentifier(21, 37));
    }

    #[test]
    fn test_ipv4_prefix() {
        let ip = NetPrefix::from_str("192.168.1.100").unwrap();
        assert_eq!(ip.to_string(), "192.168.1.");
    }

    #[test]
    fn test_ipv4_loopback() {
        let ip = NetPrefix::from_str("127.0.0.1").unwrap();
        assert_eq!(ip.to_string(), "127.0.0.");
    }

    #[test]
    fn test_ipv4_edge_case() {
        let ip = NetPrefix::from_str("0.0.0.0").unwrap();
        assert_eq!(ip.to_string(), "0.0.0.");
    }

    #[test]
    fn test_ipv6_prefix() {
        let ip = NetPrefix::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap();
        assert_eq!(ip.to_string(), "2001:db8:85a3::8a2e:370:");
    }

    #[test]
    fn test_ipv6_loopback() {
        let ip = NetPrefix::from_str("::1").unwrap();
        assert_eq!(ip.to_string(), "::");
    }

    #[test]
    fn test_ipv6_shortened() {
        let ip = NetPrefix::from_str("2001:db8::ff00:42:8329").unwrap();
        assert_eq!(ip.to_string(), "2001:db8::ff00:42:");
    }
}
