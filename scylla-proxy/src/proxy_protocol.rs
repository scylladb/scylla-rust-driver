//! The binary header of the PROXY protocol, version 2, with which a load balancer tells
//! a backend the address of the client that a connection comes from.
//!
//! Only the parts that a proxy for testing needs are supported: the PROXY command over
//! TCP/IPv4 and TCP/IPv6. See <https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt>.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::{Buf, BufMut};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const SIGNATURE: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";
/// Protocol version 2, PROXY command.
const VERSION_2_PROXY: u8 = 0x21;
const TCP_OVER_IPV4: u8 = 0x11;
const TCP_OVER_IPV6: u8 = 0x21;

/// Writes the header for a connection from `source` to `destination`, which must be addresses
/// of the same family.
pub(crate) async fn write_header(
    stream: &mut (impl AsyncWrite + Unpin),
    source: SocketAddr,
    destination: SocketAddr,
) -> io::Result<()> {
    // Room for the fixed part and IPv6 addresses.
    let mut header = Vec::with_capacity(52);
    header.put_slice(&SIGNATURE);
    header.put_u8(VERSION_2_PROXY);
    match (source.ip(), destination.ip()) {
        (IpAddr::V4(source_ip), IpAddr::V4(destination_ip)) => {
            header.put_u8(TCP_OVER_IPV4);
            header.put_u16(12);
            header.put_slice(&source_ip.octets());
            header.put_slice(&destination_ip.octets());
        }
        (IpAddr::V6(source_ip), IpAddr::V6(destination_ip)) => {
            header.put_u8(TCP_OVER_IPV6);
            header.put_u16(36);
            header.put_slice(&source_ip.octets());
            header.put_slice(&destination_ip.octets());
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{source} and {destination} are of different address families"),
            ));
        }
    }
    header.put_u16(source.port());
    header.put_u16(destination.port());
    stream.write_all(&header).await
}

/// Reads the header from the start of a connection and returns the client address it carries.
pub(crate) async fn read_header(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<SocketAddr> {
    let not_a_header = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a PROXY protocol v2 header with the PROXY command",
        )
    };
    let mut fixed_part = [0u8; 16];
    // The first byte is checked alone, so that a client that sends a CQL frame instead
    // (likely shorter than the header) is rejected rather than awaited forever.
    stream.read_exact(&mut fixed_part[..1]).await?;
    if fixed_part[0] != SIGNATURE[0] {
        return Err(not_a_header());
    }
    stream.read_exact(&mut fixed_part[1..]).await?;
    if fixed_part[..12] != SIGNATURE || fixed_part[12] != VERSION_2_PROXY {
        return Err(not_a_header());
    }
    let family = fixed_part[13];
    let length = u16::from_be_bytes([fixed_part[14], fixed_part[15]]);

    let mut addresses = vec![0u8; length.into()];
    stream.read_exact(&mut addresses).await?;
    let mut addresses = addresses.as_slice();

    // The source address and the destination address come first, then their ports.
    // Any TLVs that follow are irrelevant here.
    let source_ip: IpAddr = match (family, length) {
        (TCP_OVER_IPV4, 12..) => {
            let source = Ipv4Addr::from(addresses.get_u32());
            addresses.advance(4);
            source.into()
        }
        (TCP_OVER_IPV6, 36..) => {
            let source = Ipv6Addr::from(addresses.get_u128());
            addresses.advance(16);
            source.into()
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported PROXY protocol v2 address family {family:#x} or length {length}"
                ),
            ));
        }
    };
    Ok(SocketAddr::new(source_ip, addresses.get_u16()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A header from 192.0.2.1:56324 to 198.51.100.7:9042, as the protocol specification lays it out.
    const IPV4_HEADER: [u8; 28] = [
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a, // signature
        0x21, // version 2, PROXY command
        0x11, // TCP over IPv4
        0x00, 0x0c, // length of what follows
        192, 0, 2, 1, // source address
        198, 51, 100, 7, // destination address
        0xdc, 0x04, // source port
        0x23, 0x52, // destination port
    ];

    #[tokio::test]
    async fn writes_header_as_specified() {
        let mut written = Vec::new();
        write_header(
            &mut written,
            "192.0.2.1:56324".parse().unwrap(),
            "198.51.100.7:9042".parse().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(written, IPV4_HEADER);
    }

    #[tokio::test]
    async fn reads_client_address() {
        assert_eq!(
            read_header(&mut IPV4_HEADER.as_slice()).await.unwrap(),
            "192.0.2.1:56324".parse().unwrap()
        );

        let source = "[2001:db8::1]:56324".parse().unwrap();
        let mut ipv6_header = Vec::new();
        write_header(
            &mut ipv6_header,
            source,
            "[2001:db8::2]:9042".parse().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            read_header(&mut ipv6_header.as_slice()).await.unwrap(),
            source
        );
    }

    #[tokio::test]
    async fn leaves_data_after_header_unread() {
        let data = [IPV4_HEADER.as_slice(), b"payload"].concat();
        let mut stream = data.as_slice();
        read_header(&mut stream).await.unwrap();
        assert_eq!(stream, b"payload");
    }

    #[tokio::test]
    async fn rejects_other_data() {
        // A CQL OPTIONS request, which a client not behind a load balancer would send first.
        let options_request = [0x04, 0x00, 0x00, 0x00, 0x05, 0, 0, 0, 0];
        let error = read_header(&mut options_request.as_slice())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
