use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub mod http;
pub mod udp;

/// Announce event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
}

/// Request parameters for tracker announce
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub compact: bool,
    pub numwant: Option<i32>,
    pub event: Option<AnnounceEvent>,
    pub key: Option<String>,
    /// tracker-provided id to be echoed back in subsequent announces (HTTP)
    pub trackerid: Option<String>,
}

impl AnnounceRequest {
    pub fn left_from_total(total_size: u64, downloaded: u64) -> u64 {
        total_size.saturating_sub(downloaded)
    }
}

/// Response from tracker announce
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: u32,
    pub peers: Vec<SocketAddr>,
    pub warning_message: Option<String>,
    pub tracker_id: Option<String>,
    pub complete: Option<u32>,
    pub incomplete: Option<u32>,
}

/// Parse compact peers string into a list of socket addresses
pub fn parse_compact_peers(peers: &[u8]) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for chunk in peers.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        out.push(SocketAddr::new(IpAddr::V4(ip), port));
    }
    out
}

/// Parse compact IPv6 peers ("peers6" style) into a list of socket addresses
pub fn parse_compact_peers_v6(peers: &[u8]) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for chunk in peers.chunks_exact(18) {
        let ip = Ipv6Addr::new(
            u16::from_be_bytes([chunk[0], chunk[1]]),
            u16::from_be_bytes([chunk[2], chunk[3]]),
            u16::from_be_bytes([chunk[4], chunk[5]]),
            u16::from_be_bytes([chunk[6], chunk[7]]),
            u16::from_be_bytes([chunk[8], chunk[9]]),
            u16::from_be_bytes([chunk[10], chunk[11]]),
            u16::from_be_bytes([chunk[12], chunk[13]]),
            u16::from_be_bytes([chunk[14], chunk[15]]),
        );
        let port = u16::from_be_bytes([chunk[16], chunk[17]]);
        out.push(SocketAddr::new(IpAddr::V6(ip), port));
    }
    out
}
