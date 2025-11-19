use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use url::Url;

use crate::error::LibtorrentError;
use crate::tracker::{AnnounceRequest, AnnounceResponse, parse_compact_peers};

const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const PROTOCOL_ID: u64 = 0x41727101980; // initial connection id
const ACTION_SCRAPE: u32 = 2;

/// Build UDP socket address from udp:// URL
pub fn resolve_tracker_addr(url: &str) -> Result<SocketAddr, LibtorrentError> {
    let u = Url::parse(url).map_err(|e| LibtorrentError::Protocol(format!("invalid url: {}", e)))?;
    let host = u.host_str().ok_or_else(|| LibtorrentError::Protocol("missing host".into()))?;
    let port = u.port().unwrap_or(80);
    let addr = format!("{}:{}", host, port).parse().map_err(|e| LibtorrentError::Protocol(format!("invalid addr: {}", e)))?;
    Ok(addr)
}

fn build_connect_request(transaction_id: u32) -> Bytes {
    let mut buf = BytesMut::with_capacity(16);
    buf.put_u64(PROTOCOL_ID);
    buf.put_u32(ACTION_CONNECT);
    buf.put_u32(transaction_id);
    buf.freeze()
}

fn build_announce_request(connection_id: u64, transaction_id: u32, req: &AnnounceRequest, key: u32) -> Bytes {
    let mut buf = BytesMut::with_capacity(98);
    buf.put_u64(connection_id);
    buf.put_u32(ACTION_ANNOUNCE);
    buf.put_u32(transaction_id);
    buf.put_slice(&req.info_hash);
    buf.put_slice(&req.peer_id);
    buf.put_u64(req.downloaded);
    buf.put_u64(req.left);
    buf.put_u64(req.uploaded);
    // event
    let ev = match req.event { Some(super::AnnounceEvent::Completed) => 1, Some(super::AnnounceEvent::Started) => 2, Some(super::AnnounceEvent::Stopped) => 3, None => 0 };
    buf.put_u32(ev);
    // IP address (0 = default)
    buf.put_u32(0);
    // key
    buf.put_u32(key);
    // numwant (-1 default)
    buf.put_u32(req.numwant.unwrap_or(-1) as u32);
    buf.put_u16(req.port);
    buf.freeze()
}

/// Perform a UDP tracker announce (single-shot)
pub async fn announce(url: &str, req: &AnnounceRequest) -> Result<AnnounceResponse, LibtorrentError> {
    let tracker = resolve_tracker_addr(url)?;
    let socket = UdpSocket::bind("0.0.0.0:0").await?;

    // send connect
    let transaction_id = rand::random::<u32>();

    let connect = build_connect_request(transaction_id);
    socket.send_to(&connect, tracker).await?;

    let mut buf = [0u8; 2048];
    let (n, from) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await
        .map_err(|_| LibtorrentError::Protocol("udp tracker connect timeout".into()))??;
    if from != tracker || n < 16 {
        return Err(LibtorrentError::Protocol("invalid connect response".into()));
    }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let tid_resp = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let connection_id = u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]);
    if action != ACTION_CONNECT || tid_resp != transaction_id {
        return Err(LibtorrentError::Protocol("unexpected connect response".into()));
    }

    // announce
    let key = rand::random::<u32>();
    let transaction_id2 = rand::random::<u32>();
    let announce = build_announce_request(connection_id, transaction_id2, req, key);
    socket.send_to(&announce, tracker).await?;

    let (n, from) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await
        .map_err(|_| LibtorrentError::Protocol("udp tracker announce timeout".into()))??;
    if from != tracker || n < 20 {
        return Err(LibtorrentError::Protocol("invalid announce response".into()));
    }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let tid_resp = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if action != ACTION_ANNOUNCE || tid_resp != transaction_id2 {
        return Err(LibtorrentError::Protocol("unexpected announce response".into()));
    }
    let interval = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let _leechers = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let _seeders = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    let peers = parse_compact_peers(&buf[20..n]);

    Ok(AnnounceResponse { interval, peers, warning_message: None, tracker_id: None, complete: None, incomplete: None })
}

/// Perform a UDP tracker scrape (single info-hash)
pub async fn scrape(url: &str, info_hash: &[u8;20]) -> Result<crate::tracker::http::ScrapeResponse, LibtorrentError> {
    let tracker = resolve_tracker_addr(url)?;
    let socket = UdpSocket::bind("0.0.0.0:0").await?;

    // connect
    let transaction_id = rand::random::<u32>();
    let connect = build_connect_request(transaction_id);
    socket.send_to(&connect, tracker).await?;

    let mut buf = [0u8; 2048];
    let (n, from) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await
        .map_err(|_| LibtorrentError::Protocol("udp tracker connect timeout".into()))??;
    if from != tracker || n < 16 {
        return Err(LibtorrentError::Protocol("invalid connect response".into()));
    }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let tid_resp = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let connection_id = u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]);
    if action != ACTION_CONNECT || tid_resp != transaction_id {
        return Err(LibtorrentError::Protocol("unexpected connect response".into()));
    }

    // scrape
    let transaction_id2 = rand::random::<u32>();
    // build request: connection_id(8) + action(4=2) + transaction_id(4) + info_hashes
    let mut req_buf = BytesMut::with_capacity(8 + 4 + 4 + 20);
    req_buf.put_u64(connection_id);
    req_buf.put_u32(ACTION_SCRAPE);
    req_buf.put_u32(transaction_id2);
    req_buf.put_slice(info_hash);
    let pkt = req_buf.freeze();
    socket.send_to(&pkt, tracker).await?;

    let (n, from) = timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await
        .map_err(|_| LibtorrentError::Protocol("udp tracker scrape timeout".into()))??;
    if from != tracker || n < 8 {
        return Err(LibtorrentError::Protocol("invalid scrape response".into()));
    }
    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let tid_resp = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if action != ACTION_SCRAPE || tid_resp != transaction_id2 {
        return Err(LibtorrentError::Protocol("unexpected scrape response".into()));
    }
    // expect at least one triplet of 12 bytes: seeders, completed, leechers
    if n < 8 + 12 {
        return Err(LibtorrentError::Protocol("short scrape response".into()));
    }
    let seeders = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let completed = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let leechers = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
    Ok(crate::tracker::http::ScrapeResponse {
        complete: Some(seeders),
        incomplete: Some(leechers),
        downloaded: Some(completed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_connect_packet_len() {
        let pkt = build_connect_request(0x12345678);
        assert_eq!(pkt.len(), 16);
    }

    #[test]
    fn build_announce_packet_len() {
        let req = AnnounceRequest {
            info_hash: [0u8; 20],
            peer_id: [1u8; 20],
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 100,
            compact: true,
            numwant: Some(50),
            event: None,
            key: None,
            trackerid: None,
        };
        let pkt = build_announce_request(0x1122334455667788, 0x99AABBCC, &req, 0xCAFEBABE);
        assert!(pkt.len() >= 98);
    }
}
