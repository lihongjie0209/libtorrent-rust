use std::net::SocketAddr;

use bendy::decoding::Decoder;
use bendy::decoding::Object;
use percent_encoding::{percent_encode, AsciiSet, CONTROLS};

use crate::error::LibtorrentError;
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse, parse_compact_peers, parse_compact_peers_v6};

const QUERY_SET: &AsciiSet = &CONTROLS
    .add(b' ').add(b'!').add(b'"').add(b'#').add(b'$').add(b'%').add(b'&')
    .add(b'\'').add(b'(').add(b')').add(b'*').add(b'+').add(b',').add(b'/')
    .add(b':').add(b';').add(b'=').add(b'?').add(b'@').add(b'[').add(b']');

/// Build announce URL with query parameters
pub fn build_announce_url(base: &str, req: &AnnounceRequest) -> String {
    let mut url = String::from(base);
    if !url.contains('?') { url.push('?'); } else if !url.ends_with('&') { url.push('&'); }

    // info_hash and peer_id are raw bytes that must be percent-encoded
    url.push_str("info_hash=");
    url.push_str(&percent_encode(&req.info_hash, QUERY_SET).to_string());
    url.push('&');
    url.push_str("peer_id=");
    url.push_str(&percent_encode(&req.peer_id, QUERY_SET).to_string());
    url.push('&');

    url.push_str(&format!("port={}&uploaded={}&downloaded={}&left={}", req.port, req.uploaded, req.downloaded, req.left));
    url.push('&');
    url.push_str("compact=1");

    if let Some(n) = req.numwant { url.push_str(&format!("&numwant={}", n)); }
    if let Some(ev) = req.event {
        let ev_str = match ev { AnnounceEvent::Started => "started", AnnounceEvent::Stopped => "stopped", AnnounceEvent::Completed => "completed" };
        url.push_str(&format!("&event={}", ev_str));
    }
    if let Some(key) = &req.key { url.push_str("&key="); url.push_str(key); }
    if let Some(id) = &req.trackerid { url.push_str("&trackerid="); url.push_str(id); }

    url
}

/// Perform HTTP GET announce
pub async fn announce(url: &str) -> Result<AnnounceResponse, LibtorrentError> {
    let resp = reqwest::Client::new()
        .get(url)
        .send()
        .await?
        .error_for_status()?;

    let bytes = resp.bytes().await?;
    parse_announce_response(&bytes)
}

/// Parse bencoded tracker response
pub fn parse_announce_response(bytes: &[u8]) -> Result<AnnounceResponse, LibtorrentError> {
    let mut decoder = Decoder::new(bytes);
    let obj = decoder
        .next_object()
        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
    let obj = obj.ok_or_else(|| LibtorrentError::Protocol("empty tracker response".into()))?;
    let mut dict = obj
        .try_into_dictionary()
        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;

    let mut interval: Option<u32> = None;
    let mut warning_message: Option<String> = None;
    let mut tracker_id: Option<String> = None;
    let mut complete: Option<u32> = None;
    let mut incomplete: Option<u32> = None;
    let mut peers: Option<Vec<SocketAddr>> = None;
    let mut peers6: Option<Vec<SocketAddr>> = None;

    while let Some((key, value)) = dict
        .next_pair()
        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?
    {
        match key {
            b"interval" => {
                let v = value
                    .try_into_integer()
                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                interval = Some(v.parse::<u32>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
            }
            b"warning message" => {
                let s = String::from_utf8_lossy(
                    value
                        .try_into_bytes()
                        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?,
                )
                .into_owned();
                warning_message = Some(s);
            }
            b"tracker id" => {
                let s = String::from_utf8_lossy(
                    value
                        .try_into_bytes()
                        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?,
                )
                .into_owned();
                tracker_id = Some(s);
            }
            b"complete" => {
                let v = value
                    .try_into_integer()
                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                complete = Some(v.parse::<u32>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
            }
            b"incomplete" => {
                let v = value
                    .try_into_integer()
                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                incomplete = Some(v.parse::<u32>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
            }
            b"peers" => {
                // Could be a list of dicts or a compact string
                if let Object::Bytes(b) = value {
                    peers = Some(parse_compact_peers(b));
                } else if let Ok(mut list) = value.try_into_list() {
                    let mut out = Vec::new();
                    loop {
                        match list.next_object() {
                            Ok(Some(entry)) => {
                                let mut d = entry
                                    .try_into_dictionary()
                                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                                let mut ip: Option<String> = None;
                                let mut port: Option<u16> = None;
                                loop {
                                    match d.next_pair() {
                                        Ok(Some((k, v))) => match k {
                                            b"ip" => {
                                                let s = String::from_utf8_lossy(
                                                    v.try_into_bytes().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?,
                                                )
                                                .into_owned();
                                                ip = Some(s);
                                            }
                                            b"port" => {
                                                let p = v
                                                    .try_into_integer()
                                                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                                                port = Some(p.parse::<u16>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
                                            }
                                            _ => {}
                                        },
                                        Ok(None) => break,
                                        Err(e) => return Err(LibtorrentError::Protocol(format!("bencode: {}", e))),
                                    }
                                }
                                if let (Some(ip), Some(port)) = (ip, port) {
                                    if let Ok(addr) = ip.parse() { out.push(SocketAddr::new(addr, port)); }
                                }
                            }
                            Ok(None) => break,
                            Err(e) => return Err(LibtorrentError::Protocol(format!("bencode: {}", e))),
                        }
                    }
                    peers = Some(out);
                }
            }
            b"peers6" => {
                if let Object::Bytes(b) = value {
                    peers6 = Some(parse_compact_peers_v6(b));
                }
            }
            _ => {}
        }
    }

    let interval = interval.unwrap_or(1800);
    let mut all_peers = peers.unwrap_or_default();
    if let Some(v6) = peers6 { all_peers.extend(v6); }

    Ok(AnnounceResponse { interval, peers: all_peers, warning_message, tracker_id, complete, incomplete })
}

/// Minimal scrape response (per infohash)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrapeResponse {
    pub complete: Option<u32>,
    pub incomplete: Option<u32>,
    pub downloaded: Option<u32>,
}

/// Build scrape URL from announce URL, if possible
pub fn build_scrape_url(announce: &str) -> Option<String> {
    if let Some(pos) = announce.find("announce") {
        let mut s = announce.to_string();
        s.replace_range(pos..pos+"announce".len(), "scrape");
        Some(s)
    } else { None }
}

/// Perform HTTP GET scrape for a single info_hash
pub async fn scrape(url: &str, info_hash: &[u8;20]) -> Result<ScrapeResponse, LibtorrentError> {
    let mut full = String::from(url);
    if !full.contains('?') { full.push('?'); } else if !full.ends_with('&') { full.push('&'); }
    full.push_str("info_hash=");
    full.push_str(&percent_encode(info_hash, QUERY_SET).to_string());

    let resp = reqwest::Client::new()
        .get(&full)
        .send()
        .await?
        .error_for_status()?;

    let bytes = resp.bytes().await?;
    parse_scrape_response(&bytes)
}

fn parse_scrape_response(bytes: &[u8]) -> Result<ScrapeResponse, LibtorrentError> {
    let mut decoder = Decoder::new(bytes);
    let obj = decoder
        .next_object()
        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
    let obj = obj.ok_or_else(|| LibtorrentError::Protocol("empty scrape response".into()))?;
    let mut dict = obj
        .try_into_dictionary()
        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;

    while let Some((key, value)) = dict
        .next_pair()
        .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))? {
        if key == b"files" {
            let mut files = value
                .try_into_dictionary()
                .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
            // iterate entries; use the first (we only queried one infohash)
            while let Some((_ih, stats)) = files
                .next_pair()
                .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))? {
                let mut sdict = stats
                    .try_into_dictionary()
                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                let mut complete = None;
                let mut incomplete = None;
                let mut downloaded = None;
                while let Some((k, v)) = sdict
                    .next_pair()
                    .map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))? {
                    match k {
                        b"complete" => {
                            let n = v.try_into_integer().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                            complete = Some(n.parse::<u32>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
                        }
                        b"incomplete" => {
                            let n = v.try_into_integer().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                            incomplete = Some(n.parse::<u32>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
                        }
                        b"downloaded" => {
                            let n = v.try_into_integer().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?;
                            downloaded = Some(n.parse::<u32>().map_err(|e| LibtorrentError::Protocol(format!("bencode: {}", e)))?);
                        }
                        _ => {}
                    }
                }
                return Ok(ScrapeResponse { complete, incomplete, downloaded });
            }
        }
    }
    Err(LibtorrentError::Protocol("invalid scrape response".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_compact_response() {
        // Build a compact peers response: d8:intervali1800e5:peers12:XXXXXXXXXXXXe
        let mut resp = b"d8:intervali1800e5:peers12:".to_vec();
        // 2 peers: 1.2.3.4:6881 and 5.6.7.8:8080
        resp.extend_from_slice(&[1,2,3,4, 0x1A, 0xE1]);
        resp.extend_from_slice(&[5,6,7,8, 0x1F, 0x90]);
        resp.extend_from_slice(b"e");

        let out = parse_announce_response(&resp).unwrap();
        assert_eq!(out.interval, 1800);
        assert_eq!(out.peers.len(), 2);
        assert_eq!(out.peers[0].to_string(), "1.2.3.4:6881");
        assert_eq!(out.peers[1].to_string(), "5.6.7.8:8080");
    }

    #[test]
    fn parse_dict_peers_response() {
        // d8:intervali900e5:peersld2:ip9:127.0.0.14:porti6881eeee
        let resp = b"d8:intervali900e5:peersld2:ip9:127.0.0.14:porti6881eeee";
        let out = parse_announce_response(resp).unwrap();
        assert_eq!(out.interval, 900);
        assert_eq!(out.peers.len(), 1);
        assert_eq!(out.peers[0].to_string(), "127.0.0.1:6881");
    }
}
