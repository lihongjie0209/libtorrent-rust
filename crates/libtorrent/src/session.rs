use crate::{
    config::SessionConfig, 
    error::LibtorrentError, 
    peer::PeerSession,
    torrent::{TorrentHandle, SharedTorrentHandle},
    metainfo::TorrentMeta,
    tracker::{http as tracker_http, AnnounceRequest},
};
use libtorrent_proto::{Handshake, PEER_ID_LEN};
use std::collections::HashMap;
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use crate::net::transport;
use tokio::sync::{Semaphore, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use rand::seq::SliceRandom;
use std::collections::HashSet;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

/// InfoHash type alias
pub type InfoHash = [u8; 20];

pub struct Session {
    config: SessionConfig,
    peer_id: [u8; PEER_ID_LEN],
    /// Map of active torrents by their info hash
    torrents: Arc<RwLock<HashMap<InfoHash, SharedTorrentHandle>>>,
    upload_slots: Arc<Semaphore>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_candidates_prioritization_downloading() {
        let a: SocketAddr = "127.0.0.1:3001".parse().unwrap(); // uTP non-seed
        let b: SocketAddr = "127.0.0.1:3002".parse().unwrap(); // uTP seed
        let c: SocketAddr = "127.0.0.1:3003".parse().unwrap(); // TCP non-seed
        let d: SocketAddr = "127.0.0.1:3004".parse().unwrap(); // TCP seed
        let peers = vec![
            (d, 0x02),
            (c, 0x00),
            (b, 0x02 | 0x04),
            (a, 0x04),
        ];
        let ordered = order_candidates(peers, false);
        // Find group boundaries
        let pos_utp_nonseed = ordered.iter().position(|(sa, _)| *sa == a).unwrap();
        let pos_utp_seed = ordered.iter().position(|(sa, _)| *sa == b).unwrap();
        let pos_tcp_nonseed = ordered.iter().position(|(sa, _)| *sa == c).unwrap();
        let pos_tcp_seed = ordered.iter().position(|(sa, _)| *sa == d).unwrap();
        assert!(pos_utp_nonseed < pos_utp_seed);
        assert!(pos_utp_seed < pos_tcp_nonseed);
        assert!(pos_tcp_nonseed < pos_tcp_seed);
    }

    #[test]
    fn test_order_candidates_prioritization_complete() {
        let a: SocketAddr = "127.0.0.1:3101".parse().unwrap(); // uTP
        let b: SocketAddr = "127.0.0.1:3102".parse().unwrap(); // TCP
        let peers = vec![
            (b, 0x00),
            (a, 0x04),
        ];
        let ordered = order_candidates(peers, true);
        // All uTP should come before any TCP when complete
        let pos_utp = ordered.iter().position(|(sa, _)| *sa == a).unwrap();
        let pos_tcp = ordered.iter().position(|(sa, _)| *sa == b).unwrap();
        assert!(pos_utp < pos_tcp);
    }

    // Fallback behavior is exercised via build-time feature gate and connection fallback logic.
}

// Centralized candidate dialing policy for tracker/PEX/LSD
async fn dial_candidates(
    handle: SharedTorrentHandle,
    peer_id: [u8; PEER_ID_LEN],
    upload_slots: Arc<Semaphore>,
    pipeline_max: usize,
    request_timeout_secs: u64,
    opt_enabled: bool,
    opt_interval: u64,
    max_outbound: usize,
    utp_enabled: bool,
    peers: Vec<(SocketAddr, u8)>,
) {
    // Build known set and snapshot completion
    let (mut known, is_complete) = {
        let h = handle.read().await;
        let mut set = HashSet::new();
        for p in h.peers() { set.insert(p.addr); }
        (set, h.is_complete())
    };

    #[inline]
    fn decide_try_utp(flags: u8, utp_enabled: bool) -> bool { utp_enabled && (flags & 0x04) != 0 }

    let mut candidates: Vec<(SocketAddr, u8)> = peers
        .into_iter()
        .filter(|(a, _)| !known.contains(a))
        .collect();

    // Prioritize by flags with randomized order within buckets
    if !candidates.is_empty() {
        candidates = order_candidates(candidates, is_complete);
    }

    let current_count = known.len();
    let mut allowed_new = if current_count >= max_outbound { 0 } else { max_outbound - current_count };
    for (addr, flags) in candidates {
        if allowed_new == 0 { break; }
        // Add to peer list with flags
        {
            let mut h = handle.write().await;
            if h.peers().any(|p| p.addr == addr) { continue; }
            let num_pieces = h.meta().info.num_pieces();
            let pi = crate::torrent::PeerInfo {
                addr,
                peer_id: [0u8; 20],
                bitfield: vec![false; num_pieces],
                peer_choking: true,
                am_interested: false,
                am_choking: true,
                peer_interested: false,
                recent_downloaded: 0,
                pex_flags: flags & 0x0F,
            };
            h.add_peer(pi);
        }
        known.insert(addr);
        allowed_new -= 1;

        let try_utp = decide_try_utp(flags, utp_enabled);
        // Try uTP when flagged (feature-gated), fall back to TCP
        let transport_opt: Option<crate::net::transport::Transport> = if try_utp {
            #[cfg(feature = "utp")]
            {
                match crate::net::utp::connect(addr).await {
                    Ok(t) => Some(t),
                    Err(_) => match tokio::net::TcpStream::connect(addr).await {
                        Ok(s) => Some(transport::from_tcp(s)),
                        Err(_) => None,
                    },
                }
            }
            #[cfg(not(feature = "utp"))]
            {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(s) => Some(transport::from_tcp(s)),
                    Err(_) => None,
                }
            }
        } else {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(s) => Some(transport::from_tcp(s)),
                Err(_) => None,
            }
        };

        if let Some(stream) = transport_opt {
            let h = handle.read().await;
            let info_hash_local = h.meta().info.raw_infohash;
            drop(h);
            let per_peer_limit = 0u64;
            let session = crate::peer::PeerSession::new(
                stream,
                addr,
                info_hash_local,
                peer_id,
                Some(handle.clone()),
                Some(upload_slots.clone()),
                per_peer_limit,
                pipeline_max,
                request_timeout_secs,
                opt_enabled,
                opt_interval,
            );
            tokio::spawn(async move {
                if let Err(e) = session.run().await {
                    warn!(peer = %addr, error = %e, "peer session failed");
                }
            });
        }
    }
}

fn order_candidates(peers: Vec<(SocketAddr, u8)>, is_complete: bool) -> Vec<(SocketAddr, u8)> {
    if peers.is_empty() { return peers; }
    if !is_complete {
        let mut utp_nonseed = Vec::new();
        let mut utp_seed = Vec::new();
        let mut tcp_nonseed = Vec::new();
        let mut tcp_seed = Vec::new();
        for c in peers.into_iter() {
            let f = c.1;
            let is_utp = (f & 0x04) != 0;
            let is_seed = (f & 0x02) != 0;
            match (is_utp, is_seed) {
                (true, false) => utp_nonseed.push(c),
                (true, true) => utp_seed.push(c),
                (false, false) => tcp_nonseed.push(c),
                (false, true) => tcp_seed.push(c),
            }
        }
        let mut rng = rand::thread_rng();
        utp_nonseed.shuffle(&mut rng);
        utp_seed.shuffle(&mut rng);
        tcp_nonseed.shuffle(&mut rng);
        tcp_seed.shuffle(&mut rng);
        let mut out = utp_nonseed;
        out.extend(utp_seed);
        out.extend(tcp_nonseed);
        out.extend(tcp_seed);
        out
    } else {
        let mut utp = Vec::new();
        let mut tcp = Vec::new();
        for c in peers.into_iter() {
            if (c.1 & 0x04) != 0 { utp.push(c); } else { tcp.push(c); }
        }
        let mut rng = rand::thread_rng();
        utp.shuffle(&mut rng);
        tcp.shuffle(&mut rng);
        utp.into_iter().chain(tcp.into_iter()).collect()
    }
}

impl Session {
    pub fn new(config: SessionConfig) -> Self {
        let peer_id = Handshake::random_peer_id(config.peer_id_prefix.as_bytes());
        let upload_slots = Arc::new(Semaphore::new(config.max_upload_slots));
        Self { 
            config, 
            peer_id,
            torrents: Arc::new(RwLock::new(HashMap::new())),
            upload_slots,
        }
    }

    pub fn peer_id(&self) -> [u8; PEER_ID_LEN] {
        self.peer_id
    }

    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Add a torrent to the session
    pub async fn add_torrent(&self, meta: TorrentMeta) -> SharedTorrentHandle {
        let info_hash = meta.info.raw_infohash;
        let handle = Arc::new(RwLock::new(TorrentHandle::new(meta)));
        
        let mut torrents = self.torrents.write().await;
        torrents.insert(info_hash, handle.clone());
        
        info!(
            infohash = hex::encode(info_hash),
            "Added torrent to session"
        );
        
        // Initialize disk manager for this torrent
        {
            let mut h = handle.write().await;
            if let Err(e) = h.init_disk(&self.config.download_dir).await {
                warn!(error = %e, dir = %self.config.download_dir.display(), "disk init failed");
            }
        }
        
        // Kick off a tracker announce task based on scheme (best-effort)
        { let _ = self.start_tracker_manager_for(info_hash); }

        // Start choke/unchoke policy manager for this torrent
        {
            let _ = self.start_choke_manager_for(info_hash);
        }

        // Start PEX connector for this torrent
        {
            let _ = self.start_pex_connector_for(info_hash);
        }

        // Start LSD for this torrent (best-effort)
        {
            let _ = self.start_lsd_for(info_hash);
        }

        // Start Web Seed fetcher for this torrent (best-effort)
        {
            let _ = self.start_webseed_for(info_hash);
        }

        // Start DHT bootstrap/get_peers/announce (best-effort, non-private torrents)
        #[cfg(feature = "dht")]
        if self.config.dht_enabled {
            // Check torrent privacy flag via handle
            let is_private = {
                let h = handle.read().await;
                h.meta().info.private
            };
            if !is_private {
                let _ = self.start_dht_for(info_hash);
            }
        }

        handle
    }
    /// Start a basic web seed fetcher (BEP 17/19) for single-file torrents using HTTP range
    pub fn start_webseed_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let download_dir = self.config.download_dir.clone();
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) { Some(h) => h.clone(), None => return }
            };
            // Read web seeds
            let web_seeds = {
                let h = handle.read().await;
                h.meta().web_seeds.clone()
            };
            if web_seeds.is_empty() { return; }

            let client = reqwest::Client::new();
            let mut idx_seed = 0usize;
            let mut backoff: u64 = 5;
            loop {
                // Exit when complete
                {
                    let h = handle.read().await;
                    if h.is_complete() { break; }
                }

                // Reserve a piece to fetch
                let piece_opt = {
                    let mut h = handle.write().await;
                    // ensure disk initialized
                    if !h.has_disk() {
                        if let Err(e) = h.init_disk(&download_dir).await { warn!(error = %e, "disk init failed for webseed"); }
                    }
                    h.reserve_missing_piece()
                };
                let piece = match piece_opt { Some(p) => p, None => { tokio::time::sleep(std::time::Duration::from_secs(2)).await; continue; } };

                // Compute absolute bounds and len
                let (piece_start, piece_len) = {
                    let h = handle.read().await;
                    let _dm = match h.disk() { Some(dm) => dm, None => { // no disk yet
                        let mut hw = handle.write().await; hw.abort_piece(piece); tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); continue; }
                    };
                    let (s, e) = { // use private methods via piece index math (replicated)
                        let start = piece as u64 * h.meta().info.piece_length as u64;
                        let end = (start + h.meta().info.piece_length as u64).min(h.meta().info.total_size());
                        (start, end)
                    };
                    (s, (e - s) as u32)
                };

                // Determine single vs multi-file handling
                let (is_single_file, info_name, files) = {
                    let h = handle.read().await;
                    (h.meta().info.is_single_file(), h.meta().info.name.clone(), h.meta().info.files.clone())
                };

                let base = web_seeds[idx_seed % web_seeds.len()].clone();
                idx_seed = (idx_seed + 1) % web_seeds.len();

                // Helper: build encoded path for a given file index
                let build_path = |file_index: usize| -> String {
                    let mut parts = Vec::new();
                    parts.push(info_name.clone());
                    for comp in files[file_index].path.iter() {
                        parts.push(comp.to_string_lossy().to_string());
                    }
                    let encoded: Vec<String> = parts
                        .into_iter()
                        .map(|p| utf8_percent_encode(&p, NON_ALPHANUMERIC).to_string())
                        .collect();
                    encoded.join("/")
                };

                // Compute file slices that cover this piece
                let mut piece_bytes = Vec::with_capacity(piece_len as usize);
                if is_single_file {
                    // Single-file: fetch directly from base with piece range
                    let range_header = format!("bytes={}-{}", piece_start, piece_start + piece_len as u64 - 1);
                    match client.get(&base).header(reqwest::header::RANGE, range_header).send().await {
                        Ok(r) => {
                            let status = r.status();
                            if !(status == reqwest::StatusCode::PARTIAL_CONTENT || status == reqwest::StatusCode::OK) {
                                let mut h = handle.write().await; h.abort_piece(piece); tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); continue;
                            }
                            match r.bytes().await {
                                Ok(bytes) => {
                                    if bytes.len() as u32 != piece_len && status != reqwest::StatusCode::OK {
                                        let mut h = handle.write().await; h.abort_piece(piece); tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); continue;
                                    }
                                    piece_bytes = bytes.to_vec();
                                }
                                Err(_) => {
                                    let mut h = handle.write().await; h.abort_piece(piece);
                                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); continue;
                                }
                            }
                        }
                        Err(_) => {
                            let mut h = handle.write().await; h.abort_piece(piece);
                            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); continue;
                        }
                    }
                } else {
                    // Multi-file: split piece over file ranges and stitch
                    let mut file_starts = Vec::with_capacity(files.len());
                    let mut acc: u64 = 0;
                    for f in &files { file_starts.push(acc); acc += f.length; }
                    let piece_end = piece_start + piece_len as u64;

                    // Find first file index containing piece_start
                    let mut fi = match file_starts.binary_search(&piece_start) {
                        Ok(i) => i,
                        Err(i) => i.saturating_sub(1),
                    };
                    while fi < files.len() && file_starts[fi] + files[fi].length <= piece_start { fi += 1; }

                    let mut cursor = piece_start;
                    while cursor < piece_end && fi < files.len() {
                        let file_off_global = file_starts[fi];
                        let file_len = files[fi].length;
                        let file_end_global = file_off_global + file_len;
                        let start_in_file = cursor.saturating_sub(file_off_global);
                        let end_in_file = (piece_end.min(file_end_global)).saturating_sub(file_off_global);
                        let want_len = (end_in_file - start_in_file) as u64;

                        let rel_path = build_path(fi);
                        let url = if base.ends_with('/') { format!("{}{}", base, rel_path) } else { format!("{}/{}", base, rel_path) };
                        let range_header = format!("bytes={}-{}", start_in_file, start_in_file + want_len - 1);
                        match client.get(&url).header(reqwest::header::RANGE, range_header).send().await {
                            Ok(r) => {
                                let status = r.status();
                                if !(status == reqwest::StatusCode::PARTIAL_CONTENT || status == reqwest::StatusCode::OK) {
                                    let mut h = handle.write().await; h.abort_piece(piece); tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); piece_bytes.clear(); break;
                                }
                                match r.bytes().await {
                                    Ok(bytes) => {
                                        if bytes.len() as u64 != want_len && status != reqwest::StatusCode::OK {
                                            let mut h = handle.write().await; h.abort_piece(piece); tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); piece_bytes.clear(); break;
                                        }
                                        if status == reqwest::StatusCode::OK {
                                            // Full file returned; slice out the requested range
                                            let start = start_in_file as usize;
                                            let end = start + (want_len as usize);
                                            piece_bytes.extend_from_slice(&bytes[start..end]);
                                        } else {
                                            piece_bytes.extend_from_slice(&bytes);
                                        }
                                    }
                                    Err(_) => {
                                        let mut h = handle.write().await; h.abort_piece(piece);
                                        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); piece_bytes.clear(); break;
                                    }
                                }
                            }
                            Err(_) => {
                                let mut h = handle.write().await; h.abort_piece(piece);
                                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await; backoff = (backoff*2).min(60); piece_bytes.clear(); break;
                            }
                        }

                        cursor += want_len;
                        fi += 1;
                    }
                    if piece_bytes.len() != piece_len as usize { continue; }
                }

                // Write to disk and mark piece
                {
                    let h_ro = handle.read().await;
                    if let Some(dm) = h_ro.disk() {
                        if let Err(e) = dm.write_block(piece, 0, &piece_bytes).await { warn!(error = %e, piece = piece, "webseed write failed"); }
                    }
                }
                {
                    let mut h = handle.write().await;
                    let _ = h.record_block_and_maybe_complete(piece, 0, piece_len).await;
                }
                backoff = 5;
            }
        })
    }

    #[cfg(feature = "dht")]
    pub fn start_dht_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let peer_id = self.peer_id;
        let upload_slots = self.upload_slots.clone();
        let pipeline_max = self.config.pipeline_max_requests;
        let request_timeout_secs = self.config.request_timeout_secs;
        let opt_enabled = self.config.optimistic_unchoke_enabled;
        let opt_interval = self.config.optimistic_unchoke_interval_secs;
        let max_outbound = self.config.max_outbound_peers;
        let utp_enabled = self.config.utp_enabled;
        let listen_port = self.config.listen_addr.port();
        let bootstrap = self.config.dht_bootstrap.clone();
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) { Some(h) => h.clone(), None => return }
            };
            // Bind UDP DHT client socket (ephemeral)
            let local = std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);
            let client = match crate::net::dht::DhtClient::bind(local).await { Ok(c) => c, Err(_) => return };
            loop {
                // Exit if torrent removed or complete with no need to refresh? For minimal, always try.
                let torrents_ro = torrents.read().await;
                if !torrents_ro.contains_key(&info_hash) { break; }
                drop(torrents_ro);

                let h_for_cb = handle.clone();
                let upload_slots_for_cb = upload_slots.clone();
                let on_peers = move |peers: Vec<std::net::SocketAddr>| {
                    let h = h_for_cb.clone();
                    let upl = upload_slots_for_cb.clone();
                    tokio::spawn(async move {
                        let peers_with_flags: Vec<(std::net::SocketAddr, u8)> = peers.into_iter().map(|a| (a, 0u8)).collect();
                        let _ = dial_candidates(
                            h.clone(),
                            peer_id,
                            upl.clone(),
                            pipeline_max,
                            request_timeout_secs,
                            opt_enabled,
                            opt_interval,
                            max_outbound,
                            utp_enabled,
                            peers_with_flags,
                        ).await;
                    });
                };

                client.get_peers_and_announce(info_hash, listen_port, &bootstrap, on_peers).await;
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        })
    }
    /// Start Local Service Discovery (BEP14) for a torrent
    pub fn start_lsd_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let listen_port = self.config.listen_addr.port();
        let peer_id = self.peer_id;
        let upload_slots = self.upload_slots.clone();
        let pipeline_max = self.config.pipeline_max_requests;
        let request_timeout_secs = self.config.request_timeout_secs;
        let opt_enabled = self.config.optimistic_unchoke_enabled;
        let opt_interval = self.config.optimistic_unchoke_interval_secs;
        let max_outbound = self.config.max_outbound_peers;
        let utp_enabled = self.config.utp_enabled;
        tokio::spawn(async move {
            // Lookup handle once; exit if torrent gone
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) { Some(h) => h.clone(), None => return }
            };
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<std::net::SocketAddr>();
            // Start LSD socket in background
            let lsd_handle = crate::net::lsd::start_lsd(listen_port, info_hash, move |addr| {
                let _ = tx.send(addr);
            });
            if lsd_handle.is_err() { return; }
            let _jh = lsd_handle.unwrap();
            while let Some(addr) = rx.recv().await {
                // Dial single candidate via central helper (no flags)
                let _ = dial_candidates(
                    handle.clone(),
                    peer_id,
                    upload_slots.clone(),
                    pipeline_max,
                    request_timeout_secs,
                    opt_enabled,
                    opt_interval,
                    max_outbound,
                    utp_enabled,
                    vec![(addr, 0u8)],
                ).await;
            }
        })
    }


// Centralized candidate dialing policy for tracker/PEX/LSD will be defined below impl
    /// Remove a torrent from the session
    pub async fn remove_torrent(&self, info_hash: &InfoHash) -> Option<SharedTorrentHandle> {
        // remove handle
        let removed = {
            let mut torrents = self.torrents.write().await;
            torrents.remove(info_hash)
        };

        if let Some(h) = &removed {
            info!(infohash = hex::encode(info_hash), "Removed torrent from session");
            // fire-and-forget 'stopped' announce
            let h2 = h.clone();
            let peer_id = self.peer_id;
            let listen_port = self.config.listen_addr.port();
            let info_hash2 = *info_hash;
            tokio::spawn(async move {
                let (announce_url, total_size, downloaded) = {
                    let hh = h2.read().await;
                    (hh.meta().announce.clone(), hh.meta().info.total_size(), hh.stats().downloaded)
                };
                let req = crate::tracker::AnnounceRequest {
                    info_hash: info_hash2,
                    peer_id,
                    port: listen_port,
                    uploaded: 0,
                    downloaded,
                    left: crate::tracker::AnnounceRequest::left_from_total(total_size, downloaded),
                    compact: true,
                    numwant: Some(0),
                    event: Some(crate::tracker::AnnounceEvent::Stopped),
                    key: None,
                    trackerid: None,
                };
                if announce_url.starts_with("udp://") {
                    let _ = crate::tracker::udp::announce(&announce_url, &req).await;
                } else {
                    let url2 = crate::tracker::http::build_announce_url(&announce_url, &req);
                    let _ = crate::tracker::http::announce(&url2).await;
                }
            });
        }

        removed
    }

    /// Get a torrent handle by info hash
    pub async fn get_torrent(&self, info_hash: &InfoHash) -> Option<SharedTorrentHandle> {
        let torrents = self.torrents.read().await;
        torrents.get(info_hash).cloned()
    }

    /// List all active torrents
    pub async fn list_torrents(&self) -> Vec<(InfoHash, SharedTorrentHandle)> {
        let torrents = self.torrents.read().await;
        torrents.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Get number of active torrents
    pub async fn num_torrents(&self) -> usize {
        let torrents = self.torrents.read().await;
        torrents.len()
    }

    pub fn spawn(self) -> JoinHandle<Result<(), LibtorrentError>> {
        tokio::spawn(async move { self.run().await })
    }

    pub async fn run(self) -> Result<(), LibtorrentError> {
        let Session { config, peer_id, torrents: _, upload_slots } = self;
        let listener = TcpListener::bind(config.listen_addr).await?;
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "libtorrent session listening");
        let semaphore = Arc::new(Semaphore::new(config.max_pending_peers));

        // Optionally start a uTP listener (feature-gated)
        #[cfg(feature = "utp")]
        if config.utp_enabled {
            let utp_addr = config.utp_bind.unwrap_or(config.listen_addr);
            let sem_utp = semaphore.clone();
            let upload_slots_utp = upload_slots.clone();
            let peer_id_utp = peer_id;
            let info_hash = config.info_hash;
            tokio::spawn(async move {
                let _ = crate::net::utp::listen(utp_addr, move |transport, remote_addr| {
                    let _permit = match sem_utp.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let per_peer_limit = 0u64; // reuse same logic as TCP inbound
                    let session = PeerSession::new(
                        transport,
                        remote_addr,
                        info_hash,
                        peer_id_utp,
                        None,
                        Some(upload_slots_utp.clone()),
                        per_peer_limit,
                        config.pipeline_max_requests,
                        config.request_timeout_secs,
                        config.optimistic_unchoke_enabled,
                        config.optimistic_unchoke_interval_secs,
                    );
                    tokio::spawn(async move {
                        if let Err(err) = session.run().await {
                            warn!(%remote_addr, error = %err, "uTP peer session ended");
                        }
                    });
                }).await;
            });
        }
        loop {
            let (socket, remote_addr) = listener.accept().await?;
            socket.set_nodelay(true).ok();
            let transport = transport::from_tcp(socket);
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    warn!(%remote_addr, "dropping connection: backlog full");
                    continue;
                }
            };
            let info_hash = config.info_hash;
            let peer_id = peer_id;
            let upload_slots = upload_slots.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let per_peer_limit = if config.upload_rate_limit > 0 && config.max_upload_slots > 0 { config.upload_rate_limit / config.max_upload_slots as u64 } else { 0 };
                let session = PeerSession::new(
                    transport,
                    remote_addr,
                    info_hash,
                    peer_id,
                    None,
                    Some(upload_slots),
                    per_peer_limit,
                    config.pipeline_max_requests,
                    config.request_timeout_secs,
                    config.optimistic_unchoke_enabled,
                    config.optimistic_unchoke_interval_secs,
                );
                if let Err(err) = session.run().await {
                    warn!(%remote_addr, error = %err, "peer session ended");
                }
            });
        }
    }

    /// Start an HTTP tracker announce task for a torrent
    pub fn start_http_tracker_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let peer_id = self.peer_id;
        let listen_port = self.config.listen_addr.port();
        let upload_slots = self.upload_slots.clone();
        let pipeline_max = self.config.pipeline_max_requests;
        let request_timeout_secs = self.config.request_timeout_secs;
        let opt_enabled = self.config.optimistic_unchoke_enabled;
        let opt_interval = self.config.optimistic_unchoke_interval_secs;
        let max_outbound = self.config.max_outbound_peers;
        let utp_enabled = self.config.utp_enabled;
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) {
                    Some(h) => h.clone(),
                    None => return,
                }
            };
            // Gather announce URL once
            let announce_url = {
                let h = handle.read().await;
                h.meta().announce.clone()
            };

            let mut backoff: u64 = 15; // seconds
            let mut started_sent = false;
            let mut completed_sent = false;
            let mut last_trackerid: Option<String> = None;
            loop {
                // Snapshot stats
                let (total_size, downloaded) = {
                    let h = handle.read().await;
                    (h.meta().info.total_size(), h.stats().downloaded)
                };
                let event = if !started_sent {
                    started_sent = true; Some(crate::tracker::AnnounceEvent::Started)
                } else {
                    let complete_now = {
                        let h = handle.read().await; h.is_complete()
                    };
                    if !completed_sent && complete_now { completed_sent = true; Some(crate::tracker::AnnounceEvent::Completed) } else { None }
                };
                let req = AnnounceRequest {
                    info_hash,
                    peer_id,
                    port: listen_port,
                    uploaded: 0,
                    downloaded,
                    left: AnnounceRequest::left_from_total(total_size, downloaded),
                    compact: true,
                    numwant: Some(50),
                    event,
                    key: None,
                    trackerid: last_trackerid.clone(),
                };

                let url = tracker_http::build_announce_url(&announce_url, &req);
                match tracker_http::announce(&url).await {
                    Ok(resp) => {
                        let next_interval = (resp.interval as u64).max(10);
                        backoff = 15;
                        info!(interval = resp.interval, peers = resp.peers.len(), "tracker announce ok");
                        if let Some(id) = resp.tracker_id { last_trackerid = Some(id); }
                        let peers_with_flags: Vec<(SocketAddr,u8)> = resp.peers.into_iter().map(|a| (a, 0u8)).collect();
                        let _ = dial_candidates(
                            handle.clone(),
                            peer_id,
                            upload_slots.clone(),
                            pipeline_max,
                            request_timeout_secs,
                            opt_enabled,
                            opt_interval,
                            max_outbound,
                            utp_enabled,
                            peers_with_flags,
                        ).await;
                        tokio::time::sleep(std::time::Duration::from_secs(next_interval)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, url = %url, "tracker announce failed");
                        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                        backoff = (backoff.saturating_mul(2)).min(900);
                    }
                }
            }
        })
    }

    /// Start a UDP tracker announce task for a torrent
    pub fn start_udp_tracker_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let peer_id = self.peer_id;
        let listen_port = self.config.listen_addr.port();
        let upload_slots = self.upload_slots.clone();
        let pipeline_max = self.config.pipeline_max_requests;
        let request_timeout_secs = self.config.request_timeout_secs;
        let opt_enabled = self.config.optimistic_unchoke_enabled;
        let opt_interval = self.config.optimistic_unchoke_interval_secs;
        let max_outbound = self.config.max_outbound_peers;
        let utp_enabled = self.config.utp_enabled;
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) {
                    Some(h) => h.clone(),
                    None => return,
                }
            };
            // Gather announce URL once
            let announce_url = {
                let h = handle.read().await;
                h.meta().announce.clone()
            };

            let mut backoff: u64 = 15; // seconds
            let mut started_sent = false;
            let mut completed_sent = false;
            loop {
                // Snapshot stats
                let (total_size, downloaded) = {
                    let h = handle.read().await;
                    (h.meta().info.total_size(), h.stats().downloaded)
                };
                let event = if !started_sent {
                    started_sent = true; Some(crate::tracker::AnnounceEvent::Started)
                } else {
                    let complete_now = {
                        let h = handle.read().await; h.is_complete()
                    };
                    if !completed_sent && complete_now { completed_sent = true; Some(crate::tracker::AnnounceEvent::Completed) } else { None }
                };
                let req = AnnounceRequest {
                    info_hash,
                    peer_id,
                    port: listen_port,
                    uploaded: 0,
                    downloaded,
                    left: AnnounceRequest::left_from_total(total_size, downloaded),
                    compact: true,
                    numwant: Some(50),
                    event,
                    key: None,
                    trackerid: None,
                };

                match crate::tracker::udp::announce(&announce_url, &req).await {
                    Ok(resp) => {
                        let next_interval = (resp.interval as u64).max(10);
                        backoff = 15;
                        info!(interval = resp.interval, peers = resp.peers.len(), "udp tracker announce ok");
                        let peers_with_flags: Vec<(SocketAddr,u8)> = resp.peers.into_iter().map(|a| (a, 0u8)).collect();
                        let _ = dial_candidates(
                            handle.clone(),
                            peer_id,
                            upload_slots.clone(),
                            pipeline_max,
                            request_timeout_secs,
                            opt_enabled,
                            opt_interval,
                            max_outbound,
                            utp_enabled,
                            peers_with_flags,
                        ).await;
                        tokio::time::sleep(std::time::Duration::from_secs(next_interval)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, url = %announce_url, "udp tracker announce failed");
                        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                        backoff = (backoff.saturating_mul(2)).min(900);
                    }
                }
            }
        })
    }

    /// Start a tit-for-tat choke manager for a torrent
    pub fn start_choke_manager_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let max_slots = self.config.max_upload_slots as usize;
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) {
                    Some(h) => h.clone(),
                    None => return,
                }
            };
            let mut optimistic_idx: usize = 0;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                // take recent downloads and decide winners
                let peers_snapshot = {
                    let mut h = handle.write().await;
                    h.take_interested_recent_downloads()
                };
                if peers_snapshot.is_empty() { continue; }
                let mut peers_sorted = peers_snapshot.clone();
                peers_sorted.sort_by(|a,b| b.1.cmp(&a.1));
                let mut winners: std::collections::HashSet<SocketAddr> = std::collections::HashSet::new();
                let fixed_winners = peers_sorted.iter().take(max_slots.saturating_sub(1)).map(|(a,_,_)| *a);
                for addr in fixed_winners { winners.insert(addr); }
                // optimistic unchoke among remaining interested peers not already winners
                let remaining: Vec<SocketAddr> = peers_sorted.iter()
                    .filter(|(a,_,_)| !winners.contains(a))
                    .map(|(a,_,_)| *a)
                    .collect();
                if !remaining.is_empty() {
                    let pick = optimistic_idx % remaining.len();
                    winners.insert(remaining[pick]);
                    optimistic_idx = optimistic_idx.wrapping_add(1);
                }
                // Broadcast decisions
                let handle_ro = handle.read().await;
                for (addr, _bytes, _am_choking) in peers_sorted.iter().cloned() {
                    if winners.contains(&addr) {
                        handle_ro.broadcast_unchoke(addr);
                    } else {
                        handle_ro.broadcast_choke(addr);
                    }
                }
            }
        })
    }

    /// Unified tracker manager with tiered trackers and optional HTTP scrape
    pub fn start_tracker_manager_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let peer_id = self.peer_id;
        let listen_port = self.config.listen_addr.port();
        let upload_slots = self.upload_slots.clone();
        let pipeline_max = self.config.pipeline_max_requests;
        let request_timeout_secs = self.config.request_timeout_secs;
        let opt_enabled = self.config.optimistic_unchoke_enabled;
        let opt_interval = self.config.optimistic_unchoke_interval_secs;
        let max_outbound = self.config.max_outbound_peers;
        let _utp_enabled = self.config.utp_enabled; // reserved for future use
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) {
                    Some(h) => h.clone(),
                    None => return,
                }
            };

            let mut backoff: u64 = 15; // seconds on total failure
            // keep last trackerid per URL for HTTP trackers
            let mut http_tracker_ids: std::collections::HashMap<String, String> = std::collections::HashMap::new();
            // static key per torrent (string) for HTTP trackers
            let http_key: Option<String> = Some(format!("{:08x}", rand::random::<u32>()));

            loop {
                // snapshot tiers
                let tiers = {
                    let h = handle.read().await;
                    h.meta().trackers_by_tier()
                };

                let mut any_success = false;
                let mut next_interval: u64 = 60;

                'tiers: for tier in tiers {
                    for url in tier {
                        // Snapshot stats for this announce
                        let (total_size, downloaded) = {
                            let h = handle.read().await;
                            (h.meta().info.total_size(), h.stats().downloaded)
                        };
                        let last_id = http_tracker_ids.get(&url).cloned();
                        let req = crate::tracker::AnnounceRequest {
                            info_hash,
                            peer_id,
                            port: listen_port,
                            uploaded: 0,
                            downloaded,
                            left: crate::tracker::AnnounceRequest::left_from_total(total_size, downloaded),
                            compact: true,
                            numwant: Some(50),
                            event: None,
                            key: http_key.clone(),
                            trackerid: last_id,
                        };
                        let announce_result = if url.starts_with("udp://") {
                            crate::tracker::udp::announce(&url, &req).await.map(|r| (r, false))
                        } else {
                            let built = crate::tracker::http::build_announce_url(&url, &req);
                            crate::tracker::http::announce(&built).await.map(|r| (r, true))
                        };
                        match announce_result {
                            Ok((resp, is_http)) => {
                                any_success = true;
                                next_interval = (resp.interval as u64).max(10);
                                if is_http {
                                    if let Some(id) = resp.tracker_id.clone() { http_tracker_ids.insert(url.clone(), id); }
                                    // Best-effort scrape
                                    if let Some(scrape_url) = crate::tracker::http::build_scrape_url(&url) {
                                        if let Ok(scr) = crate::tracker::http::scrape(&scrape_url, &info_hash).await {
                                            if let Some(complete) = scr.complete { info!(complete, "scrape complete"); }
                                            if let Some(incomplete) = scr.incomplete { info!(incomplete, "scrape incomplete"); }
                                        }
                                    }
                                } else {
                                    // UDP scrape (best-effort)
                                    if let Ok(scr) = crate::tracker::udp::scrape(&url, &info_hash).await {
                                        if let Some(complete) = scr.complete { info!(complete, "scrape complete"); }
                                        if let Some(incomplete) = scr.incomplete { info!(incomplete, "scrape incomplete"); }
                                    }
                                }

                                // Build known set to dedupe
                                let mut known = std::collections::HashSet::new();
                                {
                                    let h = handle.read().await;
                                    for p in h.peers() { known.insert(p.addr); }
                                }

                                // Cap how many to connect
                                let current_count = known.len();
                                let mut allowed_new = if current_count >= max_outbound { 0 } else { max_outbound - current_count };

                                for addr in resp.peers {
                                    if allowed_new == 0 { break; }
                                    if known.contains(&addr) { continue; }

                                    // Add to peers list
                                    {
                                        let mut h = handle.write().await;
                                        let num_pieces = h.meta().info.num_pieces();
                                        let pi = crate::torrent::PeerInfo {
                                            addr,
                                            peer_id: [0u8; 20],
                                            bitfield: vec![false; num_pieces],
                                            peer_choking: true,
                                            am_interested: false,
                                            am_choking: true,
                                            peer_interested: false,
                                            recent_downloaded: 0,
                                            pex_flags: 0,
                                        };
                                        h.add_peer(pi);
                                    }

                                    known.insert(addr);
                                    allowed_new -= 1;

                                    let transport_opt: Option<crate::net::transport::Transport> = match tokio::net::TcpStream::connect(addr).await {
                                        Ok(s) => Some(transport::from_tcp(s)),
                                        Err(_) => None,
                                    };
                                    if let Some(stream) = transport_opt {
                                        let h = handle.read().await;
                                        let info_hash_local = h.meta().info.raw_infohash;
                                        drop(h);
                                        let per_peer_limit = 0u64;
                                        let session = crate::peer::PeerSession::new(
                                            stream,
                                            addr,
                                            info_hash_local,
                                            peer_id,
                                            Some(handle.clone()),
                                            Some(upload_slots.clone()),
                                            per_peer_limit,
                                            pipeline_max,
                                            request_timeout_secs,
                                            opt_enabled,
                                            opt_interval,
                                        );
                                        tokio::spawn(async move {
                                            if let Err(e) = session.run().await {
                                                warn!(peer = %addr, error = %e, "peer session failed");
                                            }
                                        });
                                    }
                                }
                                break 'tiers; // stick to this tier/tracker for this cycle
                            }
                            Err(e) => {
                                warn!(error = %e, url = %url, "tracker announce failed");
                                continue; // try next tracker in tier
                            }
                        }
                    }
                }

                if any_success {
                    backoff = 15;
                    tokio::time::sleep(std::time::Duration::from_secs(next_interval)).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                    backoff = (backoff.saturating_mul(2)).min(900);
                }
            }
        })
    }

    /// Start a PEX connector that listens for PEX events and dials peers
    pub fn start_pex_connector_for(&self, info_hash: InfoHash) -> JoinHandle<()> {
        let torrents = self.torrents.clone();
        let peer_id = self.peer_id;
        let upload_slots = self.upload_slots.clone();
        let pipeline_max = self.config.pipeline_max_requests;
        let request_timeout_secs = self.config.request_timeout_secs;
        let opt_enabled = self.config.optimistic_unchoke_enabled;
        let opt_interval = self.config.optimistic_unchoke_interval_secs;
        let max_outbound = self.config.max_outbound_peers;
        let utp_enabled = self.config.utp_enabled;
        tokio::spawn(async move {
            // Lookup torrent handle
            let handle = {
                let map = torrents.read().await;
                match map.get(&info_hash) {
                    Some(h) => h.clone(),
                    None => return,
                }
            };

            // Subscribe to torrent events
            let mut rx = { handle.read().await.events_subscribe() };
            loop {
                let evt = match rx.recv().await { Ok(e) => e, Err(_) => break };
                match evt {
                    crate::torrent::TorrentEvent::PexAddedWithFlags(peers) => {
                        if peers.is_empty() { continue; }
                        let _ = dial_candidates(
                            handle.clone(),
                            peer_id,
                            upload_slots.clone(),
                            pipeline_max,
                            request_timeout_secs,
                            opt_enabled,
                            opt_interval,
                            max_outbound,
                            utp_enabled,
                            peers,
                        ).await;
                    }
                    crate::torrent::TorrentEvent::PexAdded(peers) => {
                        if peers.is_empty() { continue; }
                        let peers_with_flags: Vec<(SocketAddr, u8)> = peers.into_iter().map(|a| (a, 0u8)).collect();
                        let _ = dial_candidates(
                            handle.clone(),
                            peer_id,
                            upload_slots.clone(),
                            pipeline_max,
                            request_timeout_secs,
                            opt_enabled,
                            opt_interval,
                            max_outbound,
                            utp_enabled,
                            peers_with_flags,
                        ).await;
                    }
                    crate::torrent::TorrentEvent::PexDropped(peers) => {
                        if peers.is_empty() { continue; }
                        // Remove peers from torrent handle if present
                        let mut h = handle.write().await;
                        for addr in peers { h.remove_peer(&addr); }
                        // Stats auto-updated by remove_peer
                    }
                    _ => {}
                }
            }
        })
    }
}
