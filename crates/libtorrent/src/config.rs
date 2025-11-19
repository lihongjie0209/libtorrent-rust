use libtorrent_proto::HASH_LEN;
use rand::RngCore;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub listen_addr: SocketAddr,
    pub info_hash: [u8; HASH_LEN],
    pub peer_id_prefix: String,
    pub max_pending_peers: usize,
    pub download_dir: PathBuf,
    pub max_outbound_peers: usize,
    pub max_upload_slots: usize,
    pub upload_rate_limit: u64,
    pub pipeline_max_requests: usize,
    pub request_timeout_secs: u64,
    pub optimistic_unchoke_enabled: bool,
    pub optimistic_unchoke_interval_secs: u64,
    pub utp_enabled: bool,
    pub utp_bind: Option<SocketAddr>,
    pub dht_enabled: bool,
    pub dht_bootstrap: Vec<String>,
}

impl SessionConfig {
    pub fn new(listen_addr: SocketAddr, info_hash: [u8; HASH_LEN]) -> Self {
        Self {
            listen_addr,
            info_hash,
            peer_id_prefix: "-LT0001-".to_string(),
            max_pending_peers: 1024,
            download_dir: PathBuf::from("downloads"),
            max_outbound_peers: 80,
            max_upload_slots: 8,
            upload_rate_limit: 0,
            pipeline_max_requests: 8,
            request_timeout_secs: 30,
            optimistic_unchoke_enabled: false,
            optimistic_unchoke_interval_secs: 30,
            utp_enabled: false,
            utp_bind: None,
            dht_enabled: false,
            dht_bootstrap: vec![
                "router.bittorrent.com:6881".to_string(),
                "dht.transmissionbt.com:6881".to_string(),
                "router.utorrent.com:6881".to_string(),
            ],
        }
    }

    pub fn with_random_info_hash(listen_addr: SocketAddr) -> Self {
        let mut hash = [0u8; HASH_LEN];
        rand::thread_rng().fill_bytes(&mut hash);
        Self::new(listen_addr, hash)
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self::with_random_info_hash(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
    }
}
