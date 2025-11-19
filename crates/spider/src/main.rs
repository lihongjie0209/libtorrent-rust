use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use hex::ToHex;
use libtorrent::net::dht::DhtClient;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Instant};
use tracing::{info, warn};
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "spider", about = "DHT metadata spider", version)]
struct SpiderConfig {
    #[arg(long, short = 'w', env = "SPIDER_WORKERS", default_value_t = 1000, help = "Number of DHT workers")]
        workers: usize,

    #[arg(long, short = 'm', alias = "meta-conc", env = "SPIDER_META_CONC", default_value_t = 64, help = "Concurrent metadata fetches")]
        metadata_concurrency: usize,

    #[arg(long, alias = "out", value_name = "DIR", env = "SPIDER_OUT", help = "Directory to store fetched metadata")]
        output_dir: Option<std::path::PathBuf>,

    #[arg(long, value_name = "PORT", env = "SPIDER_START_PORT", default_value_t = 40000, help = "Starting UDP port for DHT workers (increments sequentially)")]
        start_port: u16,

        #[arg(long = "bootstrap", value_name = "HOST:PORT", num_args = 0..,
                    default_values_t = vec![
                        String::from("router.bittorrent.com:6881"),
                        String::from("dht.transmissionbt.com:6881"),
                        String::from("router.utorrent.com:6881"),
                    ],
                    help = "Bootstrap DHT routers (repeatable)")]
        bootstrap: Vec<String>,
}

fn gen_target(i: usize) -> [u8; 20] {
        // simple evenly spaced targets across 160-bit ring
        let mut t = [0u8; 20];
        let n = i as u128;
        let step: u128 = 1u128 << 64; // coarse spacing
        let val = n.wrapping_mul(step);
        let vb = val.to_be_bytes(); // 16 bytes
        t[0..16].copy_from_slice(&vb);
        t[16..20].copy_from_slice(&(i as u32).to_be_bytes());
        t
}

fn gen_node_id(i: usize) -> [u8; 20] {
    let mut id = [0u8; 20];
    id[0..8].copy_from_slice(&(i as u64).to_be_bytes());
    id[8] = 0x42;
    id[19] = (i as u8).wrapping_mul(13);
    id
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(false).init();

    // Parse CLI arguments
    let cfg = SpiderConfig::parse();

    info!(workers = cfg.workers, meta_conc = cfg.metadata_concurrency, out = ?cfg.output_dir, "starting spider");

    let (tx_ih, mut rx_ih) = mpsc::unbounded_channel::<[u8; 20]>();

    // Spawn DHT workers
    for i in 0..cfg.workers {
        let tx = tx_ih.clone();
        let bootstrap = cfg.bootstrap.clone();
        let port_u32 = cfg.start_port as u32 + i as u32;
        if port_u32 > 65535 { warn!(start_port = cfg.start_port, i, "port range exceeded; skipping worker"); break; }
        let bind_port = port_u32 as u16;
        tokio::spawn(async move {
            let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), bind_port);
            let node_id = gen_node_id(i);
            let client = match DhtClient::bind_with_id(local, node_id).await { Ok(c) => c, Err(e) => { warn!(error = %e, "dht bind failed"); return; } };
            let target = gen_target(i);
            info!(worker = i, port = bind_port, node_id = %hex::encode(node_id), target = %hex::encode(target), "DHT worker started");
            loop {
                let tx2 = tx.clone();
                let worker_id = i;
                let on_samples = move |samples: Vec<[u8; 20]>| {
                    info!(worker = worker_id, samples = samples.len(), "received infohash samples");
                    for ih in samples { let _ = tx2.send(ih); }
                };
                info!(worker = i, "sampling infohashes");
                client.sample_infohashes(&target, &bootstrap, on_samples).await;
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // Metadata fetchers
    let (tx_meta, rx_meta) = mpsc::unbounded_channel::<([u8;20], Vec<SocketAddr>)>();

    // Coordinator: collect infohashes, query peers via DHT, enqueue to metadata workers
    let coord_bootstrap = cfg.bootstrap.clone();
    tokio::spawn(async move {
        let mut seen: HashSet<[u8;20]> = HashSet::new();
        while let Some(ih) = rx_ih.recv().await {
            if !seen.insert(ih) { continue; }
            info!(ih = %hex::encode(ih), "coordinator received infohash");
            let (tx_peers, mut rx_peers) = mpsc::unbounded_channel::<SocketAddr>();
            let on_peers = move |peers: Vec<SocketAddr>| { for p in peers { let _ = tx_peers.send(p); } };
            let ih_copy = ih;
            // spawn a short gather with a fresh ephemeral DHT client
            let coord_bootstrap2 = coord_bootstrap.clone();
            tokio::spawn(async move {
                let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
                if let Ok(client2) = DhtClient::bind(local).await {
                    info!(ih = %hex::encode(ih_copy), "start get_peers");
                    client2.get_peers_and_announce(ih_copy, 0, &coord_bootstrap2, on_peers).await;
                    info!(ih = %hex::encode(ih_copy), "get_peers done");
                }
            });
            let mut peers: Vec<SocketAddr> = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if let Ok(p) = tokio::time::timeout(Duration::from_millis(200), rx_peers.recv()).await {
                    if let Some(addr) = p { peers.push(addr); if peers.len() >= 32 { break; } } else { break; }
                }
            }
            info!(ih = %hex::encode(ih), peers = peers.len(), "coordinator collected peers");
            if !peers.is_empty() { let _ = tx_meta.send((ih, peers)); }
        }
    });

    // Metadata downloads with concurrency-limited dispatcher
    let out_dir = cfg.output_dir.clone();
    let conc = cfg.metadata_concurrency;
    let sem = std::sync::Arc::new(Semaphore::new(conc));
    tokio::spawn(async move {
        let mut rx = rx_meta;
        while let Some((ih, peers)) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.expect("semaphore");
            let out = out_dir.clone();
            tokio::spawn(async move {
                let _permit = permit;
                info!(ih = %hex::encode(ih), peers = peers.len(), "metadata fetch start");
                if let Err(e) = fetch_metadata_for_infohash(ih, peers.clone(), out.as_ref()).await {
                    warn!(ih = %hex::encode(ih), error = %e, "metadata fetch failed");
                    // simple delayed retry
                    let out2 = out.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_secs(30)).await;
                        info!(ih = %hex::encode(ih), "retry metadata fetch");
                        let _ = fetch_metadata_for_infohash(ih, peers, out2.as_ref()).await;
                    });
                }
            });
        }
    });

    // Keep main alive
    loop { sleep(Duration::from_secs(60)).await; }
}

async fn fetch_metadata_for_infohash(ih: [u8;20], peers: Vec<SocketAddr>, out_dir: Option<&std::path::PathBuf>) -> anyhow::Result<()> {
    use libtorrent::net::transport;
    use libtorrent::peer::PeerSession;
    use libtorrent::torrent::{TorrentHandle, SharedTorrentHandle};
    use tokio::sync::RwLock;

    // Build a minimal handle for applying metadata into
    let dummy_meta = libtorrent::metainfo::TorrentMeta {
        announce: String::new(),
        announce_list: Vec::new(),
        web_seeds: Vec::new(),
        info: libtorrent::metainfo::InfoDict {
            name: String::new(), piece_length: 16384, pieces: Vec::new(), files: Vec::new(), private: false, raw_infohash: [0u8;20], raw_info_bencode: Vec::new()
        }
    };
    let handle: SharedTorrentHandle = Arc::new(RwLock::new(TorrentHandle::new(dummy_meta)));

    // Try a few peers
    for addr in peers.into_iter().take(8) {
        // Connect TCP
        info!(ih = %hex::encode(ih), peer = %addr, "connect peer");
        if let Ok(s) = tokio::net::TcpStream::connect(addr).await {
            s.set_nodelay(true).ok();
            let transport = transport::from_tcp(s);
            let session = PeerSession::new(
                transport,
                addr,
                ih,
                libtorrent_proto::Handshake::random_peer_id(b"-SPDR01-"),
                Some(handle.clone()),
                None,
                0,
                1,
                15,
                false,
                60,
            );
            // Run with timeout
            info!(ih = %hex::encode(ih), peer = %addr, "peer session start");
            let res = tokio::time::timeout(Duration::from_secs(20), session.run()).await;
            let _ = res; // ignore run result
            info!(ih = %hex::encode(ih), peer = %addr, "peer session end");
        }
        // Check if metadata applied
        if let Ok(h) = handle.try_read() {
            if !h.meta().info.raw_info_bencode.is_empty() {
                let data = h.meta().info.raw_info_bencode.clone();
                if let Some(dir) = out_dir { 
                    let fname = dir.join(format!("{}.meta", ih.encode_hex::<String>()));
                    if let Err(e) = tokio::fs::create_dir_all(dir).await { warn!(error = %e, "mkdir failed"); }
                    if let Err(e) = tokio::fs::write(&fname, &data).await { warn!(error = %e, file = %fname.display(), "write failed"); }
                }
                info!(ih = %hex::encode(ih), bytes = data.len(), "metadata fetched");
                return Ok(());
            }
        }
    }
    anyhow::bail!("no metadata from peers")
}
