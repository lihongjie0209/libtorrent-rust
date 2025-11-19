use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};

use hex::ToHex;
use libtorrent::net::dht::DhtClient;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Instant};
use tracing::{info, warn};
use clap::Parser;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
                        String::from("dht.libtorrent.org:25401"),
                        String::from("router.silotis.us:6881"),
                        String::from("dht.aelitis.com:6881"),
                        String::from("router.bitcomet.com:6881"),
                    ],
                    help = "Bootstrap DHT routers (repeatable)")]
        bootstrap: Vec<String>,

    #[arg(long, alias = "dedup-db", value_name = "FILE", env = "SPIDER_DEDUP_DB", help = "Path to persistent deduplication database file")]
        dedup_db_path: Option<PathBuf>,

    #[arg(long, alias = "dedup-persist-interval", value_name = "SECS", env = "SPIDER_DEDUP_PERSIST_INTERVAL", default_value_t = 300, help = "Interval in seconds to persist deduplication state to disk")]
        dedup_persist_interval: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeduplicationState {
    seen_infohashes: HashSet<[u8; 20]>,
}

struct DeduplicationManager {
    seen: Arc<DashMap<[u8; 20], ()>>,
    db_path: Option<PathBuf>,
}

#[derive(Clone)]
struct Metrics {
    // 发现的infohash总数
    infohashes_discovered: Arc<AtomicU64>,
    // 上一次统计时的infohash数量
    last_infohashes_discovered: Arc<AtomicU64>,
    // 元数据获取尝试次数
    metadata_attempts: Arc<AtomicU64>,
    // 元数据获取成功次数
    metadata_success: Arc<AtomicU64>,
    // 每个worker的统计
    worker_samples: Arc<DashMap<usize, Arc<AtomicU64>>>,
    // 网络统计
    bytes_sent: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            infohashes_discovered: Arc::new(AtomicU64::new(0)),
            last_infohashes_discovered: Arc::new(AtomicU64::new(0)),
            metadata_attempts: Arc::new(AtomicU64::new(0)),
            metadata_success: Arc::new(AtomicU64::new(0)),
            worker_samples: Arc::new(DashMap::new()),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_received: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_infohash_discovered(&self) {
        self.infohashes_discovered.fetch_add(1, Ordering::Relaxed);
    }

    fn record_metadata_attempt(&self) {
        self.metadata_attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_metadata_success(&self) {
        self.metadata_success.fetch_add(1, Ordering::Relaxed);
    }

    fn record_worker_samples(&self, worker_id: usize, count: u64) {
        let counter = self.worker_samples.entry(worker_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(count, Ordering::Relaxed);
    }

    fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    fn print_report(&self, interval_secs: u64) {
        let total_discovered = self.infohashes_discovered.load(Ordering::Relaxed);
        let last_discovered = self.last_infohashes_discovered.load(Ordering::Relaxed);
        let delta = total_discovered.saturating_sub(last_discovered);
        let rate_per_min = (delta as f64 / interval_secs as f64) * 60.0;
        
        let attempts = self.metadata_attempts.load(Ordering::Relaxed);
        let success = self.metadata_success.load(Ordering::Relaxed);
        let success_rate = if attempts > 0 {
            (success as f64 / attempts as f64) * 100.0
        } else {
            0.0
        };

        let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
        let bytes_received = self.bytes_received.load(Ordering::Relaxed);
        let mb_sent = bytes_sent as f64 / 1024.0 / 1024.0;
        let mb_received = bytes_received as f64 / 1024.0 / 1024.0;
        let bandwidth_mbps_sent = (mb_sent / interval_secs as f64) * 8.0;
        let bandwidth_mbps_received = (mb_received / interval_secs as f64) * 8.0;

        info!("========== 指标报告 ==========");
        info!("发现速率: {:.2} infohash/分钟 (总计: {})", rate_per_min, total_discovered);
        info!("元数据获取: {}/{} ({:.1}% 成功率)", success, attempts, success_rate);
        info!("网络带宽: ↑ {:.2} Mbps ({:.2} MB) | ↓ {:.2} Mbps ({:.2} MB)", 
            bandwidth_mbps_sent, mb_sent, bandwidth_mbps_received, mb_received);
        
        // Worker效率统计
        let mut worker_stats: Vec<_> = self.worker_samples.iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Relaxed)))
            .collect();
        worker_stats.sort_by_key(|&(id, _)| id);
        
        if !worker_stats.is_empty() {
            info!("Worker效率 (前10个):");
            for (id, count) in worker_stats.iter().take(10) {
                info!("  Worker #{}: {} samples", id, count);
            }
            let total_samples: u64 = worker_stats.iter().map(|(_, c)| c).sum();
            let avg_samples = total_samples as f64 / worker_stats.len() as f64;
            info!("  平均: {:.1} samples/worker", avg_samples);
        } else {
            info!("Worker效率: 暂无数据 (DHT连接建立中...)");
        }
        
        info!("===============================");

        // 更新基准值
        self.last_infohashes_discovered.store(total_discovered, Ordering::Relaxed);
    }
}

impl DeduplicationManager {
    fn new(db_path: Option<PathBuf>) -> Arc<Self> {
        let seen = Arc::new(DashMap::new());
        
        // Load existing state from disk if available
        if let Some(ref path) = db_path {
            if path.exists() {
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        match serde_json::from_str::<DeduplicationState>(&content) {
                            Ok(state) => {
                                info!(count = state.seen_infohashes.len(), "loaded dedup state from disk");
                                for ih in state.seen_infohashes {
                                    seen.insert(ih, ());
                                }
                            }
                            Err(e) => warn!(error = %e, "failed to parse dedup state"),
                        }
                    }
                    Err(e) => warn!(error = %e, "failed to read dedup state"),
                }
            }
        }
        
        Arc::new(Self { seen, db_path })
    }

    fn is_seen(&self, infohash: &[u8; 20]) -> bool {
        self.seen.contains_key(infohash)
    }

    fn mark_seen(&self, infohash: [u8; 20]) -> bool {
        self.seen.insert(infohash, ()).is_none()
    }

    async fn persist(&self) -> anyhow::Result<()> {
        if let Some(ref path) = self.db_path {
            let seen_set: HashSet<[u8; 20]> = self.seen.iter().map(|entry| *entry.key()).collect();
            let state = DeduplicationState { seen_infohashes: seen_set };
            let json = serde_json::to_string(&state)?;
            
            // Create parent directory if needed
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            
            tokio::fs::write(path, json.as_bytes()).await?;
            info!(count = state.seen_infohashes.len(), path = %path.display(), "persisted dedup state");
        }
        Ok(())
    }

    fn stats(&self) -> (usize, Option<PathBuf>) {
        (self.seen.len(), self.db_path.clone())
    }
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

    // Initialize metrics
    let metrics = Metrics::new();

    let (tx_ih, mut rx_ih) = mpsc::unbounded_channel::<[u8; 20]>();

    // Validate port range
    let max_workers = (65535 - cfg.start_port as u32 + 1) as usize;
    let actual_workers = cfg.workers.min(max_workers);
    if actual_workers < cfg.workers {
        warn!(requested = cfg.workers, actual = actual_workers, "reduced worker count due to port range");
    }

    // Spawn DHT workers
    for i in 0..actual_workers {
        let tx = tx_ih.clone();
        let bootstrap = cfg.bootstrap.clone();
        let bind_port = cfg.start_port + i as u16;
        let worker_metrics = metrics.clone();
        tokio::spawn(async move {
            let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), bind_port);
            let node_id = gen_node_id(i);
            let client = match DhtClient::bind_with_id(local, node_id).await { 
                Ok(c) => {
                    let addr = c.local_addr().unwrap_or(local);
                    info!(worker = i, port = bind_port, local_addr = %addr, "DHT client bound successfully");
                    c
                }, 
                Err(e) => { 
                    warn!(worker = i, error = %e, port = bind_port, "dht bind failed"); 
                    return; 
                } 
            };
            let target = gen_target(i);
            info!(worker = i, port = bind_port, node_id = %hex::encode(node_id), target = %hex::encode(target), "DHT worker started");
            
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let tx2 = tx.clone();
                let worker_id = i;
                let wm = worker_metrics.clone();
                let received_any = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let received_flag = received_any.clone();
                
                let on_samples = move |samples: Vec<[u8; 20]>| {
                    let count = samples.len();
                    info!(worker = worker_id, samples = count, "received infohash samples");
                    wm.record_worker_samples(worker_id, count as u64);
                    received_flag.store(true, Ordering::Relaxed);
                    for ih in samples { let _ = tx2.send(ih); }
                };
                
                info!(worker = i, attempt, bootstrap = ?bootstrap, "starting sample_infohashes request");
                client.sample_infohashes(&target, &bootstrap, on_samples).await;
                
                if !received_any.load(Ordering::Relaxed) {
                    warn!(worker = i, attempt, "no samples received in this iteration");
                }
                
                sleep(Duration::from_secs(10)).await;
            }
        });
    }

    // Initialize deduplication manager
    let dedup_manager = DeduplicationManager::new(cfg.dedup_db_path.clone());
    info!(dedup_db = ?cfg.dedup_db_path, "deduplication manager initialized");

    // Spawn periodic dedup persistence task
    let dedup_persist = dedup_manager.clone();
    let persist_interval = cfg.dedup_persist_interval;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(persist_interval));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(e) = dedup_persist.persist().await {
                warn!(error = %e, "failed to persist dedup state");
            }
        }
    });

    // Metadata fetchers
    let (tx_meta, rx_meta) = mpsc::unbounded_channel::<([u8;20], Vec<SocketAddr>)>();

    // Coordinator: collect infohashes, query peers via DHT, enqueue to metadata workers
    let coord_bootstrap = cfg.bootstrap.clone();
    let coord_dedup = dedup_manager.clone();
    let coord_metrics = metrics.clone();
    tokio::spawn(async move {
        while let Some(ih) = rx_ih.recv().await {
            if coord_dedup.is_seen(&ih) { 
                continue; 
            }
            if !coord_dedup.mark_seen(ih) { 
                continue; 
            }
            coord_metrics.record_infohash_discovered();
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
    let meta_metrics = metrics.clone();
    tokio::spawn(async move {
        let mut rx = rx_meta;
        while let Some((ih, peers)) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.expect("semaphore");
            let out = out_dir.clone();
            let mm = meta_metrics.clone();
            tokio::spawn(async move {
                let _permit = permit;
                mm.record_metadata_attempt();
                info!(ih = %hex::encode(ih), peers = peers.len(), "metadata fetch start");
                if let Err(e) = fetch_metadata_for_infohash(ih, peers.clone(), out.as_ref(), mm.clone()).await {
                    warn!(ih = %hex::encode(ih), error = %e, "metadata fetch failed");
                    // simple delayed retry (don't count as new attempt)
                    let out2 = out.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_secs(30)).await;
                        info!(ih = %hex::encode(ih), "retry metadata fetch");
                        let _ = fetch_metadata_for_infohash(ih, peers, out2.as_ref(), Metrics::new()).await;
                    });
                }
            });
        }
    });

    // Periodic metrics reporting
    let report_metrics = metrics.clone();
    let report_dedup = dedup_manager.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            report_metrics.print_report(60);
            let (count, _) = report_dedup.stats();
            info!(seen_infohashes = count, "deduplication cache size");
        }
    });

    // Setup graceful shutdown
    let shutdown_dedup = dedup_manager.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("shutting down, persisting dedup state...");
        if let Err(e) = shutdown_dedup.persist().await {
            warn!(error = %e, "failed to persist dedup state on shutdown");
        }
        std::process::exit(0);
    });

    // Keep main alive
    loop { 
        sleep(Duration::from_secs(3600)).await;
    }
}

async fn fetch_metadata_for_infohash(ih: [u8;20], peers: Vec<SocketAddr>, out_dir: Option<&std::path::PathBuf>, metrics: Metrics) -> anyhow::Result<()> {
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
            name: String::new(), 
            piece_length: 16384, 
            pieces: Vec::new(), 
            files: Vec::new(), 
            private: false, 
            raw_infohash: [0u8;20], 
            raw_info_bencode: Vec::new(),
            meta_version: 1,
            file_tree: None,
            pieces_v2: None,
            raw_infohash_v2: None,
        },
        comment: None,
        created_by: None,
        creation_date: None,
        encoding: None,
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
                metrics.record_bytes_received(data.len() as u64);
                if let Some(dir) = out_dir { 
                    let fname = dir.join(format!("{}.meta", ih.encode_hex::<String>()));
                    if let Err(e) = tokio::fs::create_dir_all(dir).await { warn!(error = %e, "mkdir failed"); }
                    if let Err(e) = tokio::fs::write(&fname, &data).await { warn!(error = %e, file = %fname.display(), "write failed"); }
                }
                info!(ih = %hex::encode(ih), bytes = data.len(), "metadata fetched");
                metrics.record_metadata_success();
                return Ok(());
            }
        }
    }
    anyhow::bail!("no metadata from peers")
}
