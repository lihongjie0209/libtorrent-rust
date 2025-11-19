use bendy::decoding::Decoder;
use rand::RngCore;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::UdpSocket;
use tokio::time::{timeout, Duration};
use tracing::info;

const KRPC_Q: &str = "q";
const KRPC_R: &str = "r";
const KRPC_E: &str = "e";

// Client identification (2 chars) + version (2 bytes)
// "LR" = libtorrent-rust
const CLIENT_VERSION: &[u8; 4] = b"LR\x00\x01"; // LR 0.1

fn random_tid() -> [u8; 2] {
    let mut t = [0u8; 2];
    rand::thread_rng().fill_bytes(&mut t);
    t
}

fn random_node_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    rand::thread_rng().fill_bytes(&mut id);
    id
}

fn write_bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    let mut buf = itoa::Buffer::new();
    let len = buf.format(bytes.len());
    out.extend_from_slice(len.as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn write_str(out: &mut Vec<u8>, s: &str) { write_bstr(out, s.as_bytes()); }

fn write_int(out: &mut Vec<u8>, v: i64) {
    out.push(b'i');
    let mut buf = itoa::Buffer::new();
    let s = buf.format(v);
    out.extend_from_slice(s.as_bytes());
    out.push(b'e');
}

fn encode_get_peers(tid: &[u8], node_id: &[u8; 20], info_hash: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.push(b'd');
    // a
    write_str(&mut out, "a");
    out.push(b'd');
    write_str(&mut out, "id");
    write_bstr(&mut out, node_id);
    write_str(&mut out, "info_hash");
    write_bstr(&mut out, info_hash);
    out.push(b'e');
    // q
    write_str(&mut out, "q");
    write_str(&mut out, "get_peers");
    // t
    write_str(&mut out, "t");
    write_bstr(&mut out, tid);
    // v (client version)
    write_str(&mut out, "v");
    write_bstr(&mut out, CLIENT_VERSION);
    // y
    write_str(&mut out, "y");
    write_str(&mut out, "q");
    out.push(b'e');
    out
}

fn encode_announce_peer(
    tid: &[u8],
    node_id: &[u8; 20],
    info_hash: &[u8; 20],
    token: &[u8],
    port: u16,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(160);
    out.push(b'd');
    // a
    write_str(&mut out, "a");
    out.push(b'd');
    write_str(&mut out, "id");
    write_bstr(&mut out, node_id);
    write_str(&mut out, "info_hash");
    write_bstr(&mut out, info_hash);
    write_str(&mut out, "port");
    write_int(&mut out, port as i64);
    write_str(&mut out, "token");
    write_bstr(&mut out, token);
    out.push(b'e');
    // q
    write_str(&mut out, "q");
    write_str(&mut out, "announce_peer");
    // t
    write_str(&mut out, "t");
    write_bstr(&mut out, tid);
    // v (client version)
    write_str(&mut out, "v");
    write_bstr(&mut out, CLIENT_VERSION);
    // y
    write_str(&mut out, "y");
    write_str(&mut out, "q");
    out.push(b'e');
    out
}

fn parse_compact_peers_bytes(bytes: &[u8]) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    if bytes.len() % 6 != 0 { return out; }
    for chunk in bytes.chunks_exact(6) {
        let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        out.push(SocketAddr::new(std::net::IpAddr::V4(ip), port));
    }
    out
}

fn parse_compact_nodes(bytes: &[u8]) -> Vec<(SocketAddr, [u8; 20])> {
    // 26 bytes per node: 20 node id + 6 compact addr
    let mut out = Vec::new();
    for chunk in bytes.chunks_exact(26) {
        let mut nid = [0u8; 20];
        nid.copy_from_slice(&chunk[0..20]);
        let ip = std::net::Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
        let port = u16::from_be_bytes([chunk[24], chunk[25]]);
        out.push((SocketAddr::new(std::net::IpAddr::V4(ip), port), nid));
    }
    out
}

fn _to_io_str(msg: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, msg.to_string()) }

struct PendingQuery {
    kind: QueryKind,
    info_hash: [u8; 20],
}

enum QueryKind { GetPeers }

pub struct DhtClient {
    sock: UdpSocket,
    node_id: [u8; 20],
}

impl DhtClient {
    pub async fn bind(local: SocketAddr) -> io::Result<Self> {
        let sock = UdpSocket::bind(local).await?;
        Ok(Self { sock, node_id: random_node_id() })
    }

    pub async fn bind_with_id(local: SocketAddr, node_id: [u8; 20]) -> io::Result<Self> {
        let sock = UdpSocket::bind(local).await?;
        Ok(Self { sock, node_id })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    // Note: tokio::net::UdpSocket is not clonable; avoid cloning the client.

    async fn resolve_bootstrap(hosts: &[String]) -> Vec<SocketAddr> {
        let mut out = Vec::new();
        for h in hosts {
            match h.to_socket_addrs() {
                Ok(iter) => {
                    let resolved: Vec<_> = iter
                        .filter(|addr| addr.is_ipv4())  // Only use IPv4 addresses
                        .take(8)
                        .collect();
                    if resolved.is_empty() {
                        tracing::debug!(host = %h, "no IPv4 addresses found, skipping IPv6");
                    } else {
                        tracing::debug!(host = %h, resolved = ?resolved, "bootstrap node resolved");
                        out.extend(resolved);
                    }
                }
                Err(e) => {
                    tracing::warn!(host = %h, error = %e, "failed to resolve bootstrap node");
                }
            }
        }
        if !out.is_empty() {
            tracing::info!(count = out.len(), addrs = ?out, "resolved bootstrap nodes");
        }
        out
    }

    pub async fn get_peers_and_announce(
        &self,
        info_hash: [u8; 20],
        announce_port: u16,
        bootstrap: &[String],
        on_peers: impl Fn(Vec<SocketAddr>) + Send + 'static,
    ) {
        let seeds = Self::resolve_bootstrap(bootstrap).await;
        if seeds.is_empty() { return; }
        let mut queue: VecDeque<SocketAddr> = seeds.into();
        let mut seen: std::collections::HashSet<SocketAddr> = std::collections::HashSet::new();
        let mut tokens: HashMap<SocketAddr, Vec<u8>> = HashMap::new();
        let mut pending: HashMap<Vec<u8>, PendingQuery> = HashMap::new();

        // Kick off a batch
        for _ in 0..8 {
            if let Some(n) = queue.pop_front() {
                if seen.insert(n) {
                    let tid = random_tid();
                    let msg = encode_get_peers(&tid, &self.node_id, &info_hash);
                    let _ = self.sock.send_to(&msg, n).await;
                    pending.insert(tid.to_vec(), PendingQuery { kind: QueryKind::GetPeers, info_hash });
                }
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
        let mut buf = vec![0u8; 1500];
        while tokio::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(500), self.sock.recv_from(&mut buf)).await {
                Ok(Ok((n, from))) => {
                    if n == 0 { continue; }
                    let data = &buf[..n];
                    // Minimal parse: r-dict with t, r{values|nodes, token}
                    if let Ok(dec) = Decoder::new(data).next_object() {
                        if let Ok(mut dict) = dec.unwrap().try_into_dictionary() {
                            let mut tid_opt: Option<Vec<u8>> = None;
                            let mut nodes_opt: Option<Vec<u8>> = None;
                            let mut values_blobs: Vec<Vec<u8>> = Vec::new();
                            let mut token_opt: Option<Vec<u8>> = None;
                            while let Ok(Some((k, v))) = dict.next_pair() {
                                match k {
                                    b"t" => { if let Ok(b) = v.try_into_bytes() { tid_opt = Some(b.to_vec()); } }
                                    b"r" => {
                                        if let Ok(mut rdict) = v.try_into_dictionary() {
                                            while let Ok(Some((rk, rv))) = rdict.next_pair() {
                                                match rk {
                                                    b"nodes" => { if let Ok(b) = rv.try_into_bytes() { nodes_opt = Some(b.to_vec()); } }
                                                    b"values" => {
                                                        if let Ok(mut list) = rv.try_into_list() {
                                                            while let Ok(Some(item)) = list.next_object() {
                                                                if let Ok(b) = item.try_into_bytes() { values_blobs.push(b.to_vec()); }
                                                            }
                                                        }
                                                    }
                                                    b"token" => { if let Ok(b) = rv.try_into_bytes() { token_opt = Some(b.to_vec()); } }
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            if let Some(tid) = tid_opt {
                                if let Some(_pq) = pending.remove(&tid) {
                                    for vb in values_blobs.iter() {
                                        let peers = parse_compact_peers_bytes(vb);
                                        if !peers.is_empty() { on_peers(peers); }
                                    }
                                    if let Some(nb) = nodes_opt {
                                        for (addr, _nid) in parse_compact_nodes(&nb) { if seen.insert(addr) { queue.push_back(addr); } }
                                    }
                                    if let Some(tok) = token_opt { tokens.insert(from, tok); }
                                }
                            }
                        }
                    }
                }
                _ => {
                    // timeout tick
                }
            }
            // Keep sending next nodes
            while pending.len() < 16 {
                if let Some(next) = queue.pop_front() {
                    let tid = random_tid();
                    let msg = encode_get_peers(&tid, &self.node_id, &info_hash);
                    let _ = self.sock.send_to(&msg, next).await;
                    pending.insert(tid.to_vec(), PendingQuery { kind: QueryKind::GetPeers, info_hash });
                } else { break; }
            }
        }
        // Announce to nodes we have tokens for
        for (addr, tok) in tokens {
            let tid = random_tid();
            let msg = encode_announce_peer(&tid, &self.node_id, &info_hash, &tok, announce_port);
            let _ = self.sock.send_to(&msg, addr).await;
        }
        info!(peers_announced = true, "DHT iteration done");
    }

    pub async fn sample_infohashes(
        &self,
        target: &[u8; 20],
        bootstrap: &[String],
        on_samples: impl Fn(Vec<[u8; 20]>) + Send + 'static,
    ) {
        // Log socket binding info
        if let Ok(local) = self.sock.local_addr() {
            tracing::info!(local_addr = %local, "DHT socket bound");
        }
        
        let seeds = Self::resolve_bootstrap(bootstrap).await;
        if seeds.is_empty() { 
            tracing::warn!("failed to resolve any bootstrap nodes");
            return; 
        }
        let mut queue: VecDeque<SocketAddr> = seeds.into();
        let mut seen: std::collections::HashSet<SocketAddr> = std::collections::HashSet::new();
        let mut pending: HashMap<Vec<u8>, SocketAddr> = HashMap::new();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        // Phase 1: Send find_node to bootstrap nodes to build routing table
        let mut sent_count = 0;
        let mut find_node_count = 0;
        tracing::info!(bootstrap_count = queue.len(), "starting DHT bootstrap with find_node");
        for _ in 0..queue.len().min(8) {
            if let Some(n) = queue.pop_front() {
                if seen.insert(n) {
                    let tid = random_tid();
                    let msg = encode_find_node(&tid, &self.node_id, target);
                    match self.sock.send_to(&msg, n).await {
                        Ok(bytes) => {
                            find_node_count += 1;
                            sent_count += 1;
                            tracing::debug!(to = %n, bytes, "sent find_node request");
                        }
                        Err(e) => {
                            tracing::warn!(to = %n, error = %e, "failed to send find_node");
                        }
                    }
                    pending.insert(tid.to_vec(), n);
                }
            }
        }
        tracing::debug!(sent = find_node_count, pending = pending.len(), "find_node requests sent");
        
        // Phase 2: Collect node responses for 3 seconds
        let mut nodes_collected = Vec::new();
        let mut buf = vec![0u8; 1500];
        let mut responses_received = 0;
        let collect_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        
        while tokio::time::Instant::now() < collect_deadline {
            match timeout(Duration::from_millis(100), self.sock.recv_from(&mut buf)).await {
                Ok(Ok((n, from))) => {
                    if n == 0 { continue; }
                    responses_received += 1;
                    tracing::debug!(from = %from, bytes = n, "received find_node response");
                    let data = &buf[..n];
                    if let Ok(dec) = Decoder::new(data).next_object() {
                        if let Ok(mut dict) = dec.unwrap().try_into_dictionary() {
                            while let Ok(Some((k, v))) = dict.next_pair() {
                                if k == b"t" { if let Ok(b) = v.try_into_bytes() { let _ = pending.remove(&b.to_vec()); } }
                                else if k == b"r" {
                                    if let Ok(mut rdict) = v.try_into_dictionary() {
                                        while let Ok(Some((rk, rv))) = rdict.next_pair() {
                                            if rk == b"nodes" {
                                                if let Ok(b) = rv.try_into_bytes() {
                                                    for (addr, _nid) in parse_compact_nodes(&b) {
                                                        if seen.insert(addr) {
                                                            nodes_collected.push(addr);
                                                            queue.push_back(addr);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        
        tracing::info!(nodes_collected = nodes_collected.len(), responses = responses_received, "collected DHT nodes");
        
        // Phase 3: Send sample_infohashes to collected nodes
        pending.clear();
        let sample_count_target = nodes_collected.len().min(20);
        for addr in nodes_collected.iter().take(sample_count_target) {
            let tid = random_tid();
            let msg = encode_sample_infohashes(&tid, &self.node_id, target);
            match self.sock.send_to(&msg, *addr).await {
                Ok(bytes) => {
                    sent_count += 1;
                    tracing::debug!(to = %addr, bytes, "sent sample_infohashes request");
                }
                Err(e) => {
                    tracing::warn!(to = %addr, error = %e, "failed to send sample_infohashes");
                }
            }
            pending.insert(tid.to_vec(), *addr);
        }
        
        tracing::info!(sent = sample_count_target, "sample_infohashes requests sent");
        
        // Phase 4: Collect samples
        while tokio::time::Instant::now() < deadline {
            match timeout(Duration::from_millis(500), self.sock.recv_from(&mut buf)).await {
                Ok(Ok((n, from))) => {
                    if n == 0 { continue; }
                    responses_received += 1;
                    tracing::debug!(from = %from, bytes = n, "received DHT response");
                    let data = &buf[..n];
                    if let Ok(dec) = Decoder::new(data).next_object() {
                        if let Ok(mut dict) = dec.unwrap().try_into_dictionary() {
                            let mut tid_opt: Option<Vec<u8>> = None;
                            let mut response_id: Option<Vec<u8>> = None;
                            let mut nodes_opt: Option<Vec<u8>> = None;
                            let mut samples_blob: Option<Vec<u8>> = None;
                            let mut interval: Option<i64> = None;
                            let mut num: Option<i64> = None;
                            let mut error_msg: Option<String> = None;
                            
                            while let Ok(Some((k, v))) = dict.next_pair() {
                                match k {
                                    b"t" => { if let Ok(b) = v.try_into_bytes() { tid_opt = Some(b.to_vec()); } }
                                    b"e" => {
                                        // Error response
                                        if let Ok(mut elist) = v.try_into_list() {
                                            let _ = elist.next_object(); // error code
                                            if let Ok(Some(emsg)) = elist.next_object() {
                                                if let Ok(s) = emsg.try_into_bytes() {
                                                    error_msg = Some(String::from_utf8_lossy(s).into());
                                                }
                                            }
                                        }
                                    }
                                    b"r" => {
                                        if let Ok(mut rdict) = v.try_into_dictionary() {
                                            while let Ok(Some((rk, rv))) = rdict.next_pair() {
                                                match rk {
                                                    b"id" => { if let Ok(b) = rv.try_into_bytes() { response_id = Some(b.to_vec()); } }
                                                    b"nodes" => { if let Ok(b) = rv.try_into_bytes() { nodes_opt = Some(b.to_vec()); } }
                                                    b"samples" => { if let Ok(b) = rv.try_into_bytes() { samples_blob = Some(b.to_vec()); } }
                                                    b"interval" => { if let Ok(i) = rv.try_into_integer() { if let Ok(val) = i.parse::<i64>() { interval = Some(val); } } }
                                                    b"num" => { if let Ok(i) = rv.try_into_integer() { if let Ok(val) = i.parse::<i64>() { num = Some(val); } } }
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            
                            // Handle error responses
                            if let Some(err) = error_msg {
                                tracing::debug!(from = %from, error = %err, "received error response");
                                if let Some(tid) = tid_opt { let _ = pending.remove(&tid); }
                                continue;
                            }
                            
                            // Validate response has 'id' field (required by BEP 5)
                            if response_id.is_none() || response_id.as_ref().unwrap().len() != 20 {
                                tracing::debug!(from = %from, "missing or invalid 'id' in response");
                                continue;
                            }
                            
                            // Validate sample_infohashes response
                            if let Some(sb) = samples_blob.as_ref() {
                                // Validate interval (BEP 51: 0 to 21600 seconds)
                                if let Some(int) = interval {
                                    if int < 0 || int > 21600 {
                                        tracing::debug!(from = %from, interval = int, "invalid interval value");
                                        continue;
                                    }
                                    tracing::debug!(from = %from, interval = int, "sample interval received");
                                }
                                
                                // Validate num field
                                if let Some(n) = num {
                                    tracing::debug!(from = %from, num = n, "total infohashes count");
                                }
                                
                                // Validate samples length (must be multiple of 20)
                                if sb.len() % 20 != 0 {
                                    tracing::debug!(from = %from, len = sb.len(), "invalid samples length");
                                    continue;
                                }
                            }
                            
                            if let Some(tid) = tid_opt { let _ = pending.remove(&tid); }
                            if let Some(nb) = nodes_opt { for (addr, _nid) in parse_compact_nodes(&nb) { if seen.insert(addr) { queue.push_back(addr); } } }
                            if let Some(sb) = samples_blob {
                                let mut samples: Vec<[u8;20]> = Vec::new();
                                for ch in sb.chunks_exact(20) { let mut ih = [0u8;20]; ih.copy_from_slice(ch); samples.push(ih); }
                                if !samples.is_empty() { 
                                    tracing::info!(from = %from, count = samples.len(), "received valid samples");
                                    on_samples(samples); 
                                }
                            }
                        }
                    }
            }
            _ => {}
            }
            // keep sending to more nodes
            while pending.len() < 16 && !nodes_collected.is_empty() {
                if let Some(n) = nodes_collected.pop() {
                    let tid = random_tid();
                    let msg = encode_sample_infohashes(&tid, &self.node_id, target);
                    if let Ok(bytes) = self.sock.send_to(&msg, n).await {
                        sent_count += 1;
                        tracing::trace!(to = %n, bytes, "sent follow-up sample_infohashes");
                    }
                    pending.insert(tid.to_vec(), n);
                } else { break; }
            }
        }
        
        tracing::info!(
            sent = sent_count, 
            responses = responses_received, 
            pending = pending.len(),
            "sample_infohashes round completed"
        );
    }
}

fn encode_find_node(tid: &[u8], node_id: &[u8; 20], target: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.push(b'd');
    // a
    write_str(&mut out, "a");
    out.push(b'd');
    write_str(&mut out, "id");
    write_bstr(&mut out, node_id);
    write_str(&mut out, "target");
    write_bstr(&mut out, target);
    out.push(b'e');
    // q
    write_str(&mut out, "q");
    write_str(&mut out, "find_node");
    // t
    write_str(&mut out, "t");
    write_bstr(&mut out, tid);
    // v (client version)
    write_str(&mut out, "v");
    write_bstr(&mut out, CLIENT_VERSION);
    // y
    write_str(&mut out, "y");
    write_str(&mut out, "q");
    out.push(b'e');
    out
}

fn encode_sample_infohashes(tid: &[u8], node_id: &[u8; 20], target: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.push(b'd');
    // a
    write_str(&mut out, "a");
    out.push(b'd');
    write_str(&mut out, "id");
    write_bstr(&mut out, node_id);
    write_str(&mut out, "target");
    write_bstr(&mut out, target);
    out.push(b'e');
    // q
    write_str(&mut out, "q");
    write_str(&mut out, "sample_infohashes");
    // t
    write_str(&mut out, "t");
    write_bstr(&mut out, tid);
    // v (client version)
    write_str(&mut out, "v");
    write_bstr(&mut out, CLIENT_VERSION);
    // y
    write_str(&mut out, "y");
    write_str(&mut out, "q");
    out.push(b'e');
    out
}
