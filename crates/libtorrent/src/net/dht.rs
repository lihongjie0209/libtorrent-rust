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
                Ok(mut iter) => {
                    let resolved: Vec<_> = iter.by_ref().take(4).collect();
                    tracing::debug!(host = %h, resolved = ?resolved, "bootstrap node resolved");
                    out.extend(resolved);
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
        let seeds = Self::resolve_bootstrap(bootstrap).await;
        if seeds.is_empty() { 
            tracing::warn!("failed to resolve any bootstrap nodes");
            return; 
        }
        let mut queue: VecDeque<SocketAddr> = seeds.into();
        let mut seen: std::collections::HashSet<SocketAddr> = std::collections::HashSet::new();
        let mut pending: HashMap<Vec<u8>, SocketAddr> = HashMap::new();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        // kick off
        let mut sent_count = 0;
        for _ in 0..8 {
            if let Some(n) = queue.pop_front() {
                if seen.insert(n) {
                    let tid = random_tid();
                    let msg = encode_sample_infohashes(&tid, &self.node_id, target);
                    match self.sock.send_to(&msg, n).await {
                        Ok(bytes) => {
                            sent_count += 1;
                            tracing::debug!(to = %n, bytes, "sent sample_infohashes request");
                        }
                        Err(e) => {
                            tracing::warn!(to = %n, error = %e, "failed to send sample_infohashes");
                        }
                    }
                    pending.insert(tid.to_vec(), n);
                }
            }
        }
        tracing::debug!(sent = sent_count, pending = pending.len(), "initial requests sent");
        
        let mut buf = vec![0u8; 1500];
        let mut responses_received = 0;
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
                            let mut nodes_opt: Option<Vec<u8>> = None;
                            let mut samples_blob: Option<Vec<u8>> = None;
                            while let Ok(Some((k, v))) = dict.next_pair() {
                                if k == b"t" { if let Ok(b) = v.try_into_bytes() { tid_opt = Some(b.to_vec()); } }
                                else if k == b"r" {
                                    if let Ok(mut rdict) = v.try_into_dictionary() {
                                        while let Ok(Some((rk, rv))) = rdict.next_pair() {
                                            match rk {
                                                b"nodes" => { if let Ok(b) = rv.try_into_bytes() { nodes_opt = Some(b.to_vec()); } }
                                                b"samples" => { if let Ok(b) = rv.try_into_bytes() { samples_blob = Some(b.to_vec()); } }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                            if let Some(tid) = tid_opt { let _ = pending.remove(&tid); }
                            if let Some(nb) = nodes_opt { for (addr, _nid) in parse_compact_nodes(&nb) { if seen.insert(addr) { queue.push_back(addr); } } }
                            if let Some(sb) = samples_blob {
                                let mut samples: Vec<[u8;20]> = Vec::new();
                                for ch in sb.chunks_exact(20) { let mut ih = [0u8;20]; ih.copy_from_slice(ch); samples.push(ih); }
                                if !samples.is_empty() { on_samples(samples); }
                            }
                        }
                    }
            }
            _ => {}
            }
            // keep pipeline
            while pending.len() < 16 {
                if let Some(n) = queue.pop_front() {
                    let tid = random_tid();
                    let msg = encode_sample_infohashes(&tid, &self.node_id, target);
                    if let Ok(bytes) = self.sock.send_to(&msg, n).await {
                        sent_count += 1;
                        tracing::trace!(to = %n, bytes, "sent follow-up request");
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
    // y
    write_str(&mut out, "y");
    write_str(&mut out, "q");
    out.push(b'e');
    out
}
