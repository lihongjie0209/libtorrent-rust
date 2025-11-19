use crate::error::LibtorrentError;
use crate::torrent::{SharedTorrentHandle, TorrentEvent};
use bytes::BytesMut;
use libtorrent_proto::{Handshake, Message, HANDSHAKE_LEN, HASH_LEN, PEER_ID_LEN};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::net::transport::Transport;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerState { pub peer_choking: bool, pub am_interested: bool, pub am_choking: bool, pub peer_interested: bool }
impl Default for PeerState { fn default() -> Self { Self { peer_choking: true, am_interested: false, am_choking: true, peer_interested: false } } }

pub struct PeerSession {
	stream: Transport,
	remote_addr: SocketAddr,
	info_hash: [u8; HASH_LEN],
	peer_id: [u8; PEER_ID_LEN],
	remote_peer_id: Option<[u8; PEER_ID_LEN]>,
	state: PeerState,
	read_buf: BytesMut,
	handle: Option<SharedTorrentHandle>,
	upload_slots: Option<Arc<Semaphore>>,
	upload_permit: Option<OwnedSemaphorePermit>,
	per_peer_upload_limit: u64,
	sent_in_window: u64,
	window_start: Instant,
	inflight: Vec<(u32, u32, u32, Instant)>,
	retries: HashMap<(u32,u32), u8>,
	max_pipeline: usize,
	request_timeout: Duration,
	optimistic_enabled: bool,
	optimistic_next: Instant,
	optimistic_interval: Duration,
	last_rx: Instant,
	last_tx: Instant,
	keepalive_interval: Duration,
	idle_timeout: Duration,
	ext_ut_metadata: Option<u8>,
	ext_ut_pex: Option<u8>,
	// ut_metadata client state
	remote_has_metadata: bool,
	metadata_size: Option<usize>,
	metadata_buf: Vec<u8>,
	metadata_have: Vec<bool>,
	metadata_last_req: Vec<Instant>,
	metadata_request_limit: Instant,
	events_rx: Option<tokio::sync::broadcast::Receiver<TorrentEvent>>,
}

impl PeerSession {
	pub fn new(
		stream: Transport,
		remote_addr: SocketAddr,
		info_hash: [u8; HASH_LEN],
		peer_id: [u8; PEER_ID_LEN],
		handle: Option<SharedTorrentHandle>,
		upload_slots: Option<Arc<Semaphore>>,
		per_peer_upload_limit: u64,
		max_pipeline: usize,
		request_timeout_secs: u64,
		optimistic_unchoke_enabled: bool,
		optimistic_unchoke_interval_secs: u64,
	) -> Self {
		let events_rx = if let Some(h) = &handle { if let Ok(guard) = h.try_read() { Some(guard.events_subscribe()) } else { None } } else { None };
		Self { stream, remote_addr, info_hash, peer_id, remote_peer_id: None, state: PeerState::default(), read_buf: BytesMut::with_capacity(65536), handle, upload_slots, upload_permit: None, per_peer_upload_limit, sent_in_window: 0, window_start: Instant::now(), inflight: Vec::new(), retries: HashMap::new(), max_pipeline, request_timeout: Duration::from_secs(request_timeout_secs), optimistic_enabled: optimistic_unchoke_enabled, optimistic_next: Instant::now() + Duration::from_secs(optimistic_unchoke_interval_secs), optimistic_interval: Duration::from_secs(optimistic_unchoke_interval_secs), last_rx: Instant::now(), last_tx: Instant::now(), keepalive_interval: Duration::from_secs(60), idle_timeout: Duration::from_secs(180), events_rx, ext_ut_metadata: None, ext_ut_pex: None, remote_has_metadata: false, metadata_size: None, metadata_buf: Vec::new(), metadata_have: Vec::new(), metadata_last_req: Vec::new(), metadata_request_limit: Instant::now() }
	}
	pub fn remote_addr(&self) -> SocketAddr { self.remote_addr }
	pub fn remote_peer_id(&self) -> Option<[u8; PEER_ID_LEN]> { self.remote_peer_id }
	pub fn state(&self) -> &PeerState { &self.state }

	pub async fn run(mut self) -> Result<(), LibtorrentError> {
		self.perform_handshake().await?;
		info!(remote_addr = %self.remote_addr, remote_peer_id = ?self.remote_peer_id, "Handshake completed");
		self.message_loop().await
	}

	async fn perform_handshake(&mut self) -> Result<(), LibtorrentError> {
		let mut buf = [0u8; HANDSHAKE_LEN];
		self.stream.read_exact(&mut buf).await?;
		let incoming = Handshake::decode(&buf)?;
		if incoming.info_hash != self.info_hash { return Err(LibtorrentError::info_hash_mismatch(self.info_hash, incoming.info_hash)); }
		self.remote_peer_id = Some(incoming.peer_id);
		let response = Handshake::new(self.info_hash, self.peer_id);
		let payload = response.encode();
		self.stream.write_all(&payload).await?;
		if let Some(h) = &self.handle { let maybe_msg = if let Ok(guard) = h.try_read() { let bits = guard.get_bitfield(); let bytes = Self::encode_bitfield(&bits); if !bytes.is_empty() { Some(Message::Bitfield(bytes::Bytes::from(bytes))) } else { None } } else { None }; if let Some(msg) = maybe_msg { let _ = self.send_message(msg).await; } }
		let payload = {
			fn enc_str_bytes(s: &[u8]) -> Vec<u8> { let mut out = Vec::with_capacity(s.len() + 16); out.extend_from_slice(s.len().to_string().as_bytes()); out.push(b':'); out.extend_from_slice(s); out }
			fn enc_str(s: &str) -> Vec<u8> { enc_str_bytes(s.as_bytes()) }
			fn enc_int(n: i64) -> Vec<u8> { let mut out = Vec::with_capacity(32); out.push(b'i'); out.extend_from_slice(n.to_string().as_bytes()); out.push(b'e'); out }
			fn enc_dict(mut pairs: Vec<(&[u8], Vec<u8>)>) -> Vec<u8> {
				pairs.sort_by(|a,b| a.0.cmp(b.0));
				let mut out = Vec::new();
				out.push(b'd');
				for (k, v) in pairs.into_iter() {
					out.extend_from_slice(enc_str_bytes(k).as_slice());
					out.extend_from_slice(&v);
				}
				out.push(b'e');
				out
			}
			let m = enc_dict(vec![(b"ut_metadata" as &[u8], enc_int(1)), (b"ut_pex" as &[u8], enc_int(2))]);
			let mut pairs: Vec<(&[u8], Vec<u8>)> = Vec::new();
			pairs.push((b"m", m));
			if let Some(h) = &self.handle { if let Ok(g) = h.try_read() { let sz = g.meta().info.raw_info_bencode.len() as i64; if sz > 0 { pairs.push((b"metadata_size", enc_int(sz))); } } }
			pairs.push((b"reqq", enc_int(32)));
			pairs.push((b"v", enc_str("lt-rs/0.1")));
			enc_dict(pairs)
		};
		let _ = self.send_message(Message::Extended(0, bytes::Bytes::from(payload))).await;
		Ok(())
	}

	async fn message_loop(&mut self) -> Result<(), LibtorrentError> {
		let mut tick = tokio::time::interval(Duration::from_millis(250));
		loop {
			tokio::select! {
				read_res = async { let mut buf = vec![0u8; 8192]; let n = self.stream.read(&mut buf).await?; buf.truncate(n); Ok::<Vec<u8>, std::io::Error>(buf) } => {
					let buf = read_res?; if buf.is_empty() { debug!(remote_addr = %self.remote_addr, "Connection closed"); return Ok(()); }
					self.last_rx = Instant::now(); self.read_buf.extend_from_slice(&buf);
					while !self.read_buf.is_empty() { match Message::decode(&self.read_buf)? { Some((msg, consumed)) => { self.handle_message(msg).await?; let _ = self.read_buf.split_to(consumed); } None => break } }
				}
				_ = tick.tick() => { self.on_tick().await?; }
						}
					}
		}

	async fn on_tick(&mut self) -> Result<(), LibtorrentError> {
		let now = Instant::now();
		if now.duration_since(self.last_tx) >= self.keepalive_interval { let _ = self.send_message(Message::KeepAlive).await; }
		if now.duration_since(self.last_rx) >= self.idle_timeout { debug!(remote_addr = %self.remote_addr, "Idle timeout, closing"); return Ok(()); }
		let mut have_pieces: Vec<u32> = Vec::new(); let mut choke_cmd: Option<bool> = None;
				if let Some(rx) = &mut self.events_rx { loop { match rx.try_recv() { Ok(TorrentEvent::Have(piece)) => have_pieces.push(piece), Ok(TorrentEvent::Choke(addr)) => { if addr == self.remote_addr { choke_cmd = Some(true); } }, Ok(TorrentEvent::Unchoke(addr)) => { if addr == self.remote_addr { choke_cmd = Some(false); } }, Ok(TorrentEvent::PexAdded(_)) => { /* ignore in peer */ }, Ok(TorrentEvent::PexDropped(_)) => { /* ignore in peer */ }, Ok(TorrentEvent::PexAddedWithFlags(_)) => { /* ignore in peer */ }, Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break, Err(_) => break } } }
		for p in have_pieces { let _ = self.send_message(Message::Have(p)).await; }
		if let Some(ch) = choke_cmd { if ch { let _ = self.send_choke().await; } else { let _ = self.send_unchoke().await; } }
		let timeout = self.request_timeout; let mut to_resend = Vec::new();
		self.inflight.retain(|(i,b,l,t)| { if now.duration_since(*t) > timeout { to_resend.push((*i,*b,*l)); false } else { true } });
		for (i,b,l) in to_resend { let key = (i,b); let c = self.retries.entry(key).or_insert(0); if *c < 3 { *c += 1; let _ = self.send_message(Message::Request { index: i, begin: b, length: l }).await; self.inflight.push((i,b,l,Instant::now())); } else { self.retries.remove(&key); } }
		if self.optimistic_enabled && self.state.peer_interested && self.upload_permit.is_none() { let now = Instant::now(); if now >= self.optimistic_next { self.optimistic_next = now + self.optimistic_interval; if let Some(slots) = &self.upload_slots { if let Ok(permit) = slots.clone().try_acquire_owned() { self.upload_permit = Some(permit); let _ = self.send_unchoke().await; } } } return Ok(()); }
		let max_pipeline = self.max_pipeline; if let Some(h) = &self.handle { let mut to_send: Vec<(u32,u32,u32)> = Vec::new(); if let Ok(mut guard) = h.try_write() { if let Some(peer_has) = guard.peer_bitfield_clone(&self.remote_addr) { while self.inflight.len() + to_send.len() < max_pipeline { if let Some(piece) = guard.piece_picker().pick_piece(&peer_has) { guard.piece_picker_mut().piece_started(piece); let skip: Vec<u32> = self.inflight.iter().filter(|(pi,_,_,_)| *pi == piece).map(|(_,b,_,_)| *b).collect(); if let Some((begin,len)) = guard.next_missing_block_for_piece(piece, &skip) { to_send.push((piece, begin, len)); } else { break; } } else { break; } } } } for (piece,begin,len) in to_send { let _ = self.send_message(Message::Request { index: piece, begin, length: len }).await; self.inflight.push((piece, begin, len, Instant::now())); } }

		// ut_metadata client: request metadata pieces if needed
		if self.ext_ut_metadata.is_some() {
			// if we already have metadata in handle, skip
			let already_have = if let Some(h) = &self.handle { if let Ok(g) = h.try_read() { !g.meta().info.raw_info_bencode.is_empty() } else { false } } else { false };
			if !already_have {
				if let Some(total) = self.metadata_size {
					if self.metadata_buf.is_empty() {
						self.metadata_buf = vec![0u8; total];
						let pieces = (total + 16384 - 1) / 16384;
						self.metadata_have = vec![false; pieces];
						self.metadata_last_req = vec![Instant::now() - Duration::from_secs(3600); pieces];
					}
					// backoff if remote reported dont-have or failure
					if now < self.metadata_request_limit { return Ok(()); }
						if let Some(idx) = select_metadata_piece(&self.metadata_have, &self.metadata_last_req, now) {
							let mut enc = bendy::encoding::Encoder::new();
								if enc.emit_dict(|mut e| {
									e.emit_pair(b"msg_type", 0i64)?;
									e.emit_pair(b"piece", idx as i64)?;
									Ok(())
								}).is_ok() {
							if let Ok(payload) = enc.get_output() {
								let _ = self.send_message(Message::Extended(self.ext_ut_metadata.unwrap(), bytes::Bytes::from(payload))).await;
								self.metadata_last_req[idx] = now;
							}
						}
					}
				}
			}
		}
		Ok(())
	}

	async fn handle_message(&mut self, msg: Message) -> Result<(), LibtorrentError> {
		debug!(remote_addr = %self.remote_addr, message = ?msg, "Received message");
		match msg {
			Message::KeepAlive => {}
			Message::Choke => { self.state.peer_choking = true; }
			Message::Unchoke => { self.state.peer_choking = false; }
			Message::Interested => { self.state.peer_interested = true; if let Some(h) = &self.handle { if let Ok(mut w) = h.try_write() { w.set_peer_interested(&self.remote_addr, true); } } if self.state.am_choking { if let Some(slots) = &self.upload_slots { if let Ok(permit) = slots.clone().try_acquire_owned() { self.upload_permit = Some(permit); let _ = self.send_unchoke().await; } } else { let _ = self.send_unchoke().await; } } }
			Message::NotInterested => { self.state.peer_interested = false; if let Some(h) = &self.handle { if let Ok(mut w) = h.try_write() { w.set_peer_interested(&self.remote_addr, false); } } if !self.state.am_choking { let _ = self.send_choke().await; self.upload_permit = None; } }
			Message::Have(piece_index) => { if let Some(h) = &self.handle { if let Ok(mut guard) = h.try_write() { guard.peer_has_piece(&self.remote_addr, piece_index); } } }
			Message::Bitfield(data) => { if let Some(h) = &self.handle { if let Ok(guard) = h.try_read() { let num = guard.meta().info.num_pieces(); drop(guard); let mut bits = vec![false; num]; for (i, byte) in data.iter().enumerate() { for b in 0..8 { let idx = i*8 + b; if idx >= num { break; } let mask = 1u8 << (7 - b); bits[idx] = (byte & mask) != 0; } } if let Ok(mut w) = h.try_write() { w.update_peer_bitfield(&self.remote_addr, bits); } } } }
			Message::Request { index, begin, length } => { debug!(piece = index, offset = begin, length = length, "Peer requested block"); if self.state.am_choking { return Ok(()); } let handle = self.handle.clone(); if let Some(h) = handle { if let Ok(guard) = h.try_read() { if let Some(dm) = guard.disk() { if let Ok(block) = dm.read_block(index, begin, length as usize).await { self.apply_upload_limit(block.len() as u64).await; drop(guard); let msg = Message::Piece { index, begin, data: bytes::Bytes::from(block) }; let _ = self.send_message(msg).await; } } } } }
			Message::Piece { index, begin, data } => { debug!(piece = index, offset = begin, size = data.len(), "Received block"); if let Some(h) = &self.handle { if let Ok(guard) = h.try_read() { if let Some(dm) = guard.disk() { let _ = dm.write_block(index, begin, &data).await; } } let mut completed_piece = false; if let Ok(mut guard) = h.try_write() { guard.add_downloaded(data.len() as u64); guard.record_peer_download(&self.remote_addr, data.len() as u64); completed_piece = guard.record_block_and_maybe_complete(index, begin, data.len() as u32).await.unwrap_or(false); } if completed_piece { let _ = self.send_message(Message::Have(index)).await; } } self.inflight.retain(|(pi,b,l,_)| !(*pi==index && *b==begin && (*l as usize)==data.len())); self.retries.remove(&(index, begin)); }
			Message::Cancel { index, begin, length } => { debug!(piece = index, offset = begin, length = length, "Peer cancelled request"); self.inflight.retain(|(pi,b,l,_)| !(*pi==index && *b==begin && *l==length)); }
			Message::Extended(ext_id, data) => {
				if ext_id == 0 {
					if let Some(pairs) = bencode_parse_dict(&data) {
						for (k, v) in pairs {
							if k == b"m" {
								if let Some(m_pairs) = bencode_parse_dict(v) {
									for (mk, mv) in m_pairs {
										if let Some(id) = bencode_parse_int(mv) { let id = (id.max(0) as u8).max(1); if mk == b"ut_metadata" { self.ext_ut_metadata = Some(id); } if mk == b"ut_pex" { self.ext_ut_pex = Some(id); } }
									}
								}
							}
							if k == b"metadata_size" { if let Some(sz) = bencode_parse_int(v) { let sz = sz.max(0) as usize; if sz > 0 && sz <= 4*1024*1024 { self.metadata_size = Some(sz); self.remote_has_metadata = true; } } }
						}
					}
				} else if Some(ext_id) == self.ext_ut_metadata {
					let bytes = &data[..];
					let mut to_send_msg: Option<Message> = None;
					let mut msg_type = 0i64;
					let mut piece = 0i64;
					let mut total_size_hdr: Option<i64> = None;
					if let Some(pairs) = bencode_parse_dict(bytes) {
						for (k, v) in pairs {
							if k == b"msg_type" { if let Some(i) = bencode_parse_int(v) { msg_type = i; } }
							if k == b"piece" { if let Some(i) = bencode_parse_int(v) { piece = i; } }
							if k == b"total_size" { total_size_hdr = bencode_parse_int(v); }
						}
					}
						match msg_type {
							0 => {
								// serve metadata piece
								if let Some(h) = &self.handle {
									if let Ok(g) = h.try_read() {
										let info = &g.meta().info.raw_info_bencode;
										if !info.is_empty() {
											let piece_len = 16384usize;
											let off = (piece as usize).saturating_mul(piece_len);
											if off < info.len() {
												let end = (off + piece_len).min(info.len());
												let mut enc = bendy::encoding::Encoder::new();
												if enc.emit_dict(|mut e| { e.emit_pair(b"msg_type", 1i64)?; e.emit_pair(b"piece", piece)?; e.emit_pair(b"total_size", info.len() as i64)?; Ok(()) }).is_ok() {
													if let Ok(mut payload) = enc.get_output() { payload.extend_from_slice(&info[off..end]); to_send_msg = Some(Message::Extended(ext_id, bytes::Bytes::from(payload))); }
												}
											}
										} else {
											let mut enc = bendy::encoding::Encoder::new();
											if enc.emit_dict(|mut e| { e.emit_pair(b"msg_type", 2i64)?; e.emit_pair(b"piece", piece)?; Ok(()) }).is_ok() {
												if let Ok(payload) = enc.get_output() { to_send_msg = Some(Message::Extended(ext_id, bytes::Bytes::from(payload))); }
											}
										}
									}
								}
							}
							1 => {
								// received metadata piece
								let header_len = bencode_first_object_len(bytes).unwrap_or(bytes.len());
								let payload = &bytes[header_len..];
								let piece_len = 16384usize;
								// initialize buffers if needed
								if let Some(ts) = total_size_hdr {
									let ts = (ts as i64).max(0) as usize;
									if self.metadata_size.is_none() && ts > 0 && ts <= 4*1024*1024 {
										self.metadata_size = Some(ts);
										self.metadata_buf = vec![0u8; ts];
										let n = (ts + piece_len - 1) / piece_len;
										self.metadata_have = vec![false; n];
										self.metadata_last_req = vec![Instant::now() - Duration::from_secs(3600); n];
									}
								}
								if let Some(total) = self.metadata_size {
									let idx = piece as usize;
									let n = (total + piece_len - 1) / piece_len;
									if idx < n {
										let off = idx * piece_len;
										let end = (off + payload.len()).min(total);
										if off < end {
											self.metadata_buf[off..end].copy_from_slice(&payload[..(end - off)]);
											self.metadata_have[idx] = true;
										}
										// if complete, verify hash
										if self.metadata_have.iter().all(|&b| b) {
											use sha1::{Sha1, Digest};
											let mut hasher = Sha1::new();
											hasher.update(&self.metadata_buf);
											let got = hasher.finalize();
											if got.as_slice() == &self.info_hash {
												info!(remote = %self.remote_addr, size = total, "ut_metadata complete and verified");
												if let Some(h) = &self.handle {
													if let Ok(mut w) = h.try_write() {
														let _ = w.apply_metadata_from_info_bytes(self.metadata_buf.clone());
													}
												}
											} else {
												// hash mismatch. back off and retry later
												self.metadata_request_limit = Instant::now() + Duration::from_secs(30);
												for h in self.metadata_have.iter_mut() { *h = false; }
											}
										}
									}
								}
							}
							2 => {
								// don't-have metadata
								self.metadata_request_limit = Instant::now() + Duration::from_secs(60);
							}
									_ => {}
								}
								if let Some(m) = to_send_msg { let _ = self.send_message(m).await; }
									} else if Some(ext_id) == self.ext_ut_pex {
										{
																								if let Some(pairs) = bencode_parse_dict(&data) {
																																															let mut added_all: Vec<std::net::SocketAddr> = Vec::new();
																																															let mut added_flags: Vec<u8> = Vec::new();
																																															let mut dropped_all: Vec<std::net::SocketAddr> = Vec::new();
																									for (k, v) in pairs {
																										if k == b"added" {
																											if let Some(b) = bencode_parse_bytes(v) { added_all.extend(crate::tracker::parse_compact_peers(b)); }
																										} else if k == b"added6" {
																											if let Some(b6) = bencode_parse_bytes(v) { added_all.extend(crate::tracker::parse_compact_peers_v6(b6)); }
																										} else if k == b"dropped" {
																											if let Some(b) = bencode_parse_bytes(v) { dropped_all.extend(crate::tracker::parse_compact_peers(b)); }
																										} else if k == b"dropped6" {
																											if let Some(b6) = bencode_parse_bytes(v) { dropped_all.extend(crate::tracker::parse_compact_peers_v6(b6)); }
																																															} else if k == b"added.f" {
																																																if let Some(b) = bencode_parse_bytes(v) { added_flags.extend_from_slice(b); }
																																													}
																																															else if k == b"added6.f" {
																																																if let Some(b) = bencode_parse_bytes(v) { added_flags.extend_from_slice(b); }
																																													}
																								}

																																															if !added_all.is_empty() {
																																																if let Some(h) = &self.handle {
																																																	if let Ok(g) = h.try_read() {
																																																		// Pair flags with addresses if lengths match; otherwise, fall back
																																																		let pairs: Vec<(std::net::SocketAddr, u8)> = if added_flags.len() >= added_all.len() {
																																																			added_all.iter().cloned().zip(added_flags.iter().copied()).collect()
																																																	} else { Vec::new() };
																																																	if !pairs.is_empty() { g.broadcast_pex_added_with_flags(pairs); } else { g.broadcast_pex_added(added_all); }
																																																}
																																															}
																																															}
																								if !dropped_all.is_empty() { if let Some(h) = &self.handle { if let Ok(g) = h.try_read() { g.broadcast_pex_dropped(dropped_all); } } }
																							}
								}
				}
			}
			Message::Port(port) => { debug!(port = port, "Peer announced DHT port"); }
		}
		Ok(())
	}

	pub async fn send_message(&mut self, msg: Message) -> Result<(), LibtorrentError> { let encoded = msg.encode(); self.stream.write_all(&encoded).await?; self.last_tx = Instant::now(); Ok(()) }
	pub async fn send_choke(&mut self) -> Result<(), LibtorrentError> { self.state.am_choking = true; self.upload_permit = None; if let Some(h) = &self.handle { if let Ok(mut w) = h.try_write() { w.set_peer_am_choking(&self.remote_addr, true); } } self.send_message(Message::Choke).await }
	pub async fn send_unchoke(&mut self) -> Result<(), LibtorrentError> { self.state.am_choking = false; if self.upload_permit.is_none() { if let Some(slots) = &self.upload_slots { if let Ok(p) = slots.clone().try_acquire_owned() { self.upload_permit = Some(p); } } } if let Some(h) = &self.handle { if let Ok(mut w) = h.try_write() { w.set_peer_am_choking(&self.remote_addr, false); } } self.send_message(Message::Unchoke).await }
	pub async fn send_interested(&mut self) -> Result<(), LibtorrentError> { self.state.am_interested = true; self.send_message(Message::Interested).await }
	pub async fn send_not_interested(&mut self) -> Result<(), LibtorrentError> { self.state.am_interested = false; self.send_message(Message::NotInterested).await }

	async fn apply_upload_limit(&mut self, bytes: u64) { if self.per_peer_upload_limit == 0 { return; } let now = Instant::now(); if now.duration_since(self.window_start) >= Duration::from_secs(1) { self.window_start = now; self.sent_in_window = 0; } if self.sent_in_window + bytes > self.per_peer_upload_limit { let remaining = self.per_peer_upload_limit.saturating_sub(self.sent_in_window); if remaining < bytes { tokio::time::sleep(Duration::from_millis(50)).await; } } self.sent_in_window = self.sent_in_window.saturating_add(bytes); }
	fn encode_bitfield(bits: &[bool]) -> Vec<u8> { if bits.is_empty() { return Vec::new(); } let len = (bits.len() + 7) / 8; let mut out = vec![0u8; len]; for (i, &b) in bits.iter().enumerate() { if b { let byte = i/8; let offset = i%8; out[byte] |= 1u8 << (7 - offset); } } out }
}

// Select next metadata piece to request: pick the one with oldest last-req, enforce >=3s since last request
fn select_metadata_piece(have: &[bool], last_req: &[Instant], now: Instant) -> Option<usize> {
	let mut best_idx: Option<usize> = None;
	let mut best_time = Instant::now();
	for (i, &h) in have.iter().enumerate() {
		if h { continue; }
		let t = last_req.get(i).copied().unwrap_or(Instant::now() - Duration::from_secs(3600));
		if now.duration_since(t) < Duration::from_secs(3) { continue; }
		if best_idx.is_none() || t < best_time { best_idx = Some(i); best_time = t; }
	}
	best_idx
}

// Return number of bytes consumed by the first bencoded element in `data`
fn bencode_first_object_len(data: &[u8]) -> Option<usize> {
	fn parse_at(data: &[u8], i: &mut usize) -> Option<()> {
		if *i >= data.len() { return None; }
		match data[*i] {
			b'i' => {
				*i += 1;
				if *i >= data.len() { return None; }
				if data[*i] == b'-' { *i += 1; }
				if *i >= data.len() { return None; }
				while *i < data.len() && data[*i].is_ascii_digit() { *i += 1; }
				if *i >= data.len() || data[*i] != b'e' { return None; }
				*i += 1;
				Some(())
			}
			b'l' => {
				*i += 1;
				while *i < data.len() && data[*i] != b'e' { parse_at(data, i)?; }
				if *i >= data.len() { return None; }
				*i += 1;
				Some(())
			}
			b'd' => {
				*i += 1;
				while *i < data.len() && data[*i] != b'e' {
					// keys are strings
					parse_at(data, i)?; // key
					parse_at(data, i)?; // value
				}
				if *i >= data.len() { return None; }
				*i += 1;
				Some(())
			}
			b'0'..=b'9' => {
				let mut n: usize = 0;
				while *i < data.len() && data[*i].is_ascii_digit() { n = n.saturating_mul(10).saturating_add((data[*i] - b'0') as usize); *i += 1; }
				if *i >= data.len() || data[*i] != b':' { return None; }
				*i += 1; // skip ':'
				let cur = *i; *i = cur.saturating_add(n);
				if *i > data.len() { return None; }
				Some(())
			}
			_ => None,
		}
	}
	let mut idx = 0; parse_at(data, &mut idx)?; Some(idx)
}

// Minimal bencode helpers for dict/int/bytes parsing without external lifetimes
fn bencode_parse_int(data: &[u8]) -> Option<i64> {
	if data.first().copied()? != b'i' { return None; }
	let mut i = 1usize;
	let neg = if i < data.len() && data[i] == b'-' { i += 1; true } else { false };
	if i >= data.len() { return None; }
	let start = i;
	while i < data.len() && data[i].is_ascii_digit() { i += 1; }
	if i >= data.len() || data[i] != b'e' { return None; }
	let s = std::str::from_utf8(&data[start..i]).ok()?;
	let mut n: i64 = s.parse().ok()?;
	if neg { n = -n; }
	Some(n)
}

fn bencode_parse_bytes<'a>(data: &'a [u8]) -> Option<&'a [u8]> {
	let mut i = 0usize;
	if i >= data.len() || !data[i].is_ascii_digit() { return None; }
	let mut n: usize = 0;
	while i < data.len() && data[i].is_ascii_digit() { n = n.saturating_mul(10).saturating_add((data[i] - b'0') as usize); i += 1; }
	if i >= data.len() || data[i] != b':' { return None; }
	i += 1;
	if i + n > data.len() { return None; }
	Some(&data[i..i+n])
}

fn bencode_parse_dict<'a>(data: &'a [u8]) -> Option<Vec<(&'a [u8], &'a [u8])>> {
	if data.first().copied()? != b'd' { return None; }
	let mut i = 1usize;
	let mut out = Vec::new();
	while i < data.len() && data[i] != b'e' {
		// key (string)
		let _key_start = i;
		let mut n: usize = 0;
		if i >= data.len() || !data[i].is_ascii_digit() { return None; }
		while i < data.len() && data[i].is_ascii_digit() { n = n.saturating_mul(10).saturating_add((data[i] - b'0') as usize); i += 1; }
		if i >= data.len() || data[i] != b':' { return None; }
		i += 1;
		let key = if i + n <= data.len() { &data[i..i+n] } else { return None; };
		i += n;
		// value (any single bencode object)
		let start = i;
		let consumed = bencode_first_object_len(&data[start..])?;
		let val = &data[start..start+consumed];
		i += consumed;
		out.push((key, val));
	}
	if i >= data.len() || data[i] != b'e' { return None; }
	Some(out)
}

