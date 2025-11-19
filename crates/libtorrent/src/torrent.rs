use crate::metainfo::{TorrentMeta, InfoDict};
use crate::disk::DiskManager;
use std::path::Path;
use crate::piece_picker::PiecePicker;
use std::collections::HashMap;
use libtorrent_proto::BLOCK_SIZE;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

/// State of a torrent
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentState {
    /// Waiting to start downloading
    Queued,
    /// Checking existing files
    Checking,
    /// Actively downloading
    Downloading,
    /// Download complete, now seeding
    Seeding,
    /// Paused by user
    Paused,
    /// Error occurred
    Error,
}

/// Statistics for a torrent
#[derive(Debug, Clone, Default)]
pub struct TorrentStats {
    /// Total bytes downloaded
    pub downloaded: u64,
    /// Total bytes uploaded
    pub uploaded: u64,
    /// Download rate in bytes/sec
    pub download_rate: u64,
    /// Upload rate in bytes/sec
    pub upload_rate: u64,
    /// Number of connected peers
    pub num_peers: usize,
    /// Number of seeders
    pub num_seeds: usize,
}

/// Information about a connected peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    pub peer_id: [u8; 20],
    /// Which pieces this peer has
    pub bitfield: Vec<bool>,
    /// Whether we are choked by this peer
    pub peer_choking: bool,
    /// Whether we are interested in this peer
    pub am_interested: bool,
    /// Whether this peer is choked by us
    pub am_choking: bool,
    /// Whether this peer is interested in us
    pub peer_interested: bool,
    /// Bytes downloaded from this peer since last choke cycle
    pub recent_downloaded: u64,
    /// ut_pex flags hint (bitfield: 0x01 encryption, 0x02 seed, 0x04 uTP, 0x08 holepunch)
    pub pex_flags: u8,
}

/// Handle to a torrent being downloaded/seeded
pub struct TorrentHandle {
    meta: TorrentMeta,
    state: TorrentState,
    piece_picker: PiecePicker,
    peers: HashMap<SocketAddr, PeerInfo>,
    stats: TorrentStats,
    disk: Option<DiskManager>,
    piece_sizes: Vec<u32>,
    piece_blocks: Vec<Vec<bool>>,
    events: broadcast::Sender<TorrentEvent>,
}

#[derive(Debug, Clone)]
pub enum TorrentEvent {
    Have(u32),
    Choke(SocketAddr),
    Unchoke(SocketAddr),
    // Announce PEX-added peers (PeerSession -> Session)
    PexAdded(Vec<SocketAddr>),
    // Announce PEX-dropped peers (PeerSession -> Session)
    PexDropped(Vec<SocketAddr>),
    // Announce PEX-added peers with flags (PeerSession -> Session)
    PexAddedWithFlags(Vec<(SocketAddr, u8)>),
}

impl TorrentHandle {
    /// Create a new torrent handle from metadata
    pub fn new(meta: TorrentMeta) -> Self {
        let num_pieces = meta.info.num_pieces();
        // compute per-piece sizes and initialize block tracking
        let piece_len = meta.info.piece_length;
        let total = meta.info.total_size() as u64;
        let last_len = if num_pieces == 0 { 0 } else { (total - (piece_len as u64) * (num_pieces as u64 - 1)) as u32 };
        let mut piece_sizes = vec![piece_len; num_pieces];
        if num_pieces > 0 { piece_sizes[num_pieces - 1] = last_len; }
        let mut piece_blocks = Vec::with_capacity(num_pieces);
        for &sz in &piece_sizes {
            let blocks = ((sz + (BLOCK_SIZE - 1)) / BLOCK_SIZE) as usize;
            piece_blocks.push(vec![false; blocks.max(1)]);
        }

        let (tx, _rx) = broadcast::channel(128);
        Self {
            meta,
            state: TorrentState::Queued,
            piece_picker: PiecePicker::new(num_pieces),
            peers: HashMap::new(),
            stats: TorrentStats::default(),
            disk: None,
            piece_sizes,
            piece_blocks,
            events: tx,
        }
    }

    /// Reserve the next missing piece (not completed / not downloading) and mark it started.
    pub fn reserve_missing_piece(&mut self) -> Option<u32> {
        self.piece_picker.reserve_first_missing()
    }

    /// Abort a reserved piece (revert to Missing state)
    pub fn abort_piece(&mut self, piece_index: u32) {
        self.piece_picker.abort_reservation(piece_index);
    }

    /// Get torrent metadata
    pub fn meta(&self) -> &TorrentMeta {
        &self.meta
    }

    /// Get current state
    pub fn state(&self) -> TorrentState {
        self.state
    }

    /// Set torrent state
    pub fn set_state(&mut self, state: TorrentState) {
        self.state = state;
    }

    /// Get piece picker
    pub fn piece_picker(&self) -> &PiecePicker {
        &self.piece_picker
    }

    /// Get mutable piece picker
    pub fn piece_picker_mut(&mut self) -> &mut PiecePicker {
        &mut self.piece_picker
    }

    /// Add a peer
    pub fn add_peer(&mut self, peer: PeerInfo) {
        self.piece_picker.add_peer(&peer.bitfield);
        self.peers.insert(peer.addr, peer);
        self.update_stats();
    }

    /// Remove a peer
    pub fn remove_peer(&mut self, addr: &SocketAddr) {
        if let Some(peer) = self.peers.remove(addr) {
            self.piece_picker.remove_peer(&peer.bitfield);
            self.update_stats();
        }
    }

    /// Update peer bitfield when they announce a new piece
    pub fn peer_has_piece(&mut self, addr: &SocketAddr, piece_index: u32) {
        if let Some(peer) = self.peers.get_mut(addr) {
            let idx = piece_index as usize;
            if idx < peer.bitfield.len() && !peer.bitfield[idx] {
                let old = peer.bitfield.clone();
                peer.bitfield[idx] = true;
                self.piece_picker.remove_peer(&old);
                self.piece_picker.add_peer(&peer.bitfield);
                self.update_stats();
            }
        }
    }

    /// Replace a peer's entire bitfield and update availability counters
    pub fn update_peer_bitfield(&mut self, addr: &SocketAddr, new_bitfield: Vec<bool>) {
        if let Some(peer) = self.peers.get_mut(addr) {
            let old = peer.bitfield.clone();
            self.piece_picker.remove_peer(&old);
            peer.bitfield = new_bitfield;
            self.piece_picker.add_peer(&peer.bitfield);
            self.update_stats();
        }
    }

    /// Get list of all peers
    pub fn peers(&self) -> impl Iterator<Item = &PeerInfo> {
        self.peers.values()
    }
    /// Get mutable iterator over peers
    pub fn peers_mut(&mut self) -> impl Iterator<Item = (&SocketAddr, &mut PeerInfo)> {
        self.peers.iter_mut().map(|(k,v)| (k, v))
    }

    /// Get statistics
    pub fn stats(&self) -> &TorrentStats {
        &self.stats
    }

    /// Update statistics
    fn update_stats(&mut self) {
        self.stats.num_peers = self.peers.len();
        self.stats.num_seeds = self.peers.values()
            .filter(|p| p.bitfield.iter().all(|&has| has))
            .count();
    }

    /// Record downloaded bytes
    pub fn add_downloaded(&mut self, bytes: u64) {
        self.stats.downloaded += bytes;
    }

    /// Record uploaded bytes
    pub fn add_uploaded(&mut self, bytes: u64) {
        self.stats.uploaded += bytes;
    }

    /// Check if download is complete
    pub fn is_complete(&self) -> bool {
        self.piece_picker.is_complete()
    }

    /// Get download progress (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        self.piece_picker.progress()
    }

    /// Get our bitfield
    pub fn get_bitfield(&self) -> Vec<bool> {
        self.piece_picker.get_bitfield()
    }

    /// Initialize disk manager for this torrent
    pub async fn init_disk<P: AsRef<Path>>(&mut self, base: P) -> std::io::Result<()> {
        if self.disk.is_none() {
            let dm = DiskManager::new(base, &self.meta.info).await?;
            self.disk = Some(dm);
        }
        Ok(())
    }

    pub fn has_disk(&self) -> bool { self.disk.is_some() }

    pub fn disk(&self) -> Option<&DiskManager> { self.disk.as_ref() }

    pub fn disk_mut(&mut self) -> Option<&mut DiskManager> { self.disk.as_mut() }

    /// Return a clone of a peer's bitfield, if known
    pub fn peer_bitfield_clone(&self, addr: &SocketAddr) -> Option<Vec<bool>> {
        self.peers.get(addr).map(|p| p.bitfield.clone())
    }

    /// Subscribe to torrent events (e.g., Have broadcasts)
    pub fn events_subscribe(&self) -> broadcast::Receiver<TorrentEvent> {
        self.events.subscribe()
    }

    /// Broadcast a Have event to all peer sessions (best-effort)
    pub fn broadcast_have(&self, piece: u32) {
        let _ = self.events.send(TorrentEvent::Have(piece));
    }

    /// Broadcast choke command to a specific peer
    pub fn broadcast_choke(&self, addr: SocketAddr) {
        let _ = self.events.send(TorrentEvent::Choke(addr));
    }

    /// Broadcast unchoke command to a specific peer
    pub fn broadcast_unchoke(&self, addr: SocketAddr) {
        let _ = self.events.send(TorrentEvent::Unchoke(addr));
    }

    /// Broadcast PEX added peers (session will handle connecting)
    pub fn broadcast_pex_added(&self, peers: Vec<SocketAddr>) {
        let _ = self.events.send(TorrentEvent::PexAdded(peers));
    }

    /// Broadcast PEX dropped peers (session may prune or ignore)
    pub fn broadcast_pex_dropped(&self, peers: Vec<SocketAddr>) {
        let _ = self.events.send(TorrentEvent::PexDropped(peers));
    }

    /// Broadcast PEX added peers with flags (session will handle connecting)
    pub fn broadcast_pex_added_with_flags(&self, peers: Vec<(SocketAddr, u8)>) {
        let _ = self.events.send(TorrentEvent::PexAddedWithFlags(peers));
    }

    /// Find next missing block for a piece, skipping any begins in `skip_begins`.
    pub fn next_missing_block_for_piece(&self, piece_index: u32, skip_begins: &[u32]) -> Option<(u32, u32)> {
        let p = piece_index as usize;
        if p >= self.piece_blocks.len() { return None; }
        let row = &self.piece_blocks[p];
        for (bi, &recvd) in row.iter().enumerate() {
            if !recvd {
                let begin = (bi as u32) * BLOCK_SIZE;
                if skip_begins.iter().any(|&b| b == begin) { continue; }
                let piece_sz = self.piece_sizes.get(p).copied().unwrap_or(0);
                let remain = piece_sz.saturating_sub(begin);
                let len = BLOCK_SIZE.min(remain);
                if len == 0 { continue; }
                return Some((begin, len));
            }
        }
        None
    }

    /// Record a received block and verify/complete piece if done.
    /// Returns true if the piece was verified and marked complete.
    pub async fn record_block_and_maybe_complete(&mut self, piece_index: u32, begin: u32, len: u32) -> std::io::Result<bool> {
        let p = piece_index as usize;
        if p >= self.piece_blocks.len() { return Ok(false); }
        // mark covered blocks as received
        let mut off = begin;
        let end = begin.saturating_add(len);
        while off < end {
            let bi = (off / BLOCK_SIZE) as usize;
            if let Some(row) = self.piece_blocks.get_mut(p) {
                if bi < row.len() { row[bi] = true; }
            }
            let next = ((off / BLOCK_SIZE) + 1) * BLOCK_SIZE;
            if next <= off { break; }
            off = next;
        }

        // if all blocks set, verify and mark complete
        let complete = self.piece_blocks[p].iter().all(|&b| b);
        if complete {
            if let Some(dm) = self.disk.as_ref() {
                if dm.verify_piece(piece_index).await.unwrap_or(false) {
                    self.piece_picker.piece_completed(piece_index as u32);
                    // broadcast Have to sessions
                    self.broadcast_have(piece_index);
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Record bytes downloaded from a specific peer for tit-for-tat accounting
    pub fn record_peer_download(&mut self, addr: &SocketAddr, bytes: u64) {
        if let Some(p) = self.peers.get_mut(addr) {
            p.recent_downloaded = p.recent_downloaded.saturating_add(bytes);
        }
    }

    /// Take and reset recent download counters for interested peers
    pub fn take_interested_recent_downloads(&mut self) -> Vec<(SocketAddr, u64, bool)> {
        let mut out = Vec::with_capacity(self.peers.len());
        for (addr, p) in self.peers.iter_mut() {
            if p.peer_interested {
                out.push((*addr, p.recent_downloaded, p.am_choking));
            }
            p.recent_downloaded = 0;
        }
        out
    }

    /// Update a peer's interested flag
    pub fn set_peer_interested(&mut self, addr: &SocketAddr, interested: bool) {
        if let Some(p) = self.peers.get_mut(addr) { p.peer_interested = interested; }
    }

    /// Update a peer's am_choking flag
    pub fn set_peer_am_choking(&mut self, addr: &SocketAddr, am_choking: bool) {
        if let Some(p) = self.peers.get_mut(addr) { p.am_choking = am_choking; }
    }

    /// Apply metadata (info dict bencode) discovered via ut_metadata.
    /// Re-initializes picker and piece maps. Does not initialize disk.
    pub fn apply_metadata_from_info_bytes(&mut self, info_bytes: Vec<u8>) -> Result<(), crate::error::LibtorrentError> {
        let info = InfoDict::from_info_bytes(&info_bytes)?;
        // Construct a minimal TorrentMeta preserving trackers if any (unknown for magnets)
        let announce = self.meta.announce.clone();
        let announce_list = self.meta.announce_list.clone();
        self.meta = TorrentMeta { announce, announce_list, web_seeds: self.meta.web_seeds.clone(), info };

        // Rebuild piece sizes and block maps
        let num_pieces = self.meta.info.num_pieces();
        let piece_len = self.meta.info.piece_length;
        let total = self.meta.info.total_size() as u64;
        let last_len = if num_pieces == 0 { 0 } else { (total - (piece_len as u64) * (num_pieces as u64 - 1)) as u32 };
        self.piece_sizes = vec![piece_len; num_pieces];
        if num_pieces > 0 { self.piece_sizes[num_pieces - 1] = last_len; }
        self.piece_blocks.clear();
        self.piece_blocks.reserve(num_pieces);
        for &sz in &self.piece_sizes {
            let blocks = ((sz + (BLOCK_SIZE - 1)) / BLOCK_SIZE) as usize;
            self.piece_blocks.push(vec![false; blocks.max(1)]);
        }

        // Reset picker and fold in current peers' bitfields (resize as needed)
        let mut new_picker = PiecePicker::new(num_pieces);
        for p in self.peers.values_mut() {
            if p.bitfield.len() != num_pieces { p.bitfield = vec![false; num_pieces]; }
            new_picker.add_peer(&p.bitfield);
        }
        self.piece_picker = new_picker;

        // Reset state if previously error/paused
        if self.state == TorrentState::Queued || self.state == TorrentState::Paused || self.state == TorrentState::Error { self.state = TorrentState::Queued; }
        Ok(())
    }
}

/// Thread-safe handle to a torrent
pub type SharedTorrentHandle = Arc<RwLock<TorrentHandle>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::InfoDict;

    fn create_test_meta() -> TorrentMeta {
        use crate::metainfo::FileEntry;
        use std::path::PathBuf;
        
        TorrentMeta {
            announce: "http://tracker.example.com:8080/announce".to_string(),
            announce_list: Vec::new(),
            web_seeds: Vec::new(),
            info: InfoDict {
                name: "test.txt".to_string(),
                piece_length: 16384,
                pieces: vec![[0u8; 20]], // 1 piece
                files: vec![FileEntry {
                    length: 1000,
                    path: PathBuf::from("test.txt"),
                }],
                private: false,
                raw_infohash: [0u8; 20],
                raw_info_bencode: Vec::new(),
            },
        }
    }

    #[test]
    fn test_torrent_handle_creation() {
        let meta = create_test_meta();
        let handle = TorrentHandle::new(meta);
        
        assert_eq!(handle.state(), TorrentState::Queued);
        assert_eq!(handle.progress(), 0.0);
        assert!(!handle.is_complete());
    }

    #[test]
    fn test_peer_management() {
        let meta = create_test_meta();
        let mut handle = TorrentHandle::new(meta);
        
        let peer = PeerInfo {
            addr: "127.0.0.1:6881".parse().unwrap(),
            peer_id: [1u8; 20],
            bitfield: vec![true],
            peer_choking: true,
            am_interested: false,
            am_choking: true,
            peer_interested: false,
            recent_downloaded: 0,
            pex_flags: 0,
        };
        
        handle.add_peer(peer.clone());
        assert_eq!(handle.stats().num_peers, 1);
        assert_eq!(handle.stats().num_seeds, 1);
        
        handle.remove_peer(&peer.addr);
        assert_eq!(handle.stats().num_peers, 0);
    }

    #[test]
    fn test_statistics_tracking() {
        let meta = create_test_meta();
        let mut handle = TorrentHandle::new(meta);
        
        handle.add_downloaded(1000);
        handle.add_uploaded(500);
        
        assert_eq!(handle.stats().downloaded, 1000);
        assert_eq!(handle.stats().uploaded, 500);
    }

    #[test]
    fn test_state_transitions() {
        let meta = create_test_meta();
        let mut handle = TorrentHandle::new(meta);
        
        assert_eq!(handle.state(), TorrentState::Queued);
        
        handle.set_state(TorrentState::Downloading);
        assert_eq!(handle.state(), TorrentState::Downloading);
        
        handle.piece_picker_mut().piece_completed(0);
        assert!(handle.is_complete());
        assert_eq!(handle.progress(), 1.0);
        
        handle.set_state(TorrentState::Seeding);
        assert_eq!(handle.state(), TorrentState::Seeding);
    }

    #[test]
    fn test_apply_metadata_from_info_bytes_single_file() {
        // Info dict only (no outer torrent dict)
        let info_bytes = b"d6:lengthi100e4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890e";
        let mut handle = TorrentHandle::new(create_test_meta());
        assert!(handle.meta.info.raw_info_bencode.is_empty());
        handle.apply_metadata_from_info_bytes(info_bytes.to_vec()).unwrap();
        assert_eq!(handle.meta.info.name, "test");
        assert_eq!(handle.meta.info.piece_length, 16384);
        assert_eq!(handle.piece_sizes.len(), 1);
        assert_eq!(handle.state(), TorrentState::Queued);
        assert!(!handle.meta.info.raw_info_bencode.is_empty());
    }
}

