use std::collections::{BTreeMap, HashSet};

/// Priority level for piece selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PiecePriority {
    Ignore = 0,
    Low = 1,
    Normal = 4,
    High = 6,
    Top = 7,
}

/// State of a single piece
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceState {
    /// Not yet downloaded
    Missing,
    /// Currently being downloaded
    Downloading,
    /// Downloaded and verified
    Complete,
}

/// A block within a piece (pieces are divided into 16KB blocks)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockInfo {
    pub piece_index: u32,
    pub block_index: u32,
}

/// Tracks which peers have which pieces
struct PieceAvailability {
    /// Number of peers that have each piece
    availability: Vec<u32>,
}

impl PieceAvailability {
    fn new(num_pieces: usize) -> Self {
        Self {
            availability: vec![0; num_pieces],
        }
    }

    fn add_peer(&mut self, bitfield: &[bool]) {
        for (idx, &has) in bitfield.iter().enumerate() {
            if has && idx < self.availability.len() {
                self.availability[idx] += 1;
            }
        }
    }

    fn remove_peer(&mut self, bitfield: &[bool]) {
        for (idx, &has) in bitfield.iter().enumerate() {
            if has && idx < self.availability.len() && self.availability[idx] > 0 {
                self.availability[idx] -= 1;
            }
        }
    }

    fn get_rarity(&self, piece_index: u32) -> u32 {
        self.availability.get(piece_index as usize).copied().unwrap_or(0)
    }
}

/// Implements piece selection strategy (rarest-first algorithm)
pub struct PiecePicker {
    num_pieces: usize,
    piece_states: Vec<PieceState>,
    piece_priorities: Vec<PiecePriority>,
    availability: PieceAvailability,
    /// Pieces currently being downloaded by any peer
    downloading: HashSet<u32>,
    /// Completed pieces
    completed: HashSet<u32>,
}

impl PiecePicker {
    pub fn new(num_pieces: usize) -> Self {
        Self {
            num_pieces,
            piece_states: vec![PieceState::Missing; num_pieces],
            piece_priorities: vec![PiecePriority::Normal; num_pieces],
            availability: PieceAvailability::new(num_pieces),
            downloading: HashSet::new(),
            completed: HashSet::new(),
        }
    }

    /// Reserve the first missing (not complete and not currently downloading) piece and mark it started.
    pub fn reserve_first_missing(&mut self) -> Option<u32> {
        for idx in 0..self.num_pieces as u32 {
            if !self.completed.contains(&idx) && !self.downloading.contains(&idx) {
                self.piece_started(idx);
                return Some(idx);
            }
        }
        None
    }

    /// Abort a reserved piece (mark back to Missing)
    pub fn abort_reservation(&mut self, piece_index: u32) {
        self.piece_aborted(piece_index);
    }

    /// Mark a piece as complete
    pub fn piece_completed(&mut self, piece_index: u32) {
        if let Some(state) = self.piece_states.get_mut(piece_index as usize) {
            *state = PieceState::Complete;
            self.downloading.remove(&piece_index);
            self.completed.insert(piece_index);
        }
    }

    /// Mark a piece as being downloaded
    pub fn piece_started(&mut self, piece_index: u32) {
        if let Some(state) = self.piece_states.get_mut(piece_index as usize) {
            if *state == PieceState::Missing {
                *state = PieceState::Downloading;
                self.downloading.insert(piece_index);
            }
        }
    }

    /// Cancel download of a piece
    pub fn piece_aborted(&mut self, piece_index: u32) {
        if let Some(state) = self.piece_states.get_mut(piece_index as usize) {
            if *state == PieceState::Downloading {
                *state = PieceState::Missing;
                self.downloading.remove(&piece_index);
            }
        }
    }

    /// Set priority for a piece
    pub fn set_piece_priority(&mut self, piece_index: u32, priority: PiecePriority) {
        if let Some(p) = self.piece_priorities.get_mut(piece_index as usize) {
            *p = priority;
        }
    }

    /// Update availability when a peer announces they have pieces
    pub fn add_peer(&mut self, peer_bitfield: &[bool]) {
        self.availability.add_peer(peer_bitfield);
    }

    /// Update availability when a peer disconnects
    pub fn remove_peer(&mut self, peer_bitfield: &[bool]) {
        self.availability.remove_peer(peer_bitfield);
    }

    /// Pick the next piece to download using rarest-first strategy
    /// Returns piece index if there's a piece to download
    pub fn pick_piece(&self, peer_has: &[bool]) -> Option<u32> {
        // Group pieces by rarity, then select rarest available piece
        let mut candidates: BTreeMap<u32, Vec<u32>> = BTreeMap::new();

        for piece_idx in 0..self.num_pieces as u32 {
            // Skip if already complete or downloading
            if self.completed.contains(&piece_idx) || self.downloading.contains(&piece_idx) {
                continue;
            }

            // Skip if peer doesn't have this piece
            if !peer_has.get(piece_idx as usize).copied().unwrap_or(false) {
                continue;
            }

            // Skip if priority is Ignore
            let priority = self.piece_priorities.get(piece_idx as usize).copied().unwrap_or(PiecePriority::Normal);
            if priority == PiecePriority::Ignore {
                continue;
            }

            let rarity = self.availability.get_rarity(piece_idx);
            if rarity > 0 {
                candidates.entry(rarity).or_insert_with(Vec::new).push(piece_idx);
            }
        }

        // Pick from rarest group (lowest rarity count)
        candidates.values().next().and_then(|pieces| pieces.first().copied())
    }

    /// Check if download is complete
    pub fn is_complete(&self) -> bool {
        self.completed.len() == self.num_pieces
    }

    /// Get download progress (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        if self.num_pieces == 0 {
            return 1.0;
        }
        self.completed.len() as f64 / self.num_pieces as f64
    }

    /// Get number of completed pieces
    pub fn num_completed(&self) -> usize {
        self.completed.len()
    }

    /// Get total number of pieces
    pub fn num_pieces(&self) -> usize {
        self.num_pieces
    }

    /// Check if a specific piece is complete
    pub fn has_piece(&self, piece_index: u32) -> bool {
        self.completed.contains(&piece_index)
    }

    /// Get bitfield representing which pieces we have
    pub fn get_bitfield(&self) -> Vec<bool> {
        (0..self.num_pieces as u32)
            .map(|idx| self.has_piece(idx))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_piece_picker_basic() {
        let mut picker = PiecePicker::new(10);
        
        assert_eq!(picker.num_pieces(), 10);
        assert_eq!(picker.num_completed(), 0);
        assert!(!picker.is_complete());
        assert_eq!(picker.progress(), 0.0);
    }

    #[test]
    fn test_piece_completion() {
        let mut picker = PiecePicker::new(5);
        
        picker.piece_completed(0);
        picker.piece_completed(2);
        
        assert!(picker.has_piece(0));
        assert!(!picker.has_piece(1));
        assert!(picker.has_piece(2));
        assert_eq!(picker.num_completed(), 2);
        assert_eq!(picker.progress(), 0.4);
    }

    #[test]
    fn test_rarest_first_selection() {
        let mut picker = PiecePicker::new(5);
        
        // Peer 1 has pieces 0, 1, 2
        let peer1 = vec![true, true, true, false, false];
        picker.add_peer(&peer1);
        
        // Peer 2 has pieces 1, 2, 3
        let peer2 = vec![false, true, true, true, false];
        picker.add_peer(&peer2);
        
        // Peer 3 has pieces 2, 3, 4
        let peer3 = vec![false, false, true, true, true];
        picker.add_peer(&peer3);
        
        // Now rarities are: [1, 2, 3, 2, 1]
        // When peer with [true, true, true, false, false] asks,
        // should pick piece 0 (rarest with availability 1)
        let pick = picker.pick_piece(&peer1);
        assert_eq!(pick, Some(0));
        
        // Mark piece 0 as downloading
        picker.piece_started(0);
        
        // Next pick should be piece 1 (next rarest with availability 2)
        let pick = picker.pick_piece(&peer1);
        assert_eq!(pick, Some(1));
    }

    #[test]
    fn test_piece_priority() {
        let mut picker = PiecePicker::new(5);
        
        let peer = vec![true; 5];
        picker.add_peer(&peer);
        
        // Ignore piece 0
        picker.set_piece_priority(0, PiecePriority::Ignore);
        
        // Should skip piece 0 and pick piece 1
        let pick = picker.pick_piece(&peer);
        assert_eq!(pick, Some(1));
    }

    #[test]
    fn test_completion_check() {
        let mut picker = PiecePicker::new(3);
        
        assert!(!picker.is_complete());
        
        picker.piece_completed(0);
        picker.piece_completed(1);
        assert!(!picker.is_complete());
        
        picker.piece_completed(2);
        assert!(picker.is_complete());
    }

    #[test]
    fn test_bitfield_generation() {
        let mut picker = PiecePicker::new(5);
        
        picker.piece_completed(0);
        picker.piece_completed(2);
        picker.piece_completed(4);
        
        let bitfield = picker.get_bitfield();
        assert_eq!(bitfield, vec![true, false, true, false, true]);
    }
}
