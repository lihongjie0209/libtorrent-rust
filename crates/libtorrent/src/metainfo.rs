use bendy::decoding::{Decoder, Error};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::path::PathBuf;

/// Torrent metadata structure supporting both v1 (BEP 3) and v2 (BEP 52) torrents
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentMeta {
    pub announce: String,
    pub announce_list: Vec<Vec<String>>, // tracker tiers
    pub web_seeds: Vec<String>,
    pub info: InfoDict,
    
    // Additional metadata fields
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>, // Unix timestamp
    pub encoding: Option<String>,
}

/// Info dictionary - can be v1, v2, or hybrid
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfoDict {
    pub name: String,
    pub piece_length: u32,
    pub pieces: Vec<[u8; 20]>, // v1 SHA-1 hashes
    pub files: Vec<FileEntry>,
    pub private: bool,
    pub raw_infohash: [u8; 20], // v1 infohash
    pub raw_info_bencode: Vec<u8>,
    
    // BEP 52 (v2) fields
    pub meta_version: u8, // 1 for v1, 2 for v2, or hybrid
    pub file_tree: Option<Vec<u8>>, // Raw bencode of file tree for v2
    pub pieces_v2: Option<Vec<[u8; 32]>>, // v2 SHA-256 piece hashes
    pub raw_infohash_v2: Option<[u8; 32]>, // v2 infohash (SHA-256)
}

/// File entry with BEP 47 padding file support
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub length: u64,
    pub path: PathBuf,
    pub attr: Option<String>, // BEP 47: 'p' for padding files, 'x' for executable, 'h' for hidden, 'l' for symlink
    pub sha1: Option<[u8; 20]>, // Optional per-file SHA-1 hash
    pub md5sum: Option<String>, // Legacy MD5 sum
    pub mtime: Option<i64>, // BEP 47: modification time
    pub symlink_path: Option<PathBuf>, // BEP 47: symlink target
}

impl TorrentMeta {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut decoder = Decoder::new(bytes);
        let mut dict = decoder
            .next_object()?
            .ok_or_else(|| Error::unexpected_token("Dict", "Eof"))?
            .try_into_dictionary()?;

        let mut announce = None;
        let mut info_bytes = None;
        let mut announce_list: Vec<Vec<String>> = Vec::new();
        let mut web_seeds: Vec<String> = Vec::new();
        let mut comment = None;
        let mut created_by = None;
        let mut creation_date = None;
        let mut encoding = None;
        
        while let Some((key, value)) = dict.next_pair()? {
            match key {
                b"announce" => {
                    let val = value.try_into_bytes()?;
                    announce = Some(String::from_utf8_lossy(val).into_owned());
                }
                b"announce-list" => {
                    // list of lists of strings
                    if let Ok(mut tiers) = value.try_into_list() {
                        while let Some(tier_obj) = tiers.next_object()? {
                            if let Ok(mut tier_list) = tier_obj.try_into_list() {
                                let mut tier: Vec<String> = Vec::new();
                                while let Some(url_obj) = tier_list.next_object()? {
                                    if let Ok(bytes) = url_obj.try_into_bytes() {
                                        tier.push(String::from_utf8_lossy(bytes).into_owned());
                                    }
                                }
                                if !tier.is_empty() { announce_list.push(tier); }
                            }
                        }
                    }
                }
                b"url-list" => {
                    // BEP 19: url-list can be a string or list of strings
                    // Try list first, if that fails it might be a single string (but we can't recover the object)
                    if let Ok(mut list) = value.try_into_list() {
                        while let Some(elem) = list.next_object()? {
                            if let Ok(bytes) = elem.try_into_bytes() {
                                web_seeds.push(String::from_utf8_lossy(bytes).into_owned());
                            }
                        }
                    }
                    // Note: bendy's API doesn't allow fallback to try_into_bytes after try_into_list fails
                    // Single string url-list is rare in practice
                }
                b"httpseeds" => {
                    // Legacy key: list of strings
                    if let Ok(mut list) = value.try_into_list() {
                        while let Some(elem) = list.next_object()? {
                            if let Ok(bytes) = elem.try_into_bytes() {
                                web_seeds.push(String::from_utf8_lossy(bytes).into_owned());
                            }
                        }
                    }
                }
                b"comment" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        comment = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"created by" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        created_by = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"creation date" => {
                    if let Ok(int_str) = value.try_into_integer() {
                        if let Ok(timestamp) = int_str.parse::<i64>() {
                            creation_date = Some(timestamp);
                        }
                    }
                }
                b"encoding" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        encoding = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"info" => {
                    let dict = value.try_into_dictionary()?;
                    let raw = dict.into_raw()?;
                    info_bytes = Some(raw.to_vec());
                }
                _ => {}
            }
        }

        let announce = announce.unwrap_or_default(); // BEP 52: announce is optional for v2-only torrents
        let info = info_bytes.ok_or_else(|| Error::missing_field("info"))?;
        let info = InfoDict::from_info_bytes(&info)?;
        // Deduplicate web seeds
        web_seeds.sort(); web_seeds.dedup();
        Ok(Self { 
            announce, 
            announce_list, 
            web_seeds, 
            info,
            comment,
            created_by,
            creation_date,
            encoding,
        })
    }
    
    /// Get all trackers as a flat list (all tiers combined)
    pub fn all_trackers(&self) -> Vec<String> {
        let mut trackers = Vec::new();
        if !self.announce.is_empty() {
            trackers.push(self.announce.clone());
        }
        for tier in &self.announce_list {
            for tracker in tier {
                if !trackers.contains(tracker) {
                    trackers.push(tracker.clone());
                }
            }
        }
        trackers
    }
}

impl TorrentMeta {
    /// Return trackers organized by tiers. Falls back to single primary announce if list is empty.
    pub fn trackers_by_tier(&self) -> Vec<Vec<String>> {
        if !self.announce_list.is_empty() { self.announce_list.clone() } else { vec![vec![self.announce.clone()]] }
    }
}

impl InfoDict {
    /// Get the number of pieces
    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    /// Get total size of all files
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.length).sum()
    }

    /// Check if this is a single-file torrent
    pub fn is_single_file(&self) -> bool {
        self.files.len() == 1
    }
    
    /// Check if this is a v2 torrent
    pub fn is_v2(&self) -> bool {
        self.meta_version == 2
    }
    
    /// Check if this is a hybrid torrent (both v1 and v2)
    pub fn is_hybrid(&self) -> bool {
        self.meta_version == 2 && !self.pieces.is_empty()
    }
    
    /// Get all non-padding files (BEP 47)
    pub fn non_padding_files(&self) -> Vec<&FileEntry> {
        self.files.iter().filter(|f| !f.is_padding()).collect()
    }

    pub fn from_info_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut decoder = Decoder::new(bytes);
        let mut dict = decoder
            .next_object()?
            .ok_or_else(|| Error::unexpected_token("Dict", "Eof"))?
            .try_into_dictionary()?;

        let mut name = None;
        let mut piece_length = None;
        let mut pieces = None;
        let mut files = Vec::new();
        let mut single_file_length = None;
        let mut single_file_attr = None;
        let mut single_file_sha1 = None;
        let mut single_file_md5sum = None;
        let mut single_file_mtime = None;
        let mut single_file_symlink = None;
        let mut private = false;
        let mut meta_version = 1u8;
        let mut file_tree_bytes: Option<Vec<u8>> = None;
        let mut pieces_v2_root: Option<[u8; 32]> = None;

        while let Some((key, value)) = dict.next_pair()? {
            match key {
                b"name" => {
                    name = Some(String::from_utf8_lossy(value.try_into_bytes()?).into_owned());
                }
                b"piece length" => {
                    let len = value.try_into_integer()?;
                    piece_length = Some(len.parse::<u32>().map_err(|e| Error::malformed_content(e))?);
                }
                b"pieces" => {
                    let blob = value.try_into_bytes()?;
                    if blob.len() % 20 != 0 {
                        return Err(Error::malformed_content(
                            std::io::Error::new(std::io::ErrorKind::InvalidData, "pieces not multiple of 20")
                        ));
                    }
                    let hashes = blob
                        .chunks_exact(20)
                        .map(|chunk| {
                            let mut arr = [0u8; 20];
                            arr.copy_from_slice(chunk);
                            arr
                        })
                        .collect();
                    pieces = Some(hashes);
                }
                b"files" => {
                    files = decode_files(value)?;
                }
                b"length" => {
                    let len = value.try_into_integer()?;
                    single_file_length = Some(len.parse::<u64>().map_err(|e| Error::malformed_content(e))?);
                }
                b"private" => {
                    private = value.try_into_integer()?.parse::<u8>().unwrap_or(0) != 0;
                }
                // BEP 47: Single-file attributes
                b"attr" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        single_file_attr = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"sha1" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        if bytes.len() == 20 {
                            let mut arr = [0u8; 20];
                            arr.copy_from_slice(bytes);
                            single_file_sha1 = Some(arr);
                        }
                    }
                }
                b"md5sum" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        single_file_md5sum = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"mtime" => {
                    if let Ok(int_str) = value.try_into_integer() {
                        if let Ok(timestamp) = int_str.parse::<i64>() {
                            single_file_mtime = Some(timestamp);
                        }
                    }
                }
                b"symlink" => {
                    if let Ok(mut path_list) = value.try_into_list() {
                        let mut parts = Vec::new();
                        while let Some(elem) = path_list.next_object()? {
                            if let Ok(bytes) = elem.try_into_bytes() {
                                parts.push(String::from_utf8_lossy(bytes).into_owned());
                            }
                        }
                        single_file_symlink = Some(parts.into_iter().collect());
                    }
                }
                // BEP 52: v2 torrent fields
                b"meta version" => {
                    if let Ok(int_str) = value.try_into_integer() {
                        if let Ok(ver) = int_str.parse::<u8>() {
                            meta_version = ver;
                        }
                    }
                }
                b"file tree" => {
                    let dict = value.try_into_dictionary()?;
                    let raw = dict.into_raw()?;
                    file_tree_bytes = Some(raw.to_vec());
                }
                b"pieces root" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        if bytes.len() == 32 {
                            let mut arr = [0u8; 32];
                            arr.copy_from_slice(bytes);
                            pieces_v2_root = Some(arr);
                        }
                    }
                }
                _ => {}
            }
        }

        // Handle single-file torrent
        if files.is_empty() {
            let len = single_file_length.ok_or_else(|| Error::missing_field("length"))?;
            let path = PathBuf::from(name.clone().unwrap_or_default());
            files.push(FileEntry { 
                length: len, 
                path,
                attr: single_file_attr,
                sha1: single_file_sha1,
                md5sum: single_file_md5sum,
                mtime: single_file_mtime,
                symlink_path: single_file_symlink,
            });
        }

        let name = name.ok_or_else(|| Error::missing_field("name"))?;
        let piece_length = piece_length.unwrap_or(16384); // BEP 52: optional for v2-only
        let pieces = pieces.unwrap_or_default(); // BEP 52: optional for v2-only
        
        // Calculate v1 infohash (SHA-1)
        let mut hasher = Sha1::new();
        hasher.update(bytes);
        let mut raw = [0u8; 20];
        raw.copy_from_slice(&hasher.finalize());
        
        // Calculate v2 infohash (SHA-256) if this is a v2 torrent
        let raw_infohash_v2 = if meta_version == 2 {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&hasher.finalize());
            Some(arr)
        } else {
            None
        };
        
        Ok(Self {
            name,
            piece_length,
            pieces,
            files,
            private,
            raw_infohash: raw,
            raw_info_bencode: bytes.to_vec(),
            meta_version,
            file_tree: file_tree_bytes,
            pieces_v2: pieces_v2_root.map(|root| vec![root]), // Simplified; full v2 support needs piece tree parsing
            raw_infohash_v2,
        })
    }
}

impl FileEntry {
    /// Check if this is a padding file (BEP 47)
    pub fn is_padding(&self) -> bool {
        self.attr.as_deref() == Some("p")
    }
    
    /// Check if this is an executable file (BEP 47)
    pub fn is_executable(&self) -> bool {
        self.attr.as_ref().map_or(false, |a| a.contains('x'))
    }
    
    /// Check if this is a hidden file (BEP 47)
    pub fn is_hidden(&self) -> bool {
        self.attr.as_ref().map_or(false, |a| a.contains('h'))
    }
    
    /// Check if this is a symlink (BEP 47)
    pub fn is_symlink(&self) -> bool {
        self.symlink_path.is_some() || self.attr.as_ref().map_or(false, |a| a.contains('l'))
    }
}

fn decode_files(list_obj: bendy::decoding::Object) -> Result<Vec<FileEntry>, Error> {
    let mut list = list_obj.try_into_list()?;
    let mut files = Vec::new();
    while let Some(entry) = list.next_object()? {
        let mut dict = entry.try_into_dictionary()?;
        let mut length = None;
        let mut path_parts = Vec::new();
        let mut attr = None;
        let mut sha1 = None;
        let mut md5sum = None;
        let mut mtime = None;
        let mut symlink_path = None;
        
        while let Some((key, value)) = dict.next_pair()? {
            match key {
                b"length" => {
                    length = Some(
                        value
                            .try_into_integer()?.parse::<u64>()
                            .map_err(|e| Error::malformed_content(e))?,
                    );
                }
                b"path" => {
                    let mut list = value.try_into_list()?;
                    while let Some(component) = list.next_object()? {
                        let bytes = component.try_into_bytes()?;
                        path_parts.push(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                // BEP 47: File attributes
                b"attr" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        attr = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"sha1" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        if bytes.len() == 20 {
                            let mut arr = [0u8; 20];
                            arr.copy_from_slice(bytes);
                            sha1 = Some(arr);
                        }
                    }
                }
                b"md5sum" => {
                    if let Ok(bytes) = value.try_into_bytes() {
                        md5sum = Some(String::from_utf8_lossy(bytes).into_owned());
                    }
                }
                b"mtime" => {
                    if let Ok(int_str) = value.try_into_integer() {
                        if let Ok(timestamp) = int_str.parse::<i64>() {
                            mtime = Some(timestamp);
                        }
                    }
                }
                b"symlink" => {
                    if let Ok(mut symlink_list) = value.try_into_list() {
                        let mut parts = Vec::new();
                        while let Some(elem) = symlink_list.next_object()? {
                            if let Ok(bytes) = elem.try_into_bytes() {
                                parts.push(String::from_utf8_lossy(bytes).into_owned());
                            }
                        }
                        symlink_path = Some(parts.into_iter().collect());
                    }
                }
                _ => {}
            }
        }
        files.push(FileEntry {
            length: length.ok_or_else(|| Error::missing_field("file length"))?,
            path: path_parts.into_iter().collect(),
            attr,
            sha1,
            md5sum,
            mtime,
            symlink_path,
        });
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_file_torrent() {
        // Simple single-file torrent: d8:announce9:test-host4:infod6:lengthi100e4:name8:test.txt12:piece lengthi16384e6:pieces20:12345678901234567890ee
        let torrent_bytes = b"d8:announce9:test-host4:infod6:lengthi100e4:name8:test.txt12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        
        let meta = TorrentMeta::from_bytes(torrent_bytes).unwrap();
        
        assert_eq!(meta.announce, "test-host");
        assert_eq!(meta.info.name, "test.txt");
        assert_eq!(meta.info.piece_length, 16384);
        assert_eq!(meta.info.pieces.len(), 1);
        assert_eq!(meta.info.files.len(), 1);
        assert_eq!(meta.info.files[0].length, 100);
        assert!(!meta.info.private);
        assert_eq!(meta.info.meta_version, 1);
        assert!(meta.comment.is_none());
        assert!(meta.created_by.is_none());
    }

    #[test]
    fn test_parse_multi_file_torrent() {
        // Multi-file torrent with two files
        // Format: d8:announce9:test-host4:infod5:filesld6:lengthi50e4:pathl5:file1eed6:lengthi75e4:pathl3:dir5:file2eee4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee
        let torrent_bytes = b"d8:announce9:test-host4:infod5:filesld6:lengthi50e4:pathl5:file1eed6:lengthi75e4:pathl3:dir5:file2eee4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        
        let meta = TorrentMeta::from_bytes(torrent_bytes).unwrap();
        
        assert_eq!(meta.announce, "test-host");
        assert_eq!(meta.info.name, "test");
        assert_eq!(meta.info.piece_length, 16384);
        assert_eq!(meta.info.files.len(), 2);
        assert_eq!(meta.info.files[0].length, 50);
        assert_eq!(meta.info.files[1].length, 75);
    }

    #[test]
    fn test_infohash_calculation() {
        let torrent_bytes = b"d8:announce9:test-host4:infod6:lengthi100e4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        
        let meta = TorrentMeta::from_bytes(torrent_bytes).unwrap();
        
        // Infohash should be 20 bytes
        assert_eq!(meta.info.raw_infohash.len(), 20);
        // Should not be all zeros
        assert_ne!(meta.info.raw_infohash, [0u8; 20]);
    }

    #[test]
    fn test_private_flag() {
        let torrent_bytes = b"d8:announce9:test-host4:infod6:lengthi100e4:name4:test12:piece lengthi16384e6:pieces20:123456789012345678907:privatei1eee";
        
        let meta = TorrentMeta::from_bytes(torrent_bytes).unwrap();
        assert!(meta.info.private);
    }

    #[test]
    fn test_invalid_pieces_length() {
        // pieces field with invalid length (not multiple of 20)
        let torrent_bytes = b"d8:announce9:test-host4:infod6:lengthi100e4:name4:test12:piece lengthi16384e6:pieces15:123456789012345ee";
        
        let result = TorrentMeta::from_bytes(torrent_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_required_field() {
        // Missing 'name' field
        let torrent_bytes = b"d8:announce9:test-host4:infod6:lengthi100e12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        
        let result = TorrentMeta::from_bytes(torrent_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_fields() {
        // Torrent with comment, created_by, and creation_date
        let torrent_bytes = b"d8:announce9:test-host7:comment12:Test comment10:created by10:test-suite13:creation datei1234567890e4:infod6:lengthi100e4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        
        let meta = TorrentMeta::from_bytes(torrent_bytes).unwrap();
        
        assert_eq!(meta.comment, Some("Test comment".to_string()));
        assert_eq!(meta.created_by, Some("test-suite".to_string()));
        assert_eq!(meta.creation_date, Some(1234567890));
    }

    #[test]
    fn test_padding_file() {
        // Multi-file torrent with padding file (BEP 47)
        // info keys sorted: files, name, piece length, pieces
        // file keys sorted: attr, length, path
        let torrent_bytes = b"d8:announce9:test-host4:infod5:filesld6:lengthi100e4:pathl4:testeed4:attr1:p6:lengthi16384e4:pathl8:.paddingeee4:name4:test12:piece lengthi16384e6:pieces20:12345678901234567890ee";
        
        let meta = TorrentMeta::from_bytes(torrent_bytes).unwrap();
        
        assert_eq!(meta.info.files.len(), 2);
        assert!(!meta.info.files[0].is_padding());
        assert!(meta.info.files[1].is_padding());
        assert_eq!(meta.info.non_padding_files().len(), 1);
    }

    #[test]
    fn test_all_trackers() {
        // Test the all_trackers() method
        let meta = TorrentMeta {
            announce: "http://track1".to_string(),
            announce_list: vec![
                vec!["http://track2".to_string()],
                vec!["http://track3".to_string()],
            ],
            web_seeds: Vec::new(),
            info: InfoDict {
                name: "test".to_string(),
                piece_length: 16384,
                pieces: vec![[0u8; 20]],
                files: Vec::new(),
                private: false,
                raw_infohash: [0u8; 20],
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
        
        let trackers = meta.all_trackers();
        assert!(trackers.contains(&"http://track1".to_string()));
        assert!(trackers.contains(&"http://track2".to_string()));
        assert!(trackers.contains(&"http://track3".to_string()));
        assert_eq!(trackers.len(), 3);
    }
}
