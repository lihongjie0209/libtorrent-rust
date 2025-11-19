use bendy::decoding::{Decoder, Error};
use sha1::{Digest, Sha1};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentMeta {
    pub announce: String,
    pub announce_list: Vec<Vec<String>>, // tracker tiers
    pub web_seeds: Vec<String>,
    pub info: InfoDict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfoDict {
    pub name: String,
    pub piece_length: u32,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileEntry>,
    pub private: bool,
    pub raw_infohash: [u8; 20],
    pub raw_info_bencode: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub length: u64,
    pub path: PathBuf,
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
                    // Basic support: single string (BEP 19). List form is optional and skipped here.
                    if let Ok(bytes) = value.try_into_bytes() {
                        web_seeds.push(String::from_utf8_lossy(bytes).into_owned());
                    }
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
                b"info" => {
                    let dict = value.try_into_dictionary()?;
                    let raw = dict.into_raw()?;
                    info_bytes = Some(raw.to_vec());
                }
                _ => {}
            }
        }

        let announce = announce.ok_or_else(|| Error::missing_field("announce"))?;
        let info = info_bytes.ok_or_else(|| Error::missing_field("info"))?;
        let info = InfoDict::from_info_bytes(&info)?;
        // Deduplicate web seeds
        web_seeds.sort(); web_seeds.dedup();
        Ok(Self { announce, announce_list, web_seeds, info })
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
        let mut private = false;

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
                _ => {}
            }
        }

        if files.is_empty() {
            let len = single_file_length.ok_or_else(|| Error::missing_field("length"))?;
            let path = PathBuf::from(name.clone().unwrap_or_default());
            files.push(FileEntry { length: len, path });
        }

        let name = name.ok_or_else(|| Error::missing_field("name"))?;
        let piece_length = piece_length.ok_or_else(|| Error::missing_field("piece length"))?;
        let pieces = pieces.ok_or_else(|| Error::missing_field("pieces"))?;
        let mut hasher = Sha1::new();
        hasher.update(bytes);
        let mut raw = [0u8; 20];
        raw.copy_from_slice(&hasher.finalize());
        Ok(Self {
            name,
            piece_length,
            pieces,
            files,
            private,
            raw_infohash: raw,
            raw_info_bencode: bytes.to_vec(),
        })
    }
}

fn decode_files(list_obj: bendy::decoding::Object) -> Result<Vec<FileEntry>, Error> {
    let mut list = list_obj.try_into_list()?;
    let mut files = Vec::new();
    while let Some(entry) = list.next_object()? {
        let mut dict = entry.try_into_dictionary()?;
        let mut length = None;
        let mut path_parts = Vec::new();
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
                _ => {}
            }
        }
        files.push(FileEntry {
            length: length.ok_or_else(|| Error::missing_field("file length"))?,
            path: path_parts.into_iter().collect(),
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
}
