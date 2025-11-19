use sha1::{Digest, Sha1};
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::metainfo::InfoDict;

#[derive(Debug, Clone)]
struct DiskFile {
    path: PathBuf,
    length: u64,
    start: u64,
    end: u64,
}

#[derive(Debug)]
pub struct DiskManager {
    base: PathBuf,
    files: Vec<DiskFile>,
    piece_length: u32,
    pieces: Vec<[u8; 20]>,
    total_size: u64,
    is_single_file: bool,
    name: String,
}

impl DiskManager {
    pub async fn new(base: impl AsRef<Path>, info: &InfoDict) -> std::io::Result<Self> {
        let base = base.as_ref().to_path_buf();
        let mut files = Vec::new();
        let mut offset = 0u64;
        let is_single_file = info.is_single_file();
        for f in &info.files {
            let full_path = if is_single_file {
                base.join(&f.path)
            } else {
                base.join(&info.name).join(&f.path)
            };
            let start = offset;
            let end = start + f.length;
            files.push(DiskFile { path: full_path, length: f.length, start, end });
            offset = end;
        }

        // Ensure directories exist and pre-allocate files (sparse OK)
        for df in &files {
            if let Some(parent) = df.path.parent() {
                fs::create_dir_all(parent).await?;
            }
            let file = OpenOptions::new().create(true).write(true).read(true).open(&df.path).await?;
            file.set_len(df.length).await?;
        }

        Ok(Self {
            base,
            files,
            piece_length: info.piece_length,
            pieces: info.pieces.clone(),
            total_size: info.total_size(),
            is_single_file,
            name: info.name.clone(),
        })
    }

    fn piece_bounds(&self, piece_index: u32) -> (u64, u64) {
        let start = piece_index as u64 * self.piece_length as u64;
        let end = (start + self.piece_length as u64).min(self.total_size);
        (start, end)
    }

    fn map_offset_to_file(&self, abs_offset: u64) -> Option<usize> {
        self.files.iter().position(|f| abs_offset < f.end)
    }

    pub async fn write_block(&self, piece_index: u32, offset_in_piece: u32, data: &[u8]) -> std::io::Result<()> {
        let (piece_start, piece_end) = self.piece_bounds(piece_index);
        let mut abs_off = piece_start + offset_in_piece as u64;
        let mut remaining = data.len();
        let mut cursor = 0usize;
        if abs_off + remaining as u64 > piece_end {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "write exceeds piece bounds"));
        }

        while remaining > 0 {
            let idx = self.map_offset_to_file(abs_off).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "offset beyond files"))?;
            let df = &self.files[idx];
            let in_file_off = abs_off - df.start;
            let can_write = (df.length - in_file_off) as usize;
            let to_write = remaining.min(can_write);
            let mut file = OpenOptions::new().write(true).read(true).open(&df.path).await?;
            file.seek(std::io::SeekFrom::Start(in_file_off)).await?;
            file.write_all(&data[cursor..cursor + to_write]).await?;
            abs_off += to_write as u64;
            cursor += to_write;
            remaining -= to_write;
        }
        Ok(())
    }

    pub async fn read_block(&self, piece_index: u32, offset_in_piece: u32, len: usize) -> std::io::Result<Vec<u8>> {
        let (piece_start, piece_end) = self.piece_bounds(piece_index);
        let mut abs_off = piece_start + offset_in_piece as u64;
        if abs_off + len as u64 > piece_end {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "read exceeds piece bounds"));
        }
        let mut out = Vec::with_capacity(len);
        let mut remaining = len;
        while remaining > 0 {
            let idx = self.map_offset_to_file(abs_off).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "offset beyond files"))?;
            let df = &self.files[idx];
            let in_file_off = abs_off - df.start;
            let can_read = (df.length - in_file_off) as usize;
            let to_read = remaining.min(can_read);
            let mut file = OpenOptions::new().read(true).open(&df.path).await?;
            file.seek(std::io::SeekFrom::Start(in_file_off)).await?;
            let mut buf = vec![0u8; to_read];
            file.read_exact(&mut buf).await?;
            out.extend_from_slice(&buf);
            abs_off += to_read as u64;
            remaining -= to_read;
        }
        Ok(out)
    }

    pub async fn verify_piece(&self, piece_index: u32) -> std::io::Result<bool> {
        let (start, end) = self.piece_bounds(piece_index);
        let len = (end - start) as usize;
        let data = self.read_block(piece_index, 0, len).await?;
        let mut hasher = Sha1::new();
        hasher.update(&data);
        let digest = hasher.finalize();
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&digest);
        Ok(self.pieces.get(piece_index as usize) == Some(&arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;
    use tempfile::tempdir;

    fn make_info_single(name: &str, total: u64, piece_len: u32, data: &[u8]) -> InfoDict {
        let mut pieces = Vec::new();
        let mut off = 0usize;
        while off < data.len() {
            let end = (off as u64 + piece_len as u64).min(total) as usize;
            let mut hasher = Sha1::new();
            hasher.update(&data[off..end]);
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&hasher.finalize());
            pieces.push(arr);
            off = end;
        }
        InfoDict {
            name: name.to_string(),
            piece_length: piece_len,
            pieces,
            files: vec![crate::metainfo::FileEntry { length: total, path: PathBuf::from(name) }],
            private: false,
            raw_infohash: [0u8; 20],
            raw_info_bencode: Vec::new(),
        }
    }

    #[tokio::test]
    async fn single_file_write_read_verify() {
        let dir = tempdir().unwrap();
        let piece_len = 64u32;
        let total = 100u64;
        let mut data = vec![0u8; total as usize];
        rand::thread_rng().fill_bytes(&mut data);
        let info = make_info_single("file.bin", total, piece_len, &data);
        let dm = DiskManager::new(dir.path(), &info).await.unwrap();

        // Write piece 0 and 1
        dm.write_block(0, 0, &data[0..piece_len as usize]).await.unwrap();
        dm.write_block(1, 0, &data[piece_len as usize..]).await.unwrap();

        assert!(dm.verify_piece(0).await.unwrap());
        assert!(dm.verify_piece(1).await.unwrap());

        // Read back and compare
        let r0 = dm.read_block(0, 0, piece_len as usize).await.unwrap();
        assert_eq!(&r0[..], &data[0..piece_len as usize]);
        let r1 = dm.read_block(1, 0, (total - piece_len as u64) as usize).await.unwrap();
        assert_eq!(&r1[..], &data[piece_len as usize..]);
    }

    #[tokio::test]
    async fn multi_file_cross_boundary() {
        // Two files: 50 + 75; piece_len 64, so piece 0 spans file1+file2
        let dir = tempdir().unwrap();
        let piece_len = 64u32;
        let f1 = 50u64; let f2 = 75u64; let total = f1 + f2;
        let mut data = vec![0u8; total as usize];
        rand::thread_rng().fill_bytes(&mut data);
        // Build InfoDict for multi-file
        let mut pieces = Vec::new();
        let mut off = 0usize;
        while off < data.len() {
            let end = (off as u64 + piece_len as u64).min(total) as usize;
            let mut hasher = Sha1::new();
            hasher.update(&data[off..end]);
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&hasher.finalize());
            pieces.push(arr);
            off = end;
        }
        let info = InfoDict {
            name: "my_torrent".to_string(),
            piece_length: piece_len,
            pieces,
            files: vec![
                crate::metainfo::FileEntry { length: f1, path: PathBuf::from("file1.bin") },
                crate::metainfo::FileEntry { length: f2, path: PathBuf::from("dir").join("file2.bin") },
            ],
            private: false,
            raw_infohash: [0u8; 20],
            raw_info_bencode: Vec::new(),
        };

        let dm = DiskManager::new(dir.path(), &info).await.unwrap();
        // Write piece 0 (spans both files) and piece 1
        dm.write_block(0, 0, &data[0..piece_len as usize]).await.unwrap();
        dm.write_block(1, 0, &data[piece_len as usize..]).await.unwrap();
        assert!(dm.verify_piece(0).await.unwrap());
        assert!(dm.verify_piece(1).await.unwrap());

        // Read first 10 bytes of piece 0 (from file1)
        let r = dm.read_block(0, 0, 10).await.unwrap();
        assert_eq!(&r[..], &data[0..10]);
        // Read last 10 bytes of piece 0 (from file2)
        let r = dm.read_block(0, piece_len - 10, 10).await.unwrap();
        assert_eq!(&r[..], &data[(piece_len - 10) as usize..piece_len as usize]);
    }
}
