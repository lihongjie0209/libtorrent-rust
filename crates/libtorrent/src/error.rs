use thiserror::Error;

#[derive(Debug, Error)]
pub enum LibtorrentError {
    #[error("network error: {0}")]
    Network(#[from] std::io::Error),
    #[error("handshake error: {0}")]
    Handshake(#[from] libtorrent_proto::HandshakeError),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("bencode decode error: {0}")]
    Bencode(String),
}

impl LibtorrentError {
    pub fn info_hash_mismatch(expected: [u8; 20], received: [u8; 20]) -> Self {
        let msg = format!(
            "info-hash mismatch expected={} received={}",
            hex::encode(expected),
            hex::encode(received)
        );
        Self::Protocol(msg)
    }
}

impl From<bendy::decoding::Error> for LibtorrentError {
    fn from(e: bendy::decoding::Error) -> Self { Self::Bencode(e.to_string()) }
}
