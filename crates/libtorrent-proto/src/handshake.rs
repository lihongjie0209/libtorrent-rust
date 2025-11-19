use bytes::{BufMut, BytesMut};
use rand::{distributions::Alphanumeric, Rng};
use thiserror::Error;

pub const PROTOCOL_STR: &str = "BitTorrent protocol";
pub const RESERVED_BYTES: usize = 8;
pub const HASH_LEN: usize = 20;
pub const PEER_ID_LEN: usize = 20;
pub const HANDSHAKE_LEN: usize = 49 + PROTOCOL_STR.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
	pub reserved: [u8; RESERVED_BYTES],
	pub info_hash: [u8; HASH_LEN],
	pub peer_id: [u8; PEER_ID_LEN],
}

impl Handshake {
	pub fn new(info_hash: [u8; HASH_LEN], peer_id: [u8; PEER_ID_LEN]) -> Self {
		Self {
			reserved: [0; RESERVED_BYTES],
			info_hash,
			peer_id,
		}
	}

	pub fn with_reserved(
		reserved: [u8; RESERVED_BYTES],
		info_hash: [u8; HASH_LEN],
		peer_id: [u8; PEER_ID_LEN],
	) -> Self {
		Self {
			reserved,
			info_hash,
			peer_id,
		}
	}

	pub fn encode(&self) -> BytesMut {
		let mut buf = BytesMut::with_capacity(HANDSHAKE_LEN);
		buf.put_u8(PROTOCOL_STR.len() as u8);
		buf.extend_from_slice(PROTOCOL_STR.as_bytes());
		buf.extend_from_slice(&self.reserved);
		buf.extend_from_slice(&self.info_hash);
		buf.extend_from_slice(&self.peer_id);
		buf
	}

	pub fn decode(bytes: &[u8]) -> Result<Self, HandshakeError> {
		if bytes.len() < HANDSHAKE_LEN {
			return Err(HandshakeError::Length(bytes.len()));
		}
		let pstrlen = bytes[0] as usize;
		if pstrlen != PROTOCOL_STR.len() {
			return Err(HandshakeError::ProtocolString);
		}
		let protocol = &bytes[1..=pstrlen];
		if protocol != PROTOCOL_STR.as_bytes() {
			return Err(HandshakeError::ProtocolString);
		}
		let mut offset = 1 + pstrlen;
		let mut reserved = [0u8; RESERVED_BYTES];
		reserved.copy_from_slice(&bytes[offset..offset + RESERVED_BYTES]);
		offset += RESERVED_BYTES;
		let mut info_hash = [0u8; HASH_LEN];
		info_hash.copy_from_slice(&bytes[offset..offset + HASH_LEN]);
		offset += HASH_LEN;
		let mut peer_id = [0u8; PEER_ID_LEN];
		peer_id.copy_from_slice(&bytes[offset..offset + PEER_ID_LEN]);
		Ok(Self {
			reserved,
			info_hash,
			peer_id,
		})
	}

	pub fn random_peer_id(client_prefix: &[u8]) -> [u8; PEER_ID_LEN] {
		let mut peer_id = [0u8; PEER_ID_LEN];
		let prefix_len = client_prefix.len().min(PEER_ID_LEN);
		peer_id[..prefix_len].copy_from_slice(&client_prefix[..prefix_len]);
		if prefix_len < PEER_ID_LEN {
			let random_len = PEER_ID_LEN - prefix_len;
			let suffix: String = rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(random_len)
				.map(char::from)
				.collect();
			peer_id[prefix_len..].copy_from_slice(&suffix.as_bytes()[..random_len]);
		}
		peer_id
	}
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HandshakeError {
	#[error("invalid handshake length: {0}")]
	Length(usize),
	#[error("invalid protocol string")]
	ProtocolString,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn encode_decode_roundtrip() {
		let info_hash = [1u8; HASH_LEN];
		let peer_id = [2u8; PEER_ID_LEN];
		let hs = Handshake::new(info_hash, peer_id);
		let buf = hs.encode();
		assert_eq!(buf.len(), HANDSHAKE_LEN);
		let decoded = Handshake::decode(&buf).expect("decode");
		assert_eq!(decoded, hs);
	}

	#[test]
	fn peer_id_prefix() {
		let peer_id = Handshake::random_peer_id(b"-LT0001-");
		assert_eq!(&peer_id[..8], b"-LT0001-");
	}
}
