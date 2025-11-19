use bytes::{BufMut, Bytes, BytesMut};
use std::io;

/// Standard block size (16 KB)
pub const BLOCK_SIZE: u32 = 16384;

/// BitTorrent peer wire protocol messages
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Keep-alive message (no payload)
    KeepAlive,
    /// Choke the peer
    Choke,
    /// Unchoke the peer
    Unchoke,
    /// Declare interest in peer's pieces
    Interested,
    /// Declare lack of interest
    NotInterested,
    /// Announce that peer has a piece (piece_index)
    Have(u32),
    /// Send complete bitfield of pieces peer has
    Bitfield(Bytes),
    /// Request a block: (piece_index, block_offset, block_length)
    Request { index: u32, begin: u32, length: u32 },
    /// Send a block: (piece_index, block_offset, data)
    Piece { index: u32, begin: u32, data: Bytes },
    /// Cancel a request: (piece_index, block_offset, block_length)
    Cancel { index: u32, begin: u32, length: u32 },
    /// DHT port announcement
    Port(u16),
    /// Extended message (BEP 10): (ext_id, payload)
    Extended(u8, Bytes),
}

impl Message {
    /// Encode message to bytes
    pub fn encode(&self) -> BytesMut {
        match self {
            Message::KeepAlive => {
                let mut buf = BytesMut::with_capacity(4);
                buf.put_u32(0); // length = 0
                buf
            }
            Message::Choke => encode_simple(0),
            Message::Unchoke => encode_simple(1),
            Message::Interested => encode_simple(2),
            Message::NotInterested => encode_simple(3),
            Message::Have(piece_index) => {
                let mut buf = BytesMut::with_capacity(9);
                buf.put_u32(5); // length
                buf.put_u8(4); // id
                buf.put_u32(*piece_index);
                buf
            }
            Message::Bitfield(data) => {
                let len = 1 + data.len();
                let mut buf = BytesMut::with_capacity(4 + len);
                buf.put_u32(len as u32);
                buf.put_u8(5); // id
                buf.put_slice(data);
                buf
            }
            Message::Request { index, begin, length } => {
                let mut buf = BytesMut::with_capacity(17);
                buf.put_u32(13); // length
                buf.put_u8(6); // id
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
                buf
            }
            Message::Piece { index, begin, data } => {
                let len = 9 + data.len();
                let mut buf = BytesMut::with_capacity(4 + len);
                buf.put_u32(len as u32);
                buf.put_u8(7); // id
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_slice(data);
                buf
            }
            Message::Cancel { index, begin, length } => {
                let mut buf = BytesMut::with_capacity(17);
                buf.put_u32(13); // length
                buf.put_u8(8); // id
                buf.put_u32(*index);
                buf.put_u32(*begin);
                buf.put_u32(*length);
                buf
            }
            Message::Port(port) => {
                let mut buf = BytesMut::with_capacity(7);
                buf.put_u32(3); // length
                buf.put_u8(9); // id
                buf.put_u16(*port);
                buf
            }
            Message::Extended(ext_id, payload) => {
                let len = 2 + payload.len();
                let mut buf = BytesMut::with_capacity(4 + len);
                buf.put_u32(len as u32);
                buf.put_u8(20); // extended id
                buf.put_u8(*ext_id);
                buf.put_slice(payload);
                buf
            }
        }
    }

    /// Decode message from bytes
    /// Returns (message, bytes_consumed) or None if more data needed
    pub fn decode(buf: &[u8]) -> Result<Option<(Self, usize)>, io::Error> {
        if buf.len() < 4 {
            return Ok(None); // Need at least length prefix
        }

        let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total_len = 4 + length;

        if buf.len() < total_len {
            return Ok(None); // Need more data
        }

        if length == 0 {
            return Ok(Some((Message::KeepAlive, 4)));
        }

        let msg_id = buf[4];
        let payload = &buf[5..total_len];

        let message = match msg_id {
            0 => Message::Choke,
            1 => Message::Unchoke,
            2 => Message::Interested,
            3 => Message::NotInterested,
            4 => {
                if payload.len() < 4 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Have message too short"));
                }
                let piece_index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                Message::Have(piece_index)
            }
            5 => {
                Message::Bitfield(Bytes::copy_from_slice(payload))
            }
            6 => {
                if payload.len() < 12 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Request message too short"));
                }
                let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let length = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
                Message::Request { index, begin, length }
            }
            7 => {
                if payload.len() < 8 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Piece message too short"));
                }
                let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let data = Bytes::copy_from_slice(&payload[8..]);
                Message::Piece { index, begin, data }
            }
            8 => {
                if payload.len() < 12 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Cancel message too short"));
                }
                let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
                let length = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
                Message::Cancel { index, begin, length }
            }
            9 => {
                if payload.len() < 2 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Port message too short"));
                }
                let port = u16::from_be_bytes([payload[0], payload[1]]);
                Message::Port(port)
            }
            20 => {
                if payload.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Extended message too short"));
                }
                let ext_id = payload[0];
                let rest = &payload[1..];
                Message::Extended(ext_id, Bytes::copy_from_slice(rest))
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown message id: {}", msg_id),
                ));
            }
        };

        Ok(Some((message, total_len)))
    }
}

fn encode_simple(id: u8) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u32(1); // length
    buf.put_u8(id);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keepalive_roundtrip() {
        let msg = Message::KeepAlive;
        let encoded = msg.encode();
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_choke_roundtrip() {
        let msg = Message::Choke;
        let encoded = msg.encode();
        assert_eq!(encoded.len(), 5);
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_have_roundtrip() {
        let msg = Message::Have(42);
        let encoded = msg.encode();
        assert_eq!(encoded.len(), 9);
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_bitfield_roundtrip() {
        let bitfield = vec![0xFF, 0xAA, 0x55];
        let msg = Message::Bitfield(Bytes::from(bitfield.clone()));
        let encoded = msg.encode();
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 4 + 1 + 3);
    }

    #[test]
    fn test_request_roundtrip() {
        let msg = Message::Request {
            index: 10,
            begin: 16384,
            length: 16384,
        };
        let encoded = msg.encode();
        assert_eq!(encoded.len(), 17);
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 17);
    }

    #[test]
    fn test_piece_roundtrip() {
        let data = vec![1u8; 100];
        let msg = Message::Piece {
            index: 5,
            begin: 0,
            data: Bytes::from(data.clone()),
        };
        let encoded = msg.encode();
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 4 + 9 + 100);
    }

    #[test]
    fn test_cancel_roundtrip() {
        let msg = Message::Cancel {
            index: 7,
            begin: 32768,
            length: 16384,
        };
        let encoded = msg.encode();
        let (decoded, consumed) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, 17);
    }

    #[test]
    fn test_partial_message() {
        let msg = Message::Have(123);
        let encoded = msg.encode();
        
        // Only send first 3 bytes
        let result = Message::decode(&encoded[..3]).unwrap();
        assert!(result.is_none()); // Should need more data
        
        // Send complete message
        let (decoded, _) = Message::decode(&encoded).unwrap().unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_all_simple_messages() {
        let messages = vec![
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
        ];

        for msg in messages {
            let encoded = msg.encode();
            let (decoded, _) = Message::decode(&encoded).unwrap().unwrap();
            assert_eq!(decoded, msg);
        }
    }
}
