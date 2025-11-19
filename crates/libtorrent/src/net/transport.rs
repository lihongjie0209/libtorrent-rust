use tokio::io::{AsyncRead, AsyncWrite};

// A unified trait for peer transport streams (TCP or uTP)
pub trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> TransportStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub type Transport = Box<dyn TransportStream>;

pub fn from_tcp(stream: tokio::net::TcpStream) -> Transport {
    Box::new(stream)
}
