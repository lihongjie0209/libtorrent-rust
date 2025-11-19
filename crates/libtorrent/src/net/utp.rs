use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::net::transport::Transport;

#[cfg(feature = "utp")]
use librqbit_utp::{UtpSocket, UtpStream};

#[cfg(not(feature = "utp"))]
// Fallback stubs when feature is disabled
pub async fn connect(_addr: SocketAddr) -> io::Result<Transport> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "uTP feature not enabled"))
}

#[cfg(not(feature = "utp"))]
pub async fn listen<F>(_bind: SocketAddr, _on_conn: F) -> io::Result<()>
where
    F: Fn(Transport, SocketAddr) + Send + 'static,
{
    Err(io::Error::new(io::ErrorKind::Unsupported, "uTP feature not enabled"))
}

#[cfg(feature = "utp")]
fn wrap_stream(stream: UtpStream) -> Transport {
    Box::new(stream)
}

#[cfg(feature = "utp")]
fn unspecified_for(remote: &SocketAddr) -> SocketAddr {
    match remote {
        SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
    }
}

#[cfg(feature = "utp")]
pub async fn connect(addr: SocketAddr) -> io::Result<Transport> {
    // Bind outbound uTP to an ephemeral UDP port matching the address family
    let bind_addr = unspecified_for(&addr);
    let socket = UtpSocket::new_udp(bind_addr)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uTP new_udp failed: {e}")))?;
    let stream = socket
        .connect(addr)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uTP connect failed: {e}")))?;
    Ok(wrap_stream(stream))
}

#[cfg(feature = "utp")]
pub async fn listen<F>(bind: SocketAddr, on_conn: F) -> io::Result<()>
where
    F: Fn(Transport, SocketAddr) + Send + 'static,
{
    let socket = UtpSocket::new_udp(bind)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uTP new_udp failed: {e}")))?;

    // Allow invoking the provided callback from the loop
    let on_conn = Arc::new(on_conn);
    loop {
        match socket.accept().await {
            Ok(stream) => {
                let remote = stream.remote_addr();
                let transport = wrap_stream(stream);
                let cb = on_conn.clone();
                // Invoke callback; it is responsible for spawning any tasks
                cb(transport, remote);
            }
            Err(e) => {
                // Bubble up accept error to let caller decide on restart/backoff
                return Err(io::Error::new(io::ErrorKind::Other, format!("uTP accept failed: {e}")));
            }
        }
    }
}
