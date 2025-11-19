use libtorrent::{Session, SessionConfig};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

fn init_tracing() {
    if tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .with_ansi(false)
            .finish(),
    )
    .is_err()
    {
        // tracing already initialized
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let session_count = std::env::var("LT_SESSION_COUNT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(4);
    let base_port = std::env::var("LT_BASE_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(50_000);

    info!(session_count, base_port, "starting libtorrent sessions");
    let mut handles = Vec::new();
    for idx in 0..session_count {
        let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), base_port + idx);
        let mut config = SessionConfig::with_random_info_hash(listen_addr);
        config.peer_id_prefix = format!("-LT{idx:04}-");
        let session = Session::new(config);
        handles.push(session.spawn());
    }

    info!("sessions running; press Ctrl+C to stop");
    signal::ctrl_c().await?;
    info!("shutting down");
    for handle in handles {
        handle.abort();
    }
    Ok(())
}
