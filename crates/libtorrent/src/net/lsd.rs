use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tokio::time::{interval, Interval};

/// Multicast address/port for Local Service Discovery (BEP 14)
const LSD_MCAST_V4: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
const LSD_PORT: u16 = 6771;

fn build_bt_search(port: u16, infohash_hex: &str) -> Vec<u8> {
    let mut s = String::new();
    s.push_str("BT-SEARCH * HTTP/1.1\r\n");
    s.push_str("Host: 239.192.152.143:6771\r\n");
    s.push_str(&format!("Port: {}\r\n", port));
    s.push_str(&format!("Infohash: {}\r\n", infohash_hex));
    s.push_str("\r\n");
    s.into_bytes()
}

fn parse_bt_search(buf: &[u8]) -> Option<(String, u16)> {
    // Return (infohash_hex, port)
    let s = std::str::from_utf8(buf).ok()?;
    if !s.starts_with("BT-SEARCH ") { return None; }
    let mut infohash: Option<String> = None;
    let mut port: Option<u16> = None;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("Infohash:") {
            let v = rest.trim();
            if v.len() == 40 && v.chars().all(|c| c.is_ascii_hexdigit()) {
                infohash = Some(v.to_lowercase());
            }
        } else if let Some(rest) = line.strip_prefix("Port:") {
            let v = rest.trim();
            if let Ok(p) = v.parse::<u16>() { port = Some(p); }
        }
    }
    match (infohash, port) { (Some(h), Some(p)) => Some((h, p)), _ => None }
}

/// Start an LSD announcer/listener task for a single torrent
/// - Sends periodic BT-SEARCH packets to multicast with our listen port
/// - Listens for BT-SEARCH from others and reports discovered peers through the `on_peer` callback
pub fn start_lsd(
    listen_port: u16,
    infohash: [u8; 20],
    mut on_peer: impl FnMut(SocketAddr) + Send + 'static,
) -> io::Result<JoinHandle<()>> {
    // Create a UDP socket bound to 0.0.0.0:6771 and join LSD multicast
    let std_sock = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, LSD_PORT)))?;
    std_sock.set_nonblocking(true)?;
    // Join on all interfaces (0.0.0.0)
    let _ = std_sock.join_multicast_v4(&LSD_MCAST_V4, &Ipv4Addr::UNSPECIFIED);
    let sock = UdpSocket::from_std(std_sock)?;

    // Build announce payload
    let infohash_hex = hex::encode(infohash);
    let payload = build_bt_search(listen_port, &infohash_hex);
    let mcast_addr = SocketAddr::from((IpAddr::V4(LSD_MCAST_V4), LSD_PORT));

    Ok(tokio::spawn(async move {
        let mut rx_buf = vec![0u8; 1500];
        let mut tick: Interval = interval(Duration::from_secs(5 * 60)); // 5 minutes
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    // Periodic multicast announce
                    let _ = sock.send_to(&payload, mcast_addr).await;
                }
                recv = sock.recv_from(&mut rx_buf) => {
                    if let Ok((n, src)) = recv {
                        if n == 0 { continue; }
                        if let Some((ih, port)) = parse_bt_search(&rx_buf[..n]) {
                            // Only accept if the infohash matches
                            if ih == infohash_hex {
                                // Record discovered peer (src.ip + advertised port)
                                let addr = SocketAddr::new(src.ip(), port);
                                on_peer(addr);
                            }
                        }
                    }
                }
            }
        }
    }))
}
