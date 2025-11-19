use libtorrent::{Session, SessionConfig, TorrentMeta};
use std::path::PathBuf;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 libtorrent-rust demo");
    println!("======================\n");

    // Create a session
    let mut config = SessionConfig::with_random_info_hash("127.0.0.1:6881".parse().unwrap());
    config.peer_id_prefix = "-LT0001-".to_string();
    config.max_pending_peers = 100;
    config.download_dir = PathBuf::from("downloads");
    // Optional: caps for demo
    config.max_outbound_peers = 80;
    config.max_upload_slots = 8;
    config.upload_rate_limit = 0; // unlimited

    let session = Session::new(config);
    println!("✅ Session created with peer_id: {}", hex::encode(session.peer_id()));

    // Example: Parse a .torrent file (if provided)
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        let torrent_path = PathBuf::from(&args[1]);
        
        match std::fs::read(&torrent_path) {
            Ok(bytes) => {
                match TorrentMeta::from_bytes(&bytes) {
                    Ok(meta) => {
                        println!("\n📄 Torrent metadata:");
                        println!("  Name: {}", meta.info.name);
                        println!("  Announce: {}", meta.announce);
                        println!("  Piece length: {} bytes", meta.info.piece_length);
                        println!("  Number of pieces: {}", meta.info.num_pieces());
                        println!("  Total size: {} bytes", meta.info.total_size());
                        println!("  InfoHash: {}", hex::encode(meta.info.raw_infohash));
                        println!("  Files:");
                        for (i, file) in meta.info.files.iter().enumerate() {
                            println!("    [{}] {} ({} bytes)", i, file.path.display(), file.length);
                        }

                        // Add torrent to session
                        let handle = session.add_torrent(meta).await;
                        let torrent = handle.read().await;
                        println!("\n✅ Torrent added to session");
                        println!("  Progress: {:.2}%", torrent.progress() * 100.0);
                        println!("  State: {:?}", torrent.state());
                        
                        drop(torrent); // Release lock
                        
                        println!("\n📊 Session statistics:");
                        println!("  Active torrents: {}", session.num_torrents().await);
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to parse torrent file: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to read file: {}", e);
            }
        }
    } else {
        println!("\n💡 Usage: cargo run --example demo <path-to-torrent-file>");
        println!("   Or just run without arguments to see session features\n");
    }

    // Demonstrate piece picker
    println!("\n🧩 Piece Picker Demo:");
    use libtorrent::piece_picker::PiecePicker;
    
    let mut picker = PiecePicker::new(10);
    println!("  Created picker with 10 pieces");
    
    // Simulate peer with pieces [0, 1, 2, 5, 8]
    let peer_bitfield = vec![
        true, true, true, false, false,
        true, false, false, true, false
    ];
    picker.add_peer(&peer_bitfield);
    
    // Pick next piece (should use rarest-first)
    if let Some(piece) = picker.pick_piece(&peer_bitfield) {
        println!("  Selected piece {} for download", piece);
        picker.piece_started(piece);
    }
    
    // Mark a piece as complete
    picker.piece_completed(0);
    println!("  Piece 0 completed");
    println!("  Progress: {:.0}%", picker.progress() * 100.0);
    println!("  Completed: {}/{}", picker.num_completed(), picker.num_pieces());

    // Protocol messages demo
    println!("\n📨 Protocol Messages Demo:");
    use libtorrent_proto::Message;
    
    let messages = vec![
        Message::Choke,
        Message::Unchoke,
        Message::Interested,
        Message::Have(42),
        Message::Request { index: 5, begin: 0, length: 16384 },
    ];
    
    for msg in &messages {
        let encoded = msg.encode();
        println!("  {:?} -> {} bytes", msg, encoded.len());
    }

    println!("\n✨ Demo completed!");
    println!("\n📚 Implemented features:");
    println!("  ✅ Handshake protocol");
    println!("  ✅ Protocol messages (Choke, Unchoke, Have, Bitfield, Request, Piece, etc.)");
    println!("  ✅ Torrent metadata parsing (.torrent files)");
    println!("  ✅ InfoHash calculation (SHA-1)");
    println!("  ✅ Piece picker with rarest-first strategy");
    println!("  ✅ Torrent handle with state management");
    println!("  ✅ Session with multi-torrent support");
    println!("  ✅ Peer state machine (choke/unchoke, interested)");
    println!("\n🚧 Next steps:");
    println!("  - Tracker client (HTTP/UDP)");
    println!("  - DHT implementation");
    println!("  - Disk I/O with async file operations");
    println!("  - UPnP/NAT-PMP support");
    println!("  - Alert system for notifications");

    Ok(())
}
