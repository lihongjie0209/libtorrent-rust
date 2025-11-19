use std::net::SocketAddr;
use tokio::net::UdpSocket;

fn write_bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    let mut buf = itoa::Buffer::new();
    let len = buf.format(bytes.len());
    out.extend_from_slice(len.as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    write_bstr(out, s.as_bytes());
}

fn encode_sample_infohashes(tid: &[u8], node_id: &[u8; 20], target: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.push(b'd');
    // a
    write_str(&mut out, "a");
    out.push(b'd');
    write_str(&mut out, "id");
    write_bstr(&mut out, node_id);
    write_str(&mut out, "target");
    write_bstr(&mut out, target);
    out.push(b'e');
    // q
    write_str(&mut out, "q");
    write_str(&mut out, "sample_infohashes");
    // t
    write_str(&mut out, "t");
    write_bstr(&mut out, tid);
    // y
    write_str(&mut out, "y");
    write_str(&mut out, "q");
    out.push(b'e');
    out
}

#[tokio::main]
async fn main() {
    let tid = [0x12, 0x34];
    let node_id = [0u8; 20];
    let target = [0u8; 20];
    
    let msg = encode_sample_infohashes(&tid, &node_id, &target);
    
    println!("Encoded message length: {}", msg.len());
    println!("Hex: {}", hex::encode(&msg));
    println!("\nAs string (where possible):");
    println!("{}", String::from_utf8_lossy(&msg));
    
    // Verify with bendy
    match bendy::decoding::Decoder::new(&msg).next_object() {
        Ok(obj) => {
            println!("\n✓ Valid bencode!");
            if let Ok(dict) = obj.unwrap().try_into_dictionary() {
                println!("✓ Valid dictionary");
            }
        }
        Err(e) => {
            println!("\n✗ Invalid bencode: {:?}", e);
        }
    }
    
    // Try to send to a real DHT node
    println!("\n\nTrying to send to router.bittorrent.com:6881...");
    let sock = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let target_addr: SocketAddr = "67.215.246.10:6881".parse().unwrap();
    
    match sock.send_to(&msg, target_addr).await {
        Ok(n) => {
            println!("✓ Sent {} bytes", n);
            
            // Try to receive response
            let mut buf = vec![0u8; 1500];
            println!("Waiting for response (5 seconds)...");
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                sock.recv_from(&mut buf)
            ).await {
                Ok(Ok((n, from))) => {
                    println!("✓ Received {} bytes from {}", n, from);
                    println!("Response hex: {}", hex::encode(&buf[..n]));
                    println!("Response string: {}", String::from_utf8_lossy(&buf[..n]));
                }
                Ok(Err(e)) => println!("✗ Receive error: {}", e),
                Err(_) => println!("✗ No response (timeout)"),
            }
        }
        Err(e) => println!("✗ Send error: {}", e),
    }
}
