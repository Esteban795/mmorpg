use redis::{AsyncCommands, aio::MultiplexedConnection};
use shared::ServerInfo;
use tokio::time::{interval, Duration};
use tokio::process::Command;
use std::net::UdpSocket;
use std::collections::HashSet;

//Settings for the spawner. Adjust as needed for testing or production.
const HOT_SERVERS_MIN: usize = 3;

pub async fn maintain_hot_servers(mut redis_conn: MultiplexedConnection) {
    println!("Scaler started. Minimum available servers required: {}", HOT_SERVERS_MIN);
    
    let mut ticker = interval(Duration::from_secs(5));
    let mut port_cursor: u16 = 8001; 

    loop {
        //We get all the servers from Redis and count how many are available (currently just "not full" but later we can use cpu usage healthiness too). 
        //If we have less than HOT_SERVERS_MIN, we spawn new servers until we reach the minimum.
        ticker.tick().await;

        let keys: Vec<String> = redis_conn.keys("server:*").await.unwrap_or_default();
        let mut available_count = 0;
        // Create a HashSet to track ports Redis knows are currently in use
        let mut known_used_ports = HashSet::new();

        if !keys.is_empty() {
            let servers_json: Vec<String> = redis_conn.mget(&keys).await.unwrap_or_default();

            for json_str in servers_json {
                if let Ok(info) = serde_json::from_str::<ServerInfo>(&json_str) {

                    known_used_ports.insert(info.port);

                    let has_room = info.num_players < info.capacity;
                    //let is_healthy = info.cpu_usage < 80.0;

                    if has_room {
                        available_count += 1;
                    }
                }
            }
        }

        println!("Cluster Status: {}/{} available servers.", available_count, HOT_SERVERS_MIN);

        if available_count < HOT_SERVERS_MIN {
            let servers_to_spawn = HOT_SERVERS_MIN - available_count;
            println!("Need {} more servers. Spawning...", servers_to_spawn);
            
            for _ in 0..servers_to_spawn {
                
                // Find the next genuinely free port
                let free_port = find_free_port(&mut port_cursor, &known_used_ports);
                
                // Spawn the server with the guaranteed free port
                spawn_dedicated_server(free_port, "Canada").await;
            }
        }
    }
}


// Helper function to scan for a free port safely. 
// Go over the ports and ping them to see if they are actually free, instead of just assuming the next one is free.
fn find_free_port(cursor: &mut u16, known_used_ports: &HashSet<u16>) -> u16 {
    loop {
        // Prevent it from going over the maximum allowed port limit
        if *cursor > 9000 {
            *cursor = 8001; // Wrap around and start searching from the beginning
        }

        if known_used_ports.contains(cursor) {
            *cursor += 1;
            continue;
        }

        let test_addr = format!("0.0.0.0:{}", *cursor);
        
        // Try to bind. If it works, the port is free!
        if UdpSocket::bind(&test_addr).is_ok() {
            let found_port = *cursor;
            *cursor += 1; 
            return found_port;
        }

        // If we reach here, the bind failed (port is used). Increment and try the next one.
        *cursor += 1;
    }
}


// Wait to see how dedicated game servers work exactly to actually spawn them.
async fn spawn_dedicated_server(port: u16, zone: &str) {
     println!("Booting Bevy server on port {}", port);
}

//     let _ = Command::new("./target/release/dedicated_server")
//         .arg("--port")
//         .arg(port.to_string())
//         .arg("--zone")
//         .arg(zone)
//         .spawn(); 
// }