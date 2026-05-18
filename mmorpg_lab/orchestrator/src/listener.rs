use redis::{AsyncCommands, aio::MultiplexedConnection};
use tokio::net::UdpSocket;
use shared::ServerInfo;

const BUFFER_SIZE: usize = 2048;


//Listen to the heartbeats (currently just ServerInfo JSON sent by game servers) 
//and store the latest info in RAM using Redis with a short Time-To-Live (TTL) to automatically remove inactive servers.
pub async fn heartbeat_listener(mut redis_conn: MultiplexedConnection) {
    //Socket is binded on 0.0.0.0:8000 to receive heartbeats from all servers in the local network on any interface.
    let socket = UdpSocket::bind("0.0.0.0:8000").await.expect("Failed to bind UDP");
    let mut buf = [0; BUFFER_SIZE];

    println!("Listener started on UDP 8000...");

    loop {
        if let Ok((len, addr)) = socket.recv_from(&mut buf).await {
            match serde_json::from_slice::<ServerInfo>(&buf[..len]) {
                Ok(info) => {
                    let generated_id = format!("{}:{}", info.ip, info.port);
                    let redis_key = format!("server:{}", generated_id);
                    
                    if let Ok(json_string) = serde_json::to_string(&info) {

                        let _: Result<(), _> = redis_conn.hset(&redis_key,"data", json_string).await;
                        
                        // Set the TTL to 15 seconds
                        let _: Result<(), _> = redis_conn.expire(&redis_key, 15).await;
                        
                        println!("Heartbeat updated for {} (Status: {})", redis_key, info.status);
                    }
                }
                Err(e) => {
                    let raw_string = String::from_utf8_lossy(&buf[..len]);
                    eprintln!("Malformed heartbeat from {}. Error: {}", addr, e);
                    eprintln!("Raw payload: {}", raw_string);
                }
            }
        }
    }
}