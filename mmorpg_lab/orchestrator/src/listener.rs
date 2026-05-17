use redis::{AsyncCommands, aio::MultiplexedConnection};
use tokio::net::UdpSocket;
use shared::ServerInfo;


//Listen to the heartbeats (currently just ServerInfo JSON sent by game servers) 
//and store the latest info in RAM using Redis with a short Time-To-Live (TTL) to automatically remove inactive servers.
pub async fn heartbeat_listener(mut redis_conn: MultiplexedConnection) {
    //Socket is binded on 0.0.0.0:8000 to receive heartbeats from all servers in the local network on any interface.
    let socket = UdpSocket::bind("0.0.0.0:8000").await.expect("Failed to bind UDP");
    let mut buf = [0; 2048];

    println!("Listener started on UDP 8000...");

    loop {
        if let Ok((len, addr)) = socket.recv_from(&mut buf).await {
            match serde_json::from_slice::<ServerInfo>(&buf[..len]) {

                Ok(server_info) => {
                    
                    //Generate the id by combining the IP and port, and store the server info in Redis with a TTL of 15 seconds (so it will be automatically removed if no heartbeat is received for 15 seconds).
                    let server_id = format!("{}:{}", server_info.ip, server_info.port);
                    let redis_key = format!("server:{}", server_id);
                    
                    if let Ok(json_string) = serde_json::to_string(&server_info) {
                        let _: Result<(), _> = redis_conn.set_ex(&redis_key, json_string, 15).await;
                        println!("Heartbeat updated for {}", redis_key);
                    }
                }
                Err(e) => {
                    // This will print the exact reason it failed, plus the raw text it received
                    let raw_string = String::from_utf8_lossy(&buf[..len]);
                    eprintln!("Malformed heartbeat from {}. Error: {}", addr, e);
                    eprintln!("Raw payload received: {}", raw_string);
                }
            }
        }
    }
}