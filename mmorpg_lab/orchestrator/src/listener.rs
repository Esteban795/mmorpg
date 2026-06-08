use redis::{AsyncCommands, aio::MultiplexedConnection};
use shared::ServerInfo;
use tokio::net::UdpSocket;
use tracing::{error, info, warn};

const BUFFER_SIZE: usize = 2048;
const DEFAULT_ORCH_PORT: &str = "8000";

//Listen to the heartbeats (currently just ServerInfo JSON sent by game servers)
//and store the latest info in RAM using Redis with a short Time-To-Live (TTL) to automatically remove inactive servers.
pub async fn heartbeat_listener(mut redis_conn: MultiplexedConnection) {
    // Get the port from the environment, fallback to default
    let orch_port: u16 = std::env::var("ORCH_PORT")
        .unwrap_or_else(|_| DEFAULT_ORCH_PORT.to_string())
        .parse()
        .expect("Invalid ORCH_PORT configuration");

    //Socket is binded on 0.0.0.0:8000 to receive heartbeats from all servers in the local network on any interface.
    let socket = UdpSocket::bind(("0.0.0.0", orch_port))
        .await
        .expect("Failed to bind UDP");
    let mut buf = [0; BUFFER_SIZE];

    info!("Listener started on UDP {}", orch_port);

    loop {
        if let Ok((len, addr)) = socket.recv_from(&mut buf).await {
            match serde_json::from_slice::<ServerInfo>(&buf[..len]) {
                Ok(info) => {
                    let id = info.id;
                    let redis_key = format!("server:{}", id);

                    if let Ok(json_string) = serde_json::to_string(&info) {
                        let _: Result<(), _> =
                            redis_conn.hset(&redis_key, "data", json_string).await;

                        // Set the TTL to 15 seconds
                        let _: Result<(), _> = redis_conn.expire(&redis_key, 15).await;

                        info!(
                            "Heartbeat updated for {} (Status: {})",
                            redis_key, info.status
                        );
                    }
                }
                Err(e) => {
                    let raw_string = String::from_utf8_lossy(&buf[..len]);
                    error!("Malformed heartbeat from {}. Error: {}", addr, e);
                    warn!("Raw payload: {}", raw_string);
                }
            }
        }
    }
}
