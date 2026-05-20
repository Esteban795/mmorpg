use shared::ServerInfo;
use tracing::{error, info, warn};
use redis::{AsyncCommands, RedisError, aio::MultiplexedConnection};

#[derive(Clone)]
pub struct ApiState {
    pub redis_conn: MultiplexedConnection,
}

pub async fn get_servers(state: &ApiState) -> Result<Vec<ServerInfo>, RedisError> {
    let mut conn = state.redis_conn.clone();
    let mut server_keys = Vec::new();
    let mut server_infos = Vec::new();

    //Get all the servers from Redis using the new pattern for server ids.
    {
        let Ok(mut scan_iter) = conn.scan_match::<_, String>("server:*").await else {
            error!("Error : could not retrieve game servers from Redis");
            return Err(RedisError::from((
                redis::ErrorKind::IoError,
                "Could not retrieve game servers from Redis",
            )));
        };

    
        while let Some(key) = scan_iter.next_item().await {
            server_keys.push(key);
        }
    }

    

    for key in server_keys {
        if let Ok(server_json) = conn.hget::<_, _, String>(&key, "data").await {
            match serde_json::from_str::<ServerInfo>(&server_json) {
                Ok(info) => {
                    // Check availability and capacity directly from the JSON payload
                    if info.status == "available" && info.num_players < info.capacity {
                        server_infos.push(info);
                    }
                }
                Err(e) => {
                    warn!(json = server_json, "Ignoring malformed JSON: {}", e);
                    continue;
                }
            }
        }
    }
    
    Ok(server_infos)
}
