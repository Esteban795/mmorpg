use shared::ServerInfo;

use redis::{AsyncCommands, Client, RedisError, aio::MultiplexedConnection};

#[derive(Clone)]
pub struct ApiState {
    pub redis_conn: MultiplexedConnection,
}

pub async fn get_servers(state: &ApiState) -> Result<Vec<ServerInfo>, RedisError> {
    let mut conn = state.redis_conn.clone();

    let Ok(servers) = conn.smembers::<_, Vec<String>>("game_servers").await else {
        eprintln!("Error : could not retrieve game servers from Redis");
        return Err(RedisError::from((
            redis::ErrorKind::IoError,
            "Could not retrieve game servers from Redis",
        )));
    };

    let mut server_infos = Vec::new();

    for server_json in servers {
        match serde_json::from_str::<ServerInfo>(&server_json) {
            Ok(info) => {
                if info.num_players < info.capacity {
                    server_infos.push(info);
                }
            }
            Err(e) => {
                eprintln!("Ignoring malformed server entry in Redis : {}", e);
                eprintln!("JSON Error : {}", e);
                eprintln!("Raw data : {}", server_json);
                continue;
            }
        }
    }

    Ok(server_infos)
}
