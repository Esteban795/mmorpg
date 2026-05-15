use shared::ServerInfo;

use redis::{AsyncCommands, Client, RedisError, aio::MultiplexedConnection};

#[derive(Clone)]
pub struct ApiState {
    pub redis_conn: MultiplexedConnection,
}

// Multiplexed connection to avoid blocking other users when connecting a user
pub async fn init_redis(redis_url: &str) -> Result<MultiplexedConnection, RedisError> {
    let Ok(client) = Client::open(redis_url) else {
        eprintln!(
            "Error : could not create Redis client with URL '{}'",
            redis_url
        );
        return Err(RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Invalid Redis URL",
        )));
    };

    let Ok(conn) = client.get_multiplexed_async_connection().await else {
        eprintln!("Error : could not connect to Redis at '{}'", redis_url);
        eprintln!(
            "Make sure Redis is running and accessible at '{}'",
            redis_url
        );
        return Err(RedisError::from((
            redis::ErrorKind::IoError,
            "Could not connect to Redis",
        )));
    };

    Ok(conn)
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
