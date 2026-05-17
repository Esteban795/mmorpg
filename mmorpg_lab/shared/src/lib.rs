use serde::{Deserialize, Serialize};
use redis::{Client, RedisError, aio::MultiplexedConnection};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub num_players : u16,
    pub capacity : u16,
    pub lat : f64,
    pub lon : f64,
    pub cpu_usage: f32,
    pub mem_usage: u64,
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
