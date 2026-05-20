use redis::{Client, RedisError, aio::MultiplexedConnection};
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::{error};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    Join { username: String },
    // For the AOI, direction vector (x = -1 for right/y = -1 for down, 0 for no movement, x = +1 for left/y = +1 for up)
    MoveInput { x: f32, y: f32 },
}

impl fmt::Display for ClientMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientMessage::Join { username } => write!(f, "Join {{ username: {} }}", username),
            ClientMessage::MoveInput { x, y } => write!(f, "MoveInput {{ x: {}, y: {} }}", x, y),
        }
    }
}

// Messages sent from Dedicated Server to Client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Welcome { player_id: u64 },
    AOISnapshot { players: Vec<PlayerState> },
}

impl fmt::Display for ServerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerMessage::Welcome { player_id } => write!(f, "Welcome {{ player_id: {} }}", player_id),
            ServerMessage::AOISnapshot { players } => write!(f, "AOISnapshot {{ players: {:?} }}", players),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub num_players: u16,
    pub capacity: u16,
    pub status: String,
    pub lat: f64,
    pub lon: f64,
    pub cpu_usage: f32,
    pub mem_usage: u64,
}

// Multiplexed connection to avoid blocking other users when connecting a user
pub async fn init_redis(redis_url: &str) -> Result<MultiplexedConnection, RedisError> {
    let Ok(client) = Client::open(redis_url) else {
        error!(
            "Error : could not create Redis client with URL '{}'",
            redis_url
        );
        return Err(RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Invalid Redis URL",
        )));
    };

    let Ok(conn) = client.get_multiplexed_async_connection().await else {
        error!("Error : could not connect to Redis at '{}'", redis_url);
        error!(
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

// Response with the IP of the game server to connect to
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginResponse {
    pub player_uuid: String,
    pub server: SimpleServerInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SimpleServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PlayerState {
    pub id: u64,
    pub username: String,
    pub x: f32,
    pub y: f32,
}
