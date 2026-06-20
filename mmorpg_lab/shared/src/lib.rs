pub mod broker_protocol;
pub mod orchestrator_protocol;
pub mod rect;

use redis::{Client, RedisError, aio::MultiplexedConnection};
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::error;

pub const DEFAULT_REDIS_IP: &str = "redis://127.0.0.1";
pub const DEFAULT_GATEKEEPER_ADDR_PORT: &str = "127.0.0.1:8080";
pub const DEFAULT_BROKER_IP: &str = "127.0.0.1";
pub const DEFAULT_BROKER_PORT: u16 = 10001;
pub const DEFAULT_ORCHESTRATOR_ADDR: &str = "127.0.0.1";
pub const DEFAULT_ORCHESTRATOR_QUIC_PORT: u16 = 10002;
pub const DEFAULT_ORCH_HEARTBEAT_PORT: u16 = 8000;

// Game Map Boundaries
pub const MAP_BOUND_MIN: f32 = -2000.0;
pub const MAP_BOUND_MAX: f32 = 2000.0;
pub const MAP_SIZE: f32 = 4000.0;
pub const SPAWN_X: f32 = 32.0;
pub const SPAWN_Y: f32 = 40.0;

pub const BASE_PLAYER_RADIUS: f32 = 15.0; // Base radius for a player with score 0, can be adjusted as needed. SAME in SERVER, need to be consistent

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    //Use a custom fixed-size username to have enough room in the payload.
    Join { username: [u8; 12] },
    // For the AOI, direction vector (x = -1 for right/y = -1 for down, 0 for no movement, x = +1 for left/y = +1 for up)
    MoveInput { x: f32, y: f32 },
    //4 bytes, will have to pad it to 16 bytes to fit in the input struct of the broker protocol.
    Disconnect,
}

impl fmt::Display for ClientMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientMessage::Join { username } => {
                let name = String::from_utf8_lossy(username);
                write!(f, "Join {{ username: {} }}", name)
            }
            ClientMessage::MoveInput { x, y } => write!(f, "MoveInput {{ x: {}, y: {} }}", x, y),
            ClientMessage::Disconnect => write!(f, "Disconnect"),
        }
    }
}

// Messages sent from Dedicated Server to Client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Welcome { player_id: u32 },
    AOISnapshot { players: Vec<PlayerState> },
    FoodSync(Vec<FoodData>), // New food data or resync
    FoodEaten(Vec<u32>),
    GameOver,
}

impl fmt::Display for ServerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerMessage::Welcome { player_id } => {
                write!(f, "Welcome {{ player_id: {} }}", player_id)
            }
            ServerMessage::AOISnapshot { players } => {
                write!(f, "AOISnapshot {{ players: {:?} }}", players)
            }
            ServerMessage::FoodSync(food) => {
                write!(f, "FoodSync {{ food: {:?} }}", food)
            }
            ServerMessage::FoodEaten(food_ids) => {
                write!(f, "FoodEaten {{ food_ids: {:?} }}", food_ids)
            }
            ServerMessage::GameOver => write!(f, "GameOver"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub id: u32,
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
    pub id: u32,
    pub username: String,
    pub x: f32,
    pub y: f32,
    pub score: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FoodData {
    pub id: u32,
    pub x: f32,
    pub y: f32,
    pub color_index: u8,
}
