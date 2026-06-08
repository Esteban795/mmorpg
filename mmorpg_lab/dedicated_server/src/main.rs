mod heartbeat;
mod network;

use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use game_sockets::{GamePeer, protocols::QuicBackend};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use heartbeat::{HeartbeatPlugin, HeartbeatSocket};
use network::{NetworkManager, NetworkPlugin};
use shared::{DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT};
use shared::{DEFAULT_ORCHESTRATOR_ADDR, DEFAULT_ORCHESTRATOR_PORT};

const DEFAULT_DS_PORT: &str = "8001";
const DEFAULT_ZONE: &str = "shard:0";
const DEFAULT_MAX_PLAYERS: &str = "2";
const DEFAULT_SHARD_ID: u32 = 0;

use crate::heartbeat::ShardId;

#[derive(Resource)]
pub struct ServerConfig {
    pub id: u32,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub max_players: u16,
    pub orchestrator_addr: SocketAddr,
    pub broker_addr: SocketAddr,
}

fn main() {
    // -------------- INIT TRACING FOR LOGGING----------------

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    // -------------- SERVER CONFIGURATION ----------------
    // get port and orchestrator address from environment variables, with defaults
    let port: u16 = std::env::var("DS_PORT")
        .unwrap_or_else(|_| DEFAULT_DS_PORT.to_string())
        .parse()
        .expect("Invalid DS_PORT");

    info!("Starting dedicated server on port {}...", port);

    let orchestrator_addr: SocketAddr = std::env::var("ORCH_ADDR")
        .unwrap_or_else(|_| {
            format!(
                "{}:{}",
                DEFAULT_ORCHESTRATOR_ADDR, DEFAULT_ORCHESTRATOR_PORT
            )
        })
        .parse()
        .expect("Invalid ORCH_ADDR");

    info!("Orchestrator address: {}", orchestrator_addr);

    let broker_addr: SocketAddr = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| format!("{}:{}", DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT))
        .parse()
        .expect("Invalid BROKER_ADDR");

    info!("Broker address: {}", broker_addr);

    // get zone from environment variable, defaulting to DEFAULT_ZONE if not set
    let zone = std::env::var("DS_ZONE").unwrap_or_else(|_| DEFAULT_ZONE.to_string());
    info!("Server zone: {}", zone);

    // get max players from environment variable, defaulting to 100 if not set
    let max_players: u16 = std::env::var("DS_MAX_PLAYERS")
        .unwrap_or_else(|_| DEFAULT_MAX_PLAYERS.to_string())
        .parse()
        .expect("Invalid MAX_PLAYERS");

    info!("Max players: {}", max_players);

    let shard_id = std::env::var("DS_SHARD_ID")
        .unwrap_or_else(|_| DEFAULT_SHARD_ID.to_string())
        .parse()
        .expect("Invalid SHARD_ID");

    let config = ServerConfig {
        id: shard_id,
        ip: "127.0.0.1".to_string(), // Only local IP for this lab - might need to be changed for a env variable in a real deployment
        port,
        zone,
        max_players,
        orchestrator_addr,
        broker_addr,
    };

    // BIND UDP SOCKET FOR HEARTBEAT
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind heartbeat socket");
    socket.set_nonblocking(true).unwrap();

    // INIT GAME PEER FOR BROKER CONNECTION
    let backend = QuicBackend::new();
    let peer = GamePeer::new(backend);

    info!(
        "DEDICATED SERVER [{}]: Connecting to Broker at {}...",
        config.zone, config.broker_addr
    );

    // Trying to connect dedicated_server to the broker (like a client)
    if let Err(e) = peer.connect(
        &config.broker_addr.ip().to_string(),
        config.broker_addr.port(),
    ) {
        error!("CRITICAL: Failed to connect to Broker: {:?}", e);
        return;
    }

    App::new()
        .add_plugins(
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                1.0 / 20.0,
            ))),
        )
        .add_plugins(NetworkPlugin)
        .add_plugins(HeartbeatPlugin)
        .insert_resource(config)
        .insert_resource(HeartbeatSocket(socket))
        .insert_resource(NetworkManager {
            peer,
            broker_connection: None,
            reliable_stream: None,
            unreliable_stream: None,
        })
        .insert_resource(ShardId(shard_id))
        .run();
}
