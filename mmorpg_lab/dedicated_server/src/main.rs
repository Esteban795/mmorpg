use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use game_sockets::{GameNetworkEvent, GamePeer, GameStream, protocols::QuicBackend};

use shared::{ClientMessage, PlayerState, ServerInfo, ServerMessage};
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use uuid::Uuid;

use bytes::Bytes;

use tracing::{debug, error, info, warn};

const DEFAULT_DS_PORT: &str = "8001";
const DEFAULT_ORCH_PORT: &str = "8000";
const DEFAULT_ZONE: &str = "zone_A";
const DEFAULT_MAX_PLAYERS: &str = "100";

#[derive(Resource)]
pub struct ServerConfig {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub max_players: u16,
    pub orchestrator_addr: SocketAddr,
}

#[derive(Resource)]
pub struct HeartbeatSocket(UdpSocket);

#[derive(Resource)]
pub struct HeartbeatTimer(Timer);

pub struct PlayerData {
    pub username: String,
    pub position: Vec2,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<Uuid, PlayerData>, // Maps client IDs (Uuid given by game_sockets) to player data
}

#[derive(Resource)]
pub struct NetworkManager {
    pub peer: GamePeer,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // get port and orchestrator address from environment variables, with defaults
    let port: u16 = std::env::var("DS_PORT")
        .unwrap_or_else(|_| DEFAULT_DS_PORT.to_string())
        .parse()
        .expect("Invalid DS_PORT");

    let orchestrator_addr: SocketAddr = std::env::var("ORCH_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{}", DEFAULT_ORCH_PORT))
        .parse()
        .expect("Invalid ORCH_ADDR");

    // get zone from environment variable, defaulting to "zone_A" if not set
    let zone = std::env::var("DS_ZONE").unwrap_or_else(|_| DEFAULT_ZONE.to_string());

    // get max players from environment variable, defaulting to 100 if not set
    let max_players: u16 = std::env::var("DS_MAX_PLAYERS")
        .unwrap_or_else(|_| DEFAULT_MAX_PLAYERS.to_string())
        .parse()
        .expect("Invalid MAX_PLAYERS");

    let config = ServerConfig {
        id: Uuid::new_v4().to_string(),
        ip: "127.0.0.1".to_string(), // Only local IP for this lab - might need to be changed for a env variable in a real deployment
        port,
        zone,
        max_players,
        orchestrator_addr,
    };

    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind heartbeat socket");
    socket.set_nonblocking(true).unwrap();

    // Initialize GameSocket
    let backend = QuicBackend::new();
    let peer = GamePeer::new(backend);

    info!(
        "DEDICATED SERVER [{}]: Listening on port {}...",
        config.ip, config.port
    );
    if let Err(e) = peer.listen(&config.ip, config.port) {
        error!(
            "CRITICAL: Failed to listen on port {}: {:?}",
            config.port, e
        );
        return;
    }

    App::new()
        .add_plugins(
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                1.0 / 20.0,
            ))), // updates at 20Hz
        )
        .insert_resource(config)
        .insert_resource(HeartbeatSocket(socket))
        .insert_resource(HeartbeatTimer(Timer::from_seconds(
            5.0,
            TimerMode::Repeating,
        )))
        .insert_resource(NetworkManager { peer })
        .init_resource::<PlayerRegistry>()
        .add_systems(
            Update,
            (poll_network_events, broadcast_aoi, send_heartbeat).chain(),
        )
        .run();
}

fn poll_network_events(mut net: ResMut<NetworkManager>, mut registry: ResMut<PlayerRegistry>) {
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                info!("[NETWORK] Client connected: {:?}", connection.connection_id);
                registry.players.insert(
                    connection.connection_id,
                    PlayerData {
                        username: "Unknown".to_string(), // pseudo unknown at connection time, will be updated when receiving the join message from the client
                        position: Vec2::ZERO,
                        reliable_stream: None,
                        unreliable_stream: None,
                    },
                );
            }
            GameNetworkEvent::StreamCreated(connection, stream) => {
                if let Some(player) = registry.players.get_mut(&connection.connection_id) {
                    if stream.is_reliable() {
                        player.reliable_stream = Some(stream);
                    } else {
                        player.unreliable_stream = Some(stream);
                    }
                }
            }
            GameNetworkEvent::StreamClosed(connection, stream) => {
                if let Some(player) = registry.players.get_mut(&connection.connection_id) {
                    if stream.is_reliable() {
                        player.reliable_stream = None;
                    } else {
                        player.unreliable_stream = None;
                    }
                }
            }
            GameNetworkEvent::Disconnected(connection) => {
                info!(
                    "[NETWORK] Client disconnected: {}",
                    connection.connection_id
                );
                registry.players.remove(&connection.connection_id);
            }
            GameNetworkEvent::Message {
                connection,
                stream: _,
                data,
            } => {
                match bincode::deserialize::<ClientMessage>(&data) {
                    Ok(message) => match message {
                        ClientMessage::Join { username } => {
                            info!("[GAME] Player {} joined the game !", username);
                            if let Some(player) =
                                registry.players.get_mut(&connection.connection_id)
                            {
                                player.username = username;

                                // Send welcome message with player ID
                                let welcome_msg = ServerMessage::Welcome {
                                    player_id: connection.connection_id,
                                };
                                match bincode::serialize(&welcome_msg) {
                                    Ok(bytes) => {
                                        if let Some(rel_stream) = &player.reliable_stream {
                                            if let Err(e) = net.peer.send(
                                                &connection,
                                                rel_stream,
                                                Bytes::from(bytes),
                                            ) {
                                                error!("Failed to send Welcome message: {:?}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to serialize Welcome message: {:?}", e)
                                    }
                                }
                            }
                        }
                        ClientMessage::MoveInput { x, y } => {
                            debug!(
                                "Input received from {:?} : x={}, y={}",
                                connection.connection_id, x, y
                            );
                            if let Some(player) =
                                registry.players.get_mut(&connection.connection_id)
                            {
                                let speed = 5.0;
                                player.position.x += x * speed;
                                player.position.y += y * speed;
                            }
                        }
                    },
                    Err(e) => warn!(
                        "[SECURITY] Wrong packet received from {:?} : {}",
                        connection.connection_id, e
                    ),
                }
            }
            _ => {}
        }
    }
}

fn send_heartbeat(
    time: Res<Time>,
    mut timer: ResMut<HeartbeatTimer>,
    config: Res<ServerConfig>,
    registry: Res<PlayerRegistry>,
    socket: Res<HeartbeatSocket>,
) {
    // Execute every 5 seconds while being called at 20Hz
    if timer.0.tick(time.delta()).just_finished() {
        let current_players = registry.players.len() as u16;

        // Détermination dynamique du statut
        let status = if current_players >= config.max_players {
            "full".to_string()
        } else {
            "available".to_string()
        };

        let hb = ServerInfo {
            ip: config.ip.clone(),
            port: config.port,
            zone: config.zone.clone(),
            num_players: registry.players.len() as u16,
            capacity: config.max_players,
            status,
            lat: 0.0,
            lon: 0.0,
            cpu_usage: 0.0,
            mem_usage: 0,
        };

        match serde_json::to_string(&hb) {
            Ok(payload) => {
                if let Err(e) = socket
                    .0
                    .send_to(payload.as_bytes(), config.orchestrator_addr)
                {
                    error!("Failed to send heartbeat: {}", e);
                } else {
                    info!(
                        "Heartbeat sent (Players: {}/{})",
                        hb.num_players, hb.capacity
                    );
                }
            }
            Err(e) => error!("Failed to serialize heartbeat: {}", e),
        }
    }
}

// Area of interest (AOI) system : every tick, send each player a custom snapshot of all players that are within 400 pixels of them
fn broadcast_aoi(net: ResMut<NetworkManager>, registry: Res<PlayerRegistry>) {
    let camera_view_distance = 400.0; // AOI radius in pixels

    for (client_id, player_data) in &registry.players {
        let Some(unreliable_stream) = &player_data.unreliable_stream else {
            continue;
        };

        let mut visible_players = Vec::new();
        for (other_id, other_data) in &registry.players {
            if player_data.position.distance(other_data.position) < camera_view_distance {
                visible_players.push(PlayerState {
                    id: *other_id,
                    username: other_data.username.clone(),
                    x: other_data.position.x,
                    y: other_data.position.y,
                });
            }
        }

        let snapshot = ServerMessage::AOISnapshot {
            players: visible_players,
        };
        match bincode::serialize(&snapshot) {
            Ok(bytes) => {
                let conn = game_sockets::GameConnection {
                    connection_id: *client_id,
                };
                if let Err(e) = net.peer.send(&conn, unreliable_stream, Bytes::from(bytes)) {
                    // C'est un datagramme (Unreliable), donc on utilise warn! au lieu de error! si ça rate
                    warn!("Impossible d'envoyer l'AOI à {:?}: {:?}", client_id, e);
                }
            }
            Err(e) => error!("Erreur de sérialisation du Snapshot: {:?}", e),
        }
    }
}
