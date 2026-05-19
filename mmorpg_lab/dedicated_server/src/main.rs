use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use bevy_quinnet::server::certificate::CertificateRetrievalMode;
use bevy_quinnet::server::*;

use shared::{ClientMessage, PlayerState, ServerInfo, ServerMessage};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;
use uuid::Uuid;

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
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<u64, PlayerData>, // maps the unique client ID to the player's username (client ID given by Quinnet)
}

fn main() {
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

    App::new()
        .add_plugins(
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                1.0 / 20.0,
            ))), // updates at 20Hz
        )
        .add_plugins(QuinnetServerPlugin::default())
        .insert_resource(config)
        .insert_resource(HeartbeatSocket(socket))
        .insert_resource(HeartbeatTimer(Timer::from_seconds(
            5.0,
            TimerMode::Repeating,
        )))
        .init_resource::<PlayerRegistry>()
        .add_systems(Startup, start_server)
        .add_systems(
            Update,
            (
                handle_connections,
                handle_messages,
                broadcast_aoi,
                send_heartbeat,
            )
                .chain(),
        )
        .run();
}

fn start_server(mut server: ResMut<QuinnetServer>, config: Res<ServerConfig>) {
    let ip_addr: IpAddr = config.ip.parse().expect("Invalid IP");

    let endpoint_config = ServerEndpointConfiguration {
        addr_config: EndpointAddrConfiguration::from_ip(ip_addr, config.port),
        cert_mode: CertificateRetrievalMode::GenerateSelfSigned {
            server_hostname: config.ip.clone(),
        },
        defaultables: Default::default(),
    };

    if let Err(e) = server.start_endpoint(endpoint_config) {
        eprintln!(
            "ERREUR CRITIQUE : Impossible de démarrer l'endpoint QUIC : {:?}",
            e
        );
        return;
    }
    println!(
        "DEDICATED SERVER [{}]: Listening for players on port {}...",
        config.id, config.port
    );
}

// handles all connection and disconnection events, updating the player registry accordingly
// (even non conventional ways to disconnect like killing the client process or network issues)
fn handle_connections(
    mut connection_events: MessageReader<ConnectionEvent>,
    mut connection_lost_events: MessageReader<ConnectionLostEvent>,
    mut registry: ResMut<PlayerRegistry>,
) {
    for event in connection_events.read() {
        println!("Incoming QUIC connection established: ID {}", event.id);
    }

    for event in connection_lost_events.read() {
        println!("Connection lost for ID {}", event.id);
        registry.players.remove(&event.id);
    }
}

fn handle_messages(mut server: ResMut<QuinnetServer>, mut registry: ResMut<PlayerRegistry>) {
    let endpoint = server.endpoint_mut();

    for client_id in endpoint.clients() {
        while let Ok(Some(message)) =
            endpoint.receive_message_from::<ClientMessage, _>(client_id, 0u8)
        {
            match message {
                ClientMessage::Join { username } => {
                    println!("Player '{}' (ID {}) joined the game!", username, client_id);
                    registry.players.insert(
                        client_id,
                        PlayerData {
                            username,
                            position: Vec2::ZERO,
                        },
                    );

                    let _ = endpoint.send_message(
                        client_id,
                        ServerMessage::Welcome {
                            player_id: client_id,
                        },
                    );
                }
                ClientMessage::MoveInput { x, y } => {
                    if let Some(player) = registry.players.get_mut(&client_id) {
                        // Speed = 200 pixels per second, with updates at 20Hz, so we move 10 pixels per tick in the input direction
                        let speed_per_tick = 200.0 * (1.0 / 20.0);
                        let direction = Vec2::new(x, y).normalize_or_zero();

                        player.position += direction * speed_per_tick;

                        // Walls at 400 pixels from the center in all directions
                        player.position = player
                            .position
                            .clamp(Vec2::splat(-400.0), Vec2::splat(400.0));
                    }
                }
            }
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
                    eprintln!("Failed to send heartbeat: {}", e);
                } else {
                    println!(
                        "Heartbeat sent (Players: {}/{})",
                        hb.num_players, hb.capacity
                    );
                }
            }
            Err(e) => eprintln!("Failed to serialize heartbeat: {}", e),
        }
    }
}

// Area of interest (AOI) system : every tick, send each player a custom snapshot of all players that are within 400 pixels of them
fn broadcast_aoi(mut server: ResMut<QuinnetServer>, registry: Res<PlayerRegistry>) {
    let endpoint = server.endpoint_mut();
    let camera_view_distance = 400.0; // AOI radius in pixels

    for (client_id, player_data) in &registry.players {
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

        let _ = endpoint.send_message(
            *client_id,
            ServerMessage::AOISnapshot {
                players: visible_players,
            },
        );
    }
}
