use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use bevy_quinnet::server::certificate::CertificateRetrievalMode;
use bevy_quinnet::server::*;

use shared::{ClientMessage, ServerInfo, ServerMessage};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;
use uuid::Uuid;

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

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<u64, String>, // maps the unique client ID to the player's username (client ID given by Quinnet)
}

fn main() {
    // get port and orchestrator address from environment variables, with defaults
    let port: u16 = std::env::var("DS_PORT")
        .unwrap_or_else(|_| "8001".to_string())
        .parse()
        .expect("Invalid DS_PORT");

    let orchestrator_addr: SocketAddr = std::env::var("ORCH_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8000".to_string())
        .parse()
        .expect("Invalid ORCH_ADDR");

    let config = ServerConfig {
        id: Uuid::new_v4().to_string(),
        ip: "127.0.0.1".to_string(), // Only local IP for this lab - might need to be changed for a env variable in a real deployment
        port,
        zone: "zone_A".to_string(), // Might need to adjust this name to match the gatekeeper's logic
        max_players: 100,
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
            (handle_connections, handle_messages, send_heartbeat).chain(),
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

    server.start_endpoint(endpoint_config).unwrap();
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
                    registry.players.insert(client_id, username);

                    let _ = endpoint.send_message(
                        client_id,
                        ServerMessage::Welcome {
                            player_id: client_id,
                        },
                    );
                }
                ClientMessage::MoveInput { x: _, y: _ } => {
                    // Pour l'AOI plus tard !
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
        let hb = ServerInfo {
            ip: config.ip.clone(),
            port: config.port,
            zone: config.zone.clone(),
            num_players: registry.players.len() as u16,
            capacity: config.max_players,
            lat: 0.0,
            lon: 0.0,
        };
        // Might be needed to add a "status": "online", "full", "maintenance", etc... if we want to have the gatekeeper filter servers based on that

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
