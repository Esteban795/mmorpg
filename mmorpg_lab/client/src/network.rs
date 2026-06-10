use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use shared::broker_protocol::BrokerMessage;
use shared::{ClientMessage, ServerMessage};

use crate::loginmenu::ConnectionSettings;
use crate::state::AppState;

use tracing::{error, info};

#[derive(Resource, Default)]
pub struct ClientNetworkManager {
    pub peer: Option<GamePeer>,
    pub server_connection: Option<GameConnection>,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ClientNetworkManager>()
            .add_systems(OnEnter(AppState::InGame), start_connection)
            .add_systems(Update, handle_network.run_if(in_state(AppState::InGame)));
    }
}

// Creates game_socket connection to the given server (in fact to the broker)
fn start_connection(
    mut net: ResMut<ClientNetworkManager>,
    mut settings: ResMut<ConnectionSettings>,
    mut next_state: ResMut<NextState<AppState>>,
) {
    if let Some((ip, port)) = &settings.server_target {
        info!("[CLIENT] : Starting connection to {}:{}...", ip, port);

        let backend = QuicBackend::new();
        let peer = GamePeer::new(backend);

        if let Err(e) = peer.connect(ip, *port) {
            error!("[CLIENT] ERROR: Failed to connect: {:?}", e);
            settings.error_message = Some("Failed to connect to server".to_string());
            next_state.set(AppState::LoginMenu);
            return;
        }

        net.peer = Some(peer);
    }
}

// This system continuously polls the network for events from the server. It handles connection events, stream creation, incoming messages, and disconnections.
fn handle_network(
    mut net: ResMut<ClientNetworkManager>,
    settings: Res<ConnectionSettings>,
    mut commands: Commands,
    mut game_state: ResMut<crate::game::GameState>,
    mut transforms: Query<&mut Transform>,
    time: Res<Time>,
) {
    let current_time = time.elapsed_secs_f64();

    loop {
        let event_result = if let Some(peer) = &mut net.peer {
            peer.poll()
        } else {
            return; // If peer isn't intialized yet, skip the system
        };

        match event_result {
            Ok(Some(event)) => match event {
                GameNetworkEvent::Connected(conn) => {
                    info!("[CLIENT]: Connected to server !");
                    net.server_connection = Some(conn);
                    if let Some(peer) = &mut net.peer {
                        peer.create_stream(conn, GameStreamReliability::Reliable)
                            .unwrap();
                        peer.create_stream(conn, GameStreamReliability::Unreliable)
                            .unwrap();
                    }
                }
                GameNetworkEvent::StreamCreated(conn, stream) => {
                    handle_stream_created(conn, stream, &mut net, &settings);
                }
                GameNetworkEvent::Message { data, .. } => {
                    handle_server_message(
                        &data,
                        &mut commands,
                        &mut game_state,
                        &mut transforms,
                        current_time,
                    );
                }
                GameNetworkEvent::Disconnected(_) => {
                    info!("[CLIENT] : Disconnected from the server.");
                }
                _ => {}
            },
            Ok(None) | Err(_) => break, // No more event to process, exit the loop and wait for the next frame
        }
    }

    // Despawn of entities for players we haven't seen for a while:
    // If an entity in the HashMap hasn't been seen for more than 0.8 seconds (any AOI received), it is despawned and removed from the HashMap
    game_state
        .spawned_players
        .retain(|_id, (entity, last_seen)| {
            if current_time - *last_seen > 0.8 {
                commands.entity(*entity).despawn();
                false
            } else {
                true
            }
        });
}

// This function handles the creation of new streams from the broker. It distinguishes between reliable and unreliable streams,
fn handle_stream_created(
    conn: GameConnection,
    stream: GameStream,
    net: &mut ClientNetworkManager,
    settings: &ConnectionSettings,
) {
    if stream.is_reliable() {
        net.reliable_stream = Some(stream.clone());
        info!(
            "[CLIENT]: Sending join message with username: {}",
            settings.username
        );

        let mut username_bytes = [0u8; 12];
        let name_bytes = settings.username.as_bytes();
        let len = name_bytes.len().min(12);
        username_bytes[..len].copy_from_slice(&name_bytes[..len]);

        let msg = ClientMessage::Join {
            username: username_bytes,
        };
        match bincode::serialize(&msg) {
            Ok(bytes) => {
                // Send the join message to the server via the reliable stream
                let mut input_array = [0u8; 16];
                let len = bytes.len().min(16);
                input_array[..len].copy_from_slice(&bytes[..len]);

                let broker_msg = BrokerMessage::ClientInput {
                    client_id: 0, // TODO: wait for the broker's welcome message to get the actual client ID before sending inputs
                    input: input_array,
                };

                if let Some(peer) = &mut net.peer {
                    let _ = peer.send(&conn, &stream, Bytes::from(broker_msg.to_bytes()));
                }
            }
            Err(e) => error!("Error serializing join message : {:?}", e),
        }
    } else {
        net.unreliable_stream = Some(stream.clone());

        // Send fake input to wake up the stream and make sure it's ready for low-latency messages
        //(and show all players in the AOI right after login, without waiting for the first real input from the player)
        let wake_up_msg = ClientMessage::MoveInput { x: 0.0, y: 0.0 };
        match bincode::serialize(&wake_up_msg) {
            Ok(bytes) => {
                // Send the first MoveInput message to the server via the reliable stream
                let mut input_array = [0u8; 16];
                let len = bytes.len().min(16);
                input_array[..len].copy_from_slice(&bytes[..len]);

                let broker_msg = BrokerMessage::ClientInput {
                    client_id: 0, // TODO: wait for the broker's welcome message to get the actual client ID before sending inputs
                    input: input_array,
                };

                if let Some(peer) = &mut net.peer {
                    let _ = peer.send(&conn, &stream, Bytes::from(broker_msg.to_bytes()));
                }
            }
            Err(e) => error!("Error serializing MoveInput message : {:?}", e),
        }
    }
}

// This function processes messages received from the server. It deserializes the message and takes appropriate
// actions based on its type (e.g., handling welcome messages or AOI snapshots).
fn handle_server_message(
    data: &[u8],
    commands: &mut Commands,
    game_state: &mut crate::game::GameState,
    transforms: &mut Query<&mut Transform>,
    current_time: f64,
) {
    // Deserialize the broker message from the received bytes. If deserialization fails, log a warning and ignore the message.
    let Some(broker_message) = BrokerMessage::from_bytes(data) else {
        warn!("[CLIENT] Invalid broker envelope received.");
        return;
    };

    match broker_message {
        BrokerMessage::Broadcast { payload } => {
            if let Ok(server_msg) = bincode::deserialize::<ServerMessage>(&payload) {
                match server_msg {
                    ServerMessage::Welcome { player_id } => {
                        info!("[CLIENT]: Welcome ! My ID is: {}", player_id);
                        game_state.my_id = Some(player_id);
                    }
                    ServerMessage::AOISnapshot { players } => {
                        crate::network::handle_aoi_snapshot(
                            players,
                            commands,
                            game_state,
                            transforms,
                            current_time,
                        );
                    }
                }
            }
        }
        _ => {
            // The client ignores other types of messages from the broker
        }
    }
}

// This function processes the AOI snapshot received from the server, which contains the list of players currently in the client's area of interest.
// It updates existing player entities, spawns new ones, and despawns those that are no longer present.
fn handle_aoi_snapshot(
    players: Vec<shared::PlayerState>,
    commands: &mut Commands,
    game_state: &mut crate::game::GameState,
    transforms: &mut Query<&mut Transform>,
    current_time: f64,
) {
    let mut current_frame_ids = Vec::new();

    for p in players {
        current_frame_ids.push(p.id);

        if let Some(&mut (entity, ref mut last_seen)) = game_state.spawned_players.get_mut(&p.id) {
            // Existing player in AOI, update their position and last seen timestamp
            *last_seen = current_time;
            if let Ok(mut transform) = transforms.get_mut(entity) {
                transform.translation.x = p.x;
                transform.translation.y = p.y;
            }
        } else {
            // new player in AOI, spawn an entity for them
            let is_me = game_state.my_id == Some(p.id);
            let color = if is_me {
                Color::srgb(0.2, 0.2, 1.0)
            } else {
                Color::srgb(1.0, 0.2, 0.2)
            };

            // Display name truncated if too long
            let display_name = if p.username.len() > 10 {
                format!("{}...", &p.username[..8])
            } else {
                p.username
            };

            let entity = commands
                .spawn((
                    Sprite {
                        color,
                        custom_size: Some(Vec2::new(30.0, 30.0)),
                        ..default()
                    },
                    Transform::from_xyz(p.x, p.y, 0.0),
                    crate::game::PlayerComponent,
                ))
                .with_children(|parent| {
                    // Display the player's username above their character
                    parent.spawn((
                        Text2d::new(display_name),
                        TextFont {
                            font_size: 15.0,
                            ..default()
                        },
                        TextColor(Color::WHITE),
                        Transform::from_xyz(0.0, 25.0, 1.0),
                    ));
                })
                .id();

            game_state
                .spawned_players
                .insert(p.id, (entity, current_time));
        }
    }
}
