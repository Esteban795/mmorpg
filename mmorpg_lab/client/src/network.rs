use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
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

fn handle_network(
    mut net: ResMut<ClientNetworkManager>,
    settings: Res<ConnectionSettings>,
    mut commands: Commands,
    mut game_state: ResMut<crate::game::GameState>,
    mut transforms: Query<&mut Transform>,
) {
    loop {
        let event_result = if let Some(peer) = &mut net.peer {
            peer.poll()
        } else {
            return; // If peer isn't intialized yet, skip the system
        };

        match event_result {
            Ok(Some(event)) => {
                match event {
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
                        if stream.is_reliable() {
                            net.reliable_stream = Some(stream.clone());

                            info!(
                                "[CLIENT]: Sending join message with username: {}",
                                settings.username
                            );
                            let msg = ClientMessage::Join {
                                username: settings.username.clone(),
                            };
                            match bincode::serialize(&msg) {
                                Ok(bytes) => {
                                    if let Some(peer) = &mut net.peer {
                                        if let Err(e) =
                                            peer.send(&conn, &stream, Bytes::from(bytes))
                                        {
                                            error!("Erreur lors de l'envoi du Join : {:?}", e);
                                        }
                                    }
                                }
                                Err(e) => error!("Erreur de sérialisation du Join : {:?}", e),
                            }
                        } else {
                            net.unreliable_stream = Some(stream.clone());

                            // Send fake input to wake up the stream and make sure it's ready for low-latency messages
                            //(and show all players in the AOI right after login, without waiting for the first real input from the player)
                            let wake_up_msg = ClientMessage::MoveInput { x: 0.0, y: 0.0 };
                            if let Ok(bytes) = bincode::serialize(&wake_up_msg) {
                                if let Some(peer) = &mut net.peer {
                                    let _ = peer.send(&conn, &stream, Bytes::from(bytes));
                                }
                            }
                        }
                    }
                    GameNetworkEvent::Message { data, .. } => {
                        if let Ok(message) = bincode::deserialize::<ServerMessage>(&data) {
                            match message {
                                ServerMessage::Welcome { player_id } => {
                                    info!(
                                        "[CLIENT]: Welcome message received with player ID: {}",
                                        player_id
                                    );
                                    game_state.my_id = Some(player_id);
                                }
                                ServerMessage::AOISnapshot { players } => {
                                    let mut current_frame_ids = Vec::new();

                                    for p in players {
                                        current_frame_ids.push(p.id);

                                        if let Some(&entity) = game_state.spawned_players.get(&p.id)
                                        {
                                            // existing player in AOI, update position
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

                                            game_state.spawned_players.insert(p.id, entity);
                                        }
                                    }

                                    // Gestion of despawn of entities that are no longer in the AOI :
                                    // If an entity in the HashMap is not in the current frame's list of player IDs, it is despawned and removed from the HashMap
                                    game_state.spawned_players.retain(|&id, &mut entity| {
                                        if !current_frame_ids.contains(&id) {
                                            commands.entity(entity).despawn();
                                            false
                                        } else {
                                            true
                                        }
                                    });
                                }
                            }
                        }
                    }
                    GameNetworkEvent::Disconnected(_) => {
                        info!("[CLIENT] : Disconnected from the server.");
                    }
                    _ => {}
                }
            }
            Ok(None) => break, // No more event to process, exit the loop and wait for the next frame
            Err(_) => break,
        }
    }
}
