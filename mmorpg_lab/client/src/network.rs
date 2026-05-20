use bevy::prelude::*;
use bevy_quinnet::client::certificate::CertificateVerificationMode;
use bevy_quinnet::client::connection::*;
use bevy_quinnet::client::*;
use shared::{ClientMessage, ServerMessage};

use std::net::IpAddr;

use crate::loginmenu::ConnectionSettings;
use crate::state::AppState;

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(OnEnter(AppState::InGame), start_connection) // start_connection called once when entering InGame state
            .add_systems(
                Update,
                (handle_connection_events, handle_messages).run_if(in_state(AppState::InGame)),
            );
    }
}

fn start_connection(
    mut client: ResMut<QuinnetClient>,
    mut settings: ResMut<ConnectionSettings>,
    mut next_state: ResMut<NextState<AppState>>,
) {
    if let Some((ip, port)) = &settings.server_target {
        info!(
            "CLIENT : Launching QUIC connection to {}:{}...",
            ip, port
        );

        let server_ip = ip
            .parse::<IpAddr>()
            .expect("Invalid IP format from Gatekeeper");
        let local_bind_ip = [0, 0, 0, 0];

        // Opens a QUIC connection to the game server with the provided IP and port, skipping certificate verification for simplicity
        let result = client.open_connection(ClientConnectionConfiguration {
            addr_config: ClientAddrConfiguration::from_ips(server_ip, *port, local_bind_ip, 0),
            cert_mode: CertificateVerificationMode::SkipVerification,
            defaultables: Default::default(),
        });

        // handles the result of the connection attempt : if failure, return to login menu and display error message
        if let Err(e) = result {
            error!(
                "CLIENT ERROR : Failed to open connection : {:?}",
                e
            );

            settings.error_message = Some(format!("Local network failure : {:?}", e));

            next_state.set(AppState::LoginMenu);
        }
    } else {
        error!("CLIENT ERROR : No target server defined !");
        settings.error_message = Some("Internal error : Target not found".to_string());
        next_state.set(AppState::LoginMenu);
    }
}

fn handle_connection_events(
    mut connection_events: MessageReader<ConnectionEvent>,
    mut client: ResMut<QuinnetClient>,
    settings: Res<ConnectionSettings>,
) {
    for _event in connection_events.read() {
        info!(
            "CLIENT : QUIC connection established ! Sending username '{}'...",
            settings.username
        );

        // Send a Join message to the server with the username
        let connection = client.connection_mut();
        let _ = connection.send_message(ClientMessage::Join {
            username: settings.username.clone(),
        });
    }
}

fn handle_messages(
    mut client: ResMut<QuinnetClient>,
    mut commands: Commands,
    mut game_state: ResMut<crate::game::GameState>,
    mut transforms: Query<&mut Transform>,
) {
    let connection = client.connection_mut();

    while let Ok(Some(message)) = connection.receive_message::<ServerMessage>() {
        // info!("Received message : {} from server", message);
        match message {
            ServerMessage::Welcome { player_id } => {
                info!("Local ID received : {}", player_id);
                game_state.my_id = Some(player_id);
            }
            ServerMessage::AOISnapshot { players } => {
                let mut current_frame_ids = Vec::new();

                for p in players {
                    current_frame_ids.push(p.id);

                    if let Some(&entity) = game_state.spawned_players.get(&p.id) {
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
