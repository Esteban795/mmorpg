use bevy::prelude::*;
use bevy_quinnet::client::QuinnetClient;
use shared::ClientMessage;
use std::collections::HashMap;

use crate::state::AppState;
use tracing::{info};

pub struct GamePlugin;

#[derive(Resource, Default)]
pub struct GameState {
    pub my_id: Option<u64>,
    pub spawned_players: HashMap<u64, Entity>, // maps client ID to the corresponding player entity in the world
}

#[derive(Component)]
pub struct PlayerComponent; // Tag component to identify player entities

impl Plugin for GamePlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<GameState>()
            .add_systems(OnEnter(AppState::InGame), setup_map)
            .add_systems(
                Update,
                (player_input, move_camera).run_if(in_state(AppState::InGame)),
            );
    }
}

fn setup_map(mut commands: Commands) {
    // Floor
    commands.spawn((
        Sprite {
            color: Color::srgb(0.2, 0.5, 0.2),
            custom_size: Some(Vec2::new(800.0, 800.0)),
            ..default()
        },
        Transform::from_xyz(0.0, 0.0, -1.0),
    ));
}

fn player_input(keyboard: Res<ButtonInput<KeyCode>>, mut client: ResMut<QuinnetClient>) {
    let mut x = 0.0;
    let mut y = 0.0;

    if keyboard.pressed(KeyCode::KeyW) || keyboard.pressed(KeyCode::ArrowUp) {
        y += 1.0;
    }
    if keyboard.pressed(KeyCode::KeyS) || keyboard.pressed(KeyCode::ArrowDown) {
        y -= 1.0;
    }
    if keyboard.pressed(KeyCode::KeyA) || keyboard.pressed(KeyCode::ArrowLeft) {
        x -= 1.0;
    }
    if keyboard.pressed(KeyCode::KeyD) || keyboard.pressed(KeyCode::ArrowRight) {
        x += 1.0;
    }

    // if (x, y) != (0.0, 0.0) { 
    //     info!("Player input: x={}, y={}", x, y);
    // }
    if x != 0.0 || y != 0.0 {
        let connection = client.connection_mut();
        let _ = connection.send_message(ClientMessage::MoveInput { x, y });
    }
}

fn move_camera(
    game_state: Res<GameState>,
    player_query: Query<&Transform, With<PlayerComponent>>,
    mut camera_query: Query<&mut Transform, (With<Camera>, Without<PlayerComponent>)>,
) {
    if let Some(my_id) = game_state.my_id {
        if let Some(&entity) = game_state.spawned_players.get(&my_id) {
            if let Ok(player_transform) = player_query.get(entity) {
                if let Ok(mut camera_transform) = camera_query.single_mut() {
                    camera_transform.translation.x = player_transform.translation.x;
                    camera_transform.translation.y = player_transform.translation.y;
                }
            }
        }
    }
}
