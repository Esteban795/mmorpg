mod game;
mod loginmenu;
mod network;
mod state;

use bevy::prelude::*;
use bevy_egui::EguiPlugin;
use loginmenu::LoginMenuPlugin;
use network::NetworkPlugin;
use state::AppState;

use tracing::info;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting Client!");
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(LoginMenuPlugin)
        .add_plugins(EguiPlugin::default())
        .add_plugins(NetworkPlugin)
        .add_plugins(game::GamePlugin)
        .init_state::<AppState>()
        .insert_state(AppState::LoginMenu)
        .run();
}
