mod game;
mod loginmenu;
mod network;
mod state;

use bevy::prelude::*;
use bevy_egui::EguiPlugin;
use bevy_quinnet::client::QuinnetClientPlugin;
use loginmenu::LoginMenuPlugin;
use network::NetworkPlugin;
use state::AppState;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    info!("Starting client...");
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(QuinnetClientPlugin::default())
        .add_plugins(LoginMenuPlugin)
        .add_plugins(EguiPlugin::default())
        .add_plugins(NetworkPlugin)
        .add_plugins(game::GamePlugin)
        .init_state::<AppState>()
        .insert_state(AppState::LoginMenu)
        .run();
}
