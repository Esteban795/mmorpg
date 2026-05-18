mod loginmenu;
mod network;
mod state;

use bevy::prelude::*;
use bevy_egui::EguiPlugin;
use bevy_quinnet::client::QuinnetClientPlugin;
use loginmenu::LoginMenuPlugin;
use network::NetworkPlugin;
use state::AppState;

fn main() {
    println!("Hello, world!");
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(QuinnetClientPlugin::default())
        .add_plugins(LoginMenuPlugin)
        .add_plugins(EguiPlugin::default())
        .add_plugins(NetworkPlugin)
        .init_state::<AppState>()
        .insert_state(AppState::LoginMenu)
        .run();
}
