mod loginmenu;
mod state;

use bevy::prelude::*;
use bevy_egui::EguiPlugin;
use loginmenu::LoginMenuPlugin;
use state::AppState;

fn main() {
    println!("Hello, world!");
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(LoginMenuPlugin)
        .add_plugins(EguiPlugin::default())
        .init_state::<AppState>()
        .insert_state(AppState::LoginMenu)
        .run();
}
