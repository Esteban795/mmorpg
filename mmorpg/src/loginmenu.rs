use bevy::app::AppExit;
use bevy::prelude::*;
use bevy_egui::{EguiContexts, EguiPrimaryContextPass, egui};


use crate::state::AppState;

#[derive(Resource, Default)]
pub struct ConnectionSettings {
    pub username: String,
}

pub struct LoginMenuPlugin;

impl Plugin for LoginMenuPlugin {
    fn build(&self, app: &mut App) {
        // Run iff in LoginMenu state
        app.init_resource::<ConnectionSettings>()
            .add_systems(Startup, setup_camera.run_if(in_state(AppState::LoginMenu)))
            .add_systems(
                EguiPrimaryContextPass,
                menu_ui.run_if(in_state(AppState::LoginMenu)),
            );
    }
}

fn setup_camera(mut commands: Commands) {
    commands.spawn(Camera2d);
}

fn menu_ui(
    mut contexts: EguiContexts,
    mut settings: ResMut<ConnectionSettings>,
    mut next_state: ResMut<NextState<AppState>>,
    mut exit: MessageWriter<AppExit>,
) {
    let Ok(ctx) = contexts.ctx_mut() else {
        return;
    };

    egui::Window::new("Connexion au Serveur")
        .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
        .collapsible(false)
        .resizable(false)
        .show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.label("Nom d'utilisateur :");

                ui.add(egui::TextEdit::singleline(&mut settings.username).hint_text("Pseudo"));

                ui.add_space(10.0);

                if ui.button("Se Connecter").clicked() {
                    if !settings.username.is_empty() {
                        // CHANGE THIS TO ACTUALLY CONNECT TO THE SERVER
                        println!("Connexion de {}", settings.username);
                        next_state.set(AppState::InGame);
                    }
                }

                if ui.button("Quitter").clicked() {
                    exit.write(AppExit::Success);
                }
            });
        });
}
