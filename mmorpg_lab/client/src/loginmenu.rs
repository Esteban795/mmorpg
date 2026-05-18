use bevy::app::AppExit;
use bevy::prelude::*;
use bevy::tasks::{IoTaskPool, Task}; 
use bevy_egui::{EguiContexts, EguiPrimaryContextPass, egui};
use futures_lite::future;

use shared::{LoginRequest, LoginResponse};
use crate::state::AppState;


// Bevy task to run the async login request without blocking main thread
#[derive(Component)]
pub struct LoginTask(Task<Result<LoginResponse, String>>);

#[derive(Resource, Default)]
pub struct ConnectionSettings {
    pub username: String,
    pub password: String,
    pub error_message: Option<String>,
}

pub struct LoginMenuPlugin;

impl Plugin for LoginMenuPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ConnectionSettings>()
            .add_systems(Startup, setup_camera.run_if(in_state(AppState::LoginMenu)))
            .add_systems(
                EguiPrimaryContextPass, 
                (menu_ui, poll_login_task).run_if(in_state(AppState::LoginMenu)),
            );
    }
}

fn setup_camera(mut commands: Commands) {
    commands.spawn(Camera2d);
}


fn menu_ui(
    mut commands: Commands,
    mut contexts: EguiContexts,
    mut settings: ResMut<ConnectionSettings>,
    mut exit: MessageWriter<AppExit>,
    task_query: Query<&LoginTask>, 
) {
    let Ok(ctx) = contexts.ctx_mut() else { return; };

    let is_connecting = !task_query.is_empty();

    egui::Window::new("Connexion au Serveur")
        .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
        .collapsible(false)
        .resizable(false)
        .show(ctx, |ui| {
            ui.vertical_centered(|ui| {

                // Username section
                ui.label("Nom d'utilisateur :");

                ui.add_enabled(
                    !is_connecting,
                    egui::TextEdit::singleline(&mut settings.username).hint_text("Pseudo")
                );
                
                ui.add_space(10.0);

                // Password section
                ui.label("Mot de passe :");
                ui.add_enabled(
                    !is_connecting,
                    egui::TextEdit::singleline(&mut settings.password).hint_text("Mot de passe").password(true)
                );

                ui.add_space(10.0);

                if let Some(err) = &settings.error_message {
                    ui.colored_label(egui::Color32::RED, err);
                    ui.add_space(5.0);
                }

                ui.add_enabled_ui(!is_connecting, |ui| {
                    let btn_text = if is_connecting { "Connexion..." } else { "Se Connecter" };
                    
                    if ui.button(btn_text).clicked() {
                        if !settings.username.is_empty() && !settings.password.is_empty() {
                            settings.error_message = None;
                            
                            let payload = LoginRequest {
                                username: settings.username.clone(),
                                password: settings.password.clone(),
                            };

                            // Get IO Task pool from Bevy
                            let thread_pool = IoTaskPool::get();

                            let task = thread_pool.spawn(async move {
                                let mut res = surf::post("http://127.0.0.1:8080/login")
                                    .body_json(&payload)
                                    .map_err(|_| "Erreur de formatage JSON".to_string())?
                                    .await
                                    .map_err(|e| format!("Impossible de joindre le Gatekeeper: {}", e))?;

                                if res.status().is_success() {
                                    res.body_json::<LoginResponse>()
                                        .await
                                        .map_err(|_| "Format de réponse invalide".to_string())
                                } else {
                                    Err("Identifiants incorrects ou serveurs pleins".to_string())
                                }
                            });

                            commands.spawn(LoginTask(task));
                        }
                    }
                });

                if ui.button("Quitter").clicked() {
                    exit.write(AppExit::Success);
                }
            });
        });
}

// Poll to see when the login task is done, and handle the result (success or error)

fn poll_login_task(
    mut commands: Commands,
    mut query: Query<(Entity, &mut LoginTask)>, // Note le `mut` ici !
    mut next_state: ResMut<NextState<AppState>>,
    mut settings: ResMut<ConnectionSettings>,
) {
    for (entity, mut task) in &mut query {
        
        if let Some(result) = future::block_on(future::poll_once(&mut task.0)) {
            
            commands.entity(entity).despawn();

            match result {
                Ok(login_response) => {
                    println!(
                        "Connexion réussie ! Redirection vers le serveur : {}:{}", 
                        login_response.server.ip, 
                        login_response.server.port
                    );
                    
                    next_state.set(AppState::InGame);
                }
                Err(error_msg) => {
                    println!("Erreur : {}", error_msg);
                    settings.error_message = Some(error_msg);
                }
            }
        }
    }
}