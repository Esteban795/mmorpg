use bevy::prelude::*;
use bevy_egui::{EguiContexts, EguiPrimaryContextPass, egui};
use bytes::Bytes;
use shared::broker_protocol::BrokerMessage;

use crate::network::ClientNetworkManager;
use crate::state::AppState;
// Importe ton GameState depuis là où tu l'as défini
use crate::game::GameState;

// La ressource qui stocke l'état du chat
#[derive(Resource, Default)]
pub struct ChatState {
    pub current_input: String,
    pub is_focused: bool,
    pub chat_history: Vec<String>,
}

// La définition de ton Plugin
pub struct ChatPlugin;

impl Plugin for ChatPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ChatState>().add_systems(
            EguiPrimaryContextPass,
            draw_chat_ui.run_if(in_state(AppState::InGame)),
        );
    }
}

fn draw_chat_ui(
    mut contexts: EguiContexts,
    mut chat_state: ResMut<ChatState>,
    mut net: ResMut<ClientNetworkManager>,
    game_state: Res<GameState>,
) {
    let ctx = contexts.ctx_mut();

    if let Ok(ctx) = ctx {
        egui::Window::new("Chat")
            .anchor(egui::Align2::LEFT_BOTTOM, egui::vec2(10.0, -10.0))
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                // Affichage de l'historique
                egui::ScrollArea::vertical()
                    .max_height(150.0)
                    .show(ui, |ui| {
                        for msg in &chat_state.chat_history {
                            ui.label(msg);
                        }
                    });

                ui.separator();

                // Boîte de texte
                let response = ui.add(
                    egui::TextEdit::singleline(&mut chat_state.current_input)
                        .hint_text("Appuyez sur Entrée pour parler...")
                        .desired_width(250.0),
                );

                chat_state.is_focused = response.has_focus();

                // Envoi du message
                if response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                    let message_to_send = chat_state.current_input.clone();
                    chat_state.current_input.clear();

                    if !message_to_send.trim().is_empty() {
                        let my_id = game_state.my_id.unwrap_or(0);
                        let conn_opt = net.server_connection.clone();
                        let stream_opt = net.reliable_stream.clone();

                        if let (Some(peer), Some(conn), Some(stream)) =
                            (&mut net.peer, conn_opt, stream_opt)
                        {
                            let mut msg_array = [0u8; 64];
                            let bytes = message_to_send.as_bytes();
                            let len = bytes.len().min(64);
                            msg_array[..len].copy_from_slice(&bytes[..len]);

                            let broker_msg = BrokerMessage::ClientChatMessage {
                                client_id: my_id,
                                msg: msg_array,
                            };

                            if let Err(e) =
                                peer.send(&conn, &stream, Bytes::from(broker_msg.to_bytes()))
                            {
                                tracing::warn!("Chat message loss : {:?}", e);
                            }
                        }
                        response.request_focus();
                    }
                }
            });
    }
}
