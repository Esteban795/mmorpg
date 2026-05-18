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
        println!(
            "CLIENT : Lancement de la connexion QUIC vers {}:{}...",
            ip, port
        );

        let server_ip = ip
            .parse::<IpAddr>()
            .expect("Format IP invalide fourni par le Gatekeeper");
        let local_bind_ip = [0, 0, 0, 0];

        // Opens a QUIC connection to the game server with the provided IP and port, skipping certificate verification for simplicity
        let result = client.open_connection(ClientConnectionConfiguration {
            addr_config: ClientAddrConfiguration::from_ips(server_ip, *port, local_bind_ip, 0),
            cert_mode: CertificateVerificationMode::SkipVerification,
            defaultables: Default::default(),
        });

        // handles the result of the connection attempt : if failure, return to login menu and display error message
        if let Err(e) = result {
            eprintln!(
                "CLIENT ERREUR : Échec de l'ouverture de connexion : {:?}",
                e
            );

            settings.error_message = Some(format!("Échec réseau local : {:?}", e));

            next_state.set(AppState::LoginMenu);
        }
    } else {
        eprintln!("ERREUR : Aucun serveur cible n'a été défini !");
        settings.error_message = Some("Erreur interne : Cible introuvable".to_string());
        next_state.set(AppState::LoginMenu);
    }
}

fn handle_connection_events(
    mut connection_events: MessageReader<ConnectionEvent>,
    mut client: ResMut<QuinnetClient>,
    settings: Res<ConnectionSettings>,
) {
    for _event in connection_events.read() {
        println!(
            "CLIENT : Connexion QUIC établie ! Envoi du pseudo '{}'...",
            settings.username
        );

        // Send a Join message to the server with the username
        let connection = client.connection_mut();
        let _ = connection.send_message(ClientMessage::Join {
            username: settings.username.clone(),
        });
    }
}

fn handle_messages(mut client: ResMut<QuinnetClient>) {
    let connection = client.connection_mut();

    // Check for incoming messages from the server without blocking, on stream 0 (reliable ordered)
    while let Ok(Some(message)) = connection.receive_message::<ServerMessage>() {
        match message {
            ServerMessage::Welcome { player_id } => {
                println!("CLIENT : Welcome reçu ! Mon ID est : {}", player_id);
            }
        }
    }
}
