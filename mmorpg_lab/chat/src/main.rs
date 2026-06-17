pub mod chat_service;
pub mod moderator;

use std::path::PathBuf;

use game_sockets::{GamePeer, protocols::QuicBackend};
use shared::{DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT};
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use crate::{chat_service::ChatService, moderator::Moderator};

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    info!("Starting chat server...");

    let backend = QuicBackend::new();
    let peer = GamePeer::new(backend);

    let broker_addr: String = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| DEFAULT_BROKER_IP.to_string())
        .parse()
        .expect("Invalid BROKER_ADDR");

    let broker_port: u16 = std::env::var("BROKER_PORT")
        .unwrap_or_else(|_| DEFAULT_BROKER_PORT.to_string())
        .parse()
        .expect("Invalid BROKER_PORT");

    // Find the absolute file path of the badwords.txt file
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mut badwords_path = PathBuf::from(manifest_dir);
    badwords_path.push("assets");
    badwords_path.push("badwords.txt");
    let path_str = badwords_path.to_str().expect("Chemin invalide");

    info!("Loading bad words from file: {}", path_str);
    let moderator = Moderator::new(path_str);

    if let Err(e) = peer.connect(&broker_addr, broker_port) {
        error!("Failed to connect to broker: {:?}", e);
        return;
    }

    let mut chat_service = ChatService::new(moderator, peer);
    chat_service.run();
}
