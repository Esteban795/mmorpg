pub mod chat_service;
pub mod moderator;

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

    let moderator = Moderator::new("/home/esteban/CODE/mmorpg/mmorpg_lab/chat/src/badwords.txt");

    if let Err(e) = peer.connect(&broker_addr, broker_port) {
        error!("Failed to connect to broker: {:?}", e);
        return;
    }

    let mut chat_service = ChatService::new(moderator, peer);
    chat_service.run();
}
