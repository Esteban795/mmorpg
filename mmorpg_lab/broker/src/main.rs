mod network;
mod routing;
mod state;

use crate::network::BrokerNetwork;
use crate::routing::process_network_events;
use crate::state::BrokerState;
use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use game_sockets::{GamePeer, protocols::QuicBackend};
use shared::DEFAULT_BROKER_PORT;
use std::time::Duration;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

fn main() {
    //Debug
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Fatal error: unable to set up logging subscriber");

    // Initialize the Quic Backend
    let broker_peer = GamePeer::new(QuicBackend::new());
    broker_peer
        .listen("0.0.0.0", DEFAULT_BROKER_PORT)
        .expect("Failed to bind Broker socket");
    info!("Broker is running on 0.0.0.0:{}", DEFAULT_BROKER_PORT);

    App::new()
        .add_plugins(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
            1.0 / 60.0,
        )))
        .insert_resource(BrokerState::default())
        .insert_resource(BrokerNetwork { peer: broker_peer })
        .add_systems(Update, process_network_events)
        .run();
}
