mod state;
mod network;
mod routing;

use bevy::prelude::*;
use bevy::app::ScheduleRunnerPlugin;
use std::time::Duration;
use game_sockets::{GamePeer, protocols::QuicBackend};
use crate::state::BrokerState;
use crate::network::BrokerNetwork;
use crate::routing::process_network_events;
use shared::DEFAULT_BROKER_PORT;

fn main() {
    // Initialize the Quic Backend
    let broker_peer = GamePeer::new(QuicBackend::new());
    broker_peer.listen("0.0.0.0", DEFAULT_BROKER_PORT).expect("Failed to bind Broker socket");
    println!("Broker is running on 0.0.0.0:{}", DEFAULT_BROKER_PORT);

    App::new()
        .add_plugins(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(1.0 / 60.0))) 
        .insert_resource(BrokerState::default())
        .insert_resource(BrokerNetwork { peer: broker_peer })
        .add_systems(Update, process_network_events)
        .run();
}