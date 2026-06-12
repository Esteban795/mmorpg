use bevy::prelude::*;
use game_sockets::GamePeer;

//Wrapper to use the GamePeer as a Bevy Resource, so we can access it in our systems to send/receive messages

#[derive(Resource)]
pub struct BrokerNetwork {
    pub peer: GamePeer,
}