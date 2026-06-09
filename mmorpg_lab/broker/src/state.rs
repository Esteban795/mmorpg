use bevy::prelude::*;
use game_sockets::GameStream;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub type Topic = [u8; 32];

//Basically all the things the broker needs to know to do its job of routing messages between clients and shards

#[derive(Resource)]
pub struct BrokerState {
    // PubSub routing

    // Topic -> Set of Client IDs subscribed to that topic
    pub topic_subscribers: HashMap<Topic, HashSet<u32>>,
    // Client ID -> Topic (shard) they are subscribed to
    pub client_to_topic: HashMap<u32, Topic>,

    // Shard routing (Topic -> Shard's Network Uuid)
    pub topic_to_shard: HashMap<Topic, Uuid>,

    // Network Identity mapping (Uuid <-> u32)
    pub next_client_id: u32,
    pub uuid_to_id: HashMap<Uuid, u32>,
    pub id_to_uuid: HashMap<u32, Uuid>,
    pub spatial_server_uuid: Option<Uuid>, // Track the spatial server's UUID for direct routing of position updates

    // Stream registry — one reliable + one unreliable stream per connection UUID.
    // Must be populated via StreamCreated events before any sends are attempted.
    pub connection_reliable_streams: HashMap<Uuid, GameStream>,
    pub connection_unreliable_streams: HashMap<Uuid, GameStream>,
}

impl Default for BrokerState {
    fn default() -> Self {
        Self {
            topic_subscribers: HashMap::new(),
            client_to_topic: HashMap::new(),
            topic_to_shard: HashMap::new(),
            next_client_id: 1, // Start IDs at 1
            uuid_to_id: HashMap::new(),
            id_to_uuid: HashMap::new(),
            connection_reliable_streams: HashMap::new(),
            connection_unreliable_streams: HashMap::new(),
            spatial_server_uuid: None,
        }
    }
}
