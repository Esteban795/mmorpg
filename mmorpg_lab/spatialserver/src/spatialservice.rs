use std::collections::HashMap;
use tracing::{info, warn};

use crate::quadtree::QuadTree;
use crate::rect::{Rect, Vec2};
use game_sockets::{GameNetworkEvent, GamePeer, protocols::QuicBackend};

pub struct SpatialService {
    quad_tree: QuadTree,
    client_shards: HashMap<u32, u32>, // client_id -> shard_id
    peer: GamePeer,
}

impl SpatialService {
    pub fn new() -> Self {
        let backend = QuicBackend::new();
        let peer = GamePeer::new(backend);

        Self {
            quad_tree: QuadTree::new(
                Rect {
                    x: 0.0,
                    y: 0.0,
                    width: 1000.0,
                    height: 1000.0,
                },
                0,
                4,
                10,
            ),
            client_shards: HashMap::new(),
            peer,
        }
    }

    pub fn run(&mut self) {
        loop {
            while let Ok(Some(event)) = self.peer.poll() {
                match event {
                    GameNetworkEvent::Connected(connection) => {
                        info!("[NETWORK] Client connected: {:?}", connection.connection_id);
                    }
                    GameNetworkEvent::StreamCreated(connection, stream) => {
                        info!(
                            "[NETWORK] Stream created for client {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );
                    }
                    GameNetworkEvent::StreamClosed(connection, stream) => {
                        info!(
                            "[NETWORK] Stream closed for client {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );
                    }
                    GameNetworkEvent::Disconnected(game_connection) => {
                        info!(
                            "[NETWORK] Client disconnected: {:?}",
                            game_connection.connection_id
                        );
                        // TODO : 
                    }
                    GameNetworkEvent::Message {
                        connection,
                        stream,
                        data,
                    } => {
                        info!(
                            "[NETWORK] Message received from client {:?} on stream {:?}: {} bytes",
                            connection.connection_id,
                            stream,
                            data.len()
                        );
                        // TODO: Extract Vec2 from message and call handle_position_update
                    }
                    GameNetworkEvent::Error { connection, inner } => {
                        warn!(
                            "[NETWORK] Error on connection {:?}: {:?}",
                            connection.connection_id, inner
                        );
                    }
                }
            }
        }
    }

    pub fn handle_position_update(&mut self, client_id: u32, pos: &Vec2, margin: f32) {
        // updates is the list of (affected_client_id, new_shard_id) for ALL clients affected by this position update (including the active one)
        let updates = self.quad_tree.insert(client_id, *pos);

        // Impossible since QuadTree.insert() will return at least the active player if the position is valid, but we check just in case
        if updates.is_empty() {
            warn!(
                client_id,
                x = pos.x,
                y = pos.y,
                "Out of bounds position update ignored"
            );
            return;
        }

        // Handle shard changes for all affected clients
        for (affected_client, new_shard) in updates {
            let old_shard_opt = self.client_shards.get(&affected_client).copied();

            if old_shard_opt != Some(new_shard) {
                // Unsubscribe from old shard if exists
                if let Some(old_shard) = old_shard_opt {
                    self.send_unsubscribe(affected_client, old_shard);
                }

                // Subscribe to new shard
                self.send_subscribe(affected_client, new_shard);
                self.client_shards.insert(affected_client, new_shard);

                if affected_client != client_id {
                    info!(
                        "Passive Handoff: Player {} moved to Shard {} (Split triggered by {})",
                        affected_client, new_shard, client_id
                    );
                }
            }
        }

        let nearby_shards = self.quad_tree.shards_near(pos, margin);
        if nearby_shards.len() > 1 {
            self.emit_crossing_alert(client_id, nearby_shards);
        }
    }

    fn send_unsubscribe(&self, client_id: u32, shard_id: u32) {
        let topic = format!("shard:{}", shard_id);
        info!(client_id, topic, "Unsubscribe");
            
    }

    fn send_subscribe(&self, client_id: u32, shard_id: u32) {
        let topic = format!("shard:{}", shard_id);
        info!(client_id, topic, "Subscribe");
        // TODO: Call broker
    }

    fn emit_crossing_alert(&self, client_id: u32, nearby_shards: Vec<u32>) {
        info!(client_id, ?nearby_shards, "CrossingAlert émis");
        // TODO: Call broker
    }
}
