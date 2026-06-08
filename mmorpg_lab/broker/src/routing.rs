use crate::network::BrokerNetwork;
use crate::state::BrokerState;
use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::GameNetworkEvent;
use shared::ServerMessage;
use shared::broker_protocol::{BrokerMessage, string_to_topic};

pub fn process_network_events(mut network: ResMut<BrokerNetwork>, mut state: ResMut<BrokerState>) {
    // Poll all available events from the background network thread
    while let Ok(Some(event)) = network.peer.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                // Assign a u32 Client ID to the new Uuid connection
                let new_id = state.next_client_id;
                state.next_client_id += 1; // TODO: rendre cette opération atomique si jamais on a des problèmes de concurrence

                state.uuid_to_id.insert(conn.connection_id, new_id);
                state.id_to_uuid.insert(new_id, conn.connection_id);

                println!(
                    "New connection: Uuid {:?} assigned ID {}",
                    conn.connection_id, new_id
                );
            }

            GameNetworkEvent::Disconnected(conn) => {
                // Cleanup disconnected clients
                if let Some(id) = state.uuid_to_id.remove(&conn.connection_id) {
                    state.id_to_uuid.remove(&id);
                    if let Some(topic) = state.client_to_topic.remove(&id) {
                        if let Some(subs) = state.topic_subscribers.get_mut(&topic) {
                            subs.remove(&id);
                        }
                    }
                }
            }

            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(msg) = BrokerMessage::from_bytes(&data) else {
                    continue;
                };

                match msg {
                    BrokerMessage::Subscribe { client_id, topic } => {
                        state
                            .topic_subscribers
                            .entry(topic)
                            .or_default()
                            .insert(client_id);
                        state.client_to_topic.insert(client_id, topic);
                        println!(" Client {} subscribed", client_id);
                    }

                    BrokerMessage::Unsubscribe { client_id, topic } => {
                        if let Some(subs) = state.topic_subscribers.get_mut(&topic) {
                            subs.remove(&client_id);
                        }
                        state.client_to_topic.remove(&client_id);
                        println!(" Client {} unsubscribed", client_id);
                    }

                    BrokerMessage::Publish { topic, payload } => {
                        // Register Shard
                        state.topic_to_shard.insert(topic, connection.connection_id);

                        // Broadcast to clients
                        if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                            for &client_id in subscribers {
                                if let Some(&client_uuid) = state.id_to_uuid.get(&client_id) {
                                    let out_msg = BrokerMessage::Broadcast {
                                        payload: payload.clone(),
                                    }
                                    .to_bytes();

                                    // Convert Vec<u8> into Bytes for game_sockets
                                    let _ = network.peer.send(
                                        &client_uuid.into(),
                                        &stream,
                                        Bytes::from(out_msg),
                                    );
                                }
                            }
                        }
                    }

                    BrokerMessage::ClientInput {
                        mut client_id,
                        input,
                    } => {
                        // THE HANDSHAKE INTERCEPT
                        if client_id == 0 {
                            if let Some(&real_id) = state.uuid_to_id.get(&connection.connection_id)
                            {
                                client_id = real_id; // Swap to real ID for forwarding

                                let welcome = ServerMessage::Welcome { player_id: real_id };
                                if let Ok(bincode_payload) = bincode::serialize(&welcome) {
                                    let welcome_msg = BrokerMessage::Broadcast {
                                        payload: bincode_payload,
                                    }
                                    .to_bytes();
                                    let _ = network.peer.send(
                                        &connection,
                                        &stream,
                                        Bytes::from(welcome_msg),
                                    );
                                }

                                let default_topic = string_to_topic("shard:0");
                                state.client_to_topic.insert(real_id, default_topic);
                                state
                                    .topic_subscribers
                                    .entry(default_topic)
                                    .or_default()
                                    .insert(real_id);

                                println!(
                                    "Intercepted Join. Assigned ID: {}. Spawned in shard:0",
                                    real_id
                                );
                            }
                        }

                        // ROUTE TO SHARD
                        if let Some(topic) = state.client_to_topic.get(&client_id) {
                            if let Some(&shard_uuid) = state.topic_to_shard.get(topic) {
                                let forward_msg =
                                    BrokerMessage::ClientInput { client_id, input }.to_bytes();
                                let _ = network.peer.send(
                                    &shard_uuid.into(),
                                    &stream,
                                    Bytes::from(forward_msg),
                                );
                            }
                        }
                    }

                    BrokerMessage::PositionUpdate { .. } => {
                        // Ignoring the 0x10 tag for now
                    }

                    _ => {}
                }
            }
            _ => {}
        }
    }
}
