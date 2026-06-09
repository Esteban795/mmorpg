use crate::network::BrokerNetwork;
use crate::state::BrokerState;
use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameNetworkEvent, GameStream};
use shared::broker_protocol::{BrokerMessage, string_to_topic};
use shared::{ClientMessage, ServerMessage};
use tracing::{debug, info, warn};

pub const DEFAULT_TOPIC: &str = "shard:0";

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

                info!(
                    "[BROKER] New connection: UUID {:?} assigned ID {}",
                    conn.connection_id, new_id
                );
            }

            //Register each connection's streams here so we can use them for outgoing sends.
            GameNetworkEvent::StreamCreated(conn, stream) => {
                if stream.is_reliable() {
                    state
                        .connection_reliable_streams
                        .insert(conn.connection_id, stream);
                    info!(
                        "[BROKER] Reliable stream registered for UUID {:?}",
                        conn.connection_id
                    );
                } else {
                    state
                        .connection_unreliable_streams
                        .insert(conn.connection_id, stream);
                    info!(
                        "[BROKER] Unreliable stream registered for UUID {:?}",
                        conn.connection_id
                    );
                }
            }

            GameNetworkEvent::Disconnected(conn) => {
                // Cleanup disconnected clients
                state
                    .connection_reliable_streams
                    .remove(&conn.connection_id);
                state
                    .connection_unreliable_streams
                    .remove(&conn.connection_id);

                if let Some(id) = state.uuid_to_id.remove(&conn.connection_id) {
                    state.id_to_uuid.remove(&id);
                    if let Some(topic) = state.client_to_topic.remove(&id) {
                        if let Some(subs) = state.topic_subscribers.get_mut(&topic) {
                            subs.remove(&id);
                        }
                    }
                    info!(
                        "[BROKER] Client ID {} (UUID {:?}) disconnected and cleaned up.",
                        id, conn.connection_id
                    );
                }
            }

            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let Some(msg) = BrokerMessage::from_bytes(&data) else {
                    warn!(
                        "[BROKER] Malformed message from UUID {:?}, ignoring.",
                        connection.connection_id
                    );
                    continue;
                };

                match msg {
                    BrokerMessage::Subscribe { client_id, topic } => {
                        if state.spatial_server_uuid.is_none() {
                            // If this is the first subscription from the spatial server, register its UUID for direct routing of position updates.
                            state.spatial_server_uuid = Some(connection.connection_id);
                            info!(
                                "[BROKER] Registered spatial server UUID {:?}",
                                connection.connection_id
                            );
                        }

                        state
                            .topic_subscribers
                            .entry(topic)
                            .or_default()
                            .insert(client_id);
                        state.client_to_topic.insert(client_id, topic);
                        info!("[BROKER] Client {} subscribed to topic.", client_id);
                    }

                    BrokerMessage::Unsubscribe { client_id, topic } => {
                        if let Some(subs) = state.topic_subscribers.get_mut(&topic) {
                            subs.remove(&client_id);
                        }
                        state.client_to_topic.remove(&client_id);
                        info!("[BROKER] Client {} unsubscribed.", client_id);
                    }

                    // Shards publish AOI updates to the Broker with the "Publish" message, and the Broker forwards them to all subscribed clients.
                    BrokerMessage::Publish { topic, payload } => {
                        // Register Shard
                        state.topic_to_shard.insert(topic, connection.connection_id);

                        // Broadcast to clients
                        if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                            let subscriber_ids: Vec<u32> = subscribers.iter().copied().collect();
                            let count = subscriber_ids.len();

                            for client_id in subscriber_ids {
                                // Look up the client's UUID
                                if let Some(&client_uuid) = state.id_to_uuid.get(&client_id) {
                                    // Retrieve the client unreliable stream
                                    if let Some(client_stream) =
                                        state.connection_unreliable_streams.get(&client_uuid)
                                    {
                                        let out_msg = BrokerMessage::Broadcast {
                                            payload: payload.clone(),
                                        }
                                        .to_bytes();
                                        //Send the AOI update to the client on the unreliable stream.
                                        let _ = network.peer.send(
                                            &client_uuid.into(),
                                            client_stream,
                                            Bytes::from(out_msg),
                                        );
                                    } else {
                                        warn!(
                                            "[BROKER] No unreliable stream yet for client {} (UUID {:?}). AOI dropped.",
                                            client_id, client_uuid
                                        );
                                    }
                                }
                            }
                            debug!("[BROKER] Published AOI to {} subscriber(s).", count);
                        }
                    }

                    BrokerMessage::ClientInput {
                        mut client_id,
                        input,
                    } => {
                        // THE HANDSHAKE INTERCEPT (to assign an actual Client ID on Join and spawn them in a default shard/topic)
                        if client_id == 0 {
                            let is_join = bincode::deserialize::<ClientMessage>(&input)
                                .map(|m| matches!(m, ClientMessage::Join { .. }))
                                .unwrap_or(false);

                            if is_join {
                                if let Some(&real_id) =
                                    state.uuid_to_id.get(&connection.connection_id)
                                {
                                    client_id = real_id;

                                    let welcome = ServerMessage::Welcome { player_id: real_id };
                                    if let Ok(bincode_payload) = bincode::serialize(&welcome) {
                                        let welcome_msg = BrokerMessage::Broadcast {
                                            payload: bincode_payload,
                                        }
                                        .to_bytes();
                                        // Welcome reply on the same connection / stream as the Join.
                                        let _ = network.peer.send(
                                            &connection,
                                            &stream,
                                            Bytes::from(welcome_msg),
                                        );
                                    }

                                    //Assign the new player to a default shard/topic for now as a spawn point.
                                    let default_topic = string_to_topic(DEFAULT_TOPIC);
                                    state.client_to_topic.insert(real_id, default_topic);
                                    state
                                        .topic_subscribers
                                        .entry(default_topic)
                                        .or_default()
                                        .insert(real_id);

                                    info!(
                                        "[BROKER] Intercepted Join — assigned ID {} to UUID {:?}, spawned in shard:0.",
                                        real_id, connection.connection_id
                                    );
                                }
                            } else {
                                // Wake-up MoveInput or other message with id == 0 before Welcome:
                                // resolve the real id from the UUID so the forward below still works.
                                if let Some(&real_id) =
                                    state.uuid_to_id.get(&connection.connection_id)
                                {
                                    client_id = real_id;
                                }
                            }
                        }

                        // ROUTE INPUT TO THE CORRECT SHARD
                        if let Some(&topic) = state.client_to_topic.get(&client_id) {
                            if let Some(&shard_uuid) = state.topic_to_shard.get(&topic) {
                                //use the SHARD's own stream, matching the reliability of the
                                // incoming client stream.
                                let shard_stream_opt: Option<&GameStream> = if stream.is_reliable()
                                {
                                    state.connection_reliable_streams.get(&shard_uuid)
                                } else {
                                    state.connection_unreliable_streams.get(&shard_uuid)
                                };

                                if let Some(shard_stream) = shard_stream_opt {
                                    let forward_msg =
                                        BrokerMessage::ClientInput { client_id, input }.to_bytes();
                                    let _ = network.peer.send(
                                        &shard_uuid.into(),
                                        shard_stream,
                                        Bytes::from(forward_msg),
                                    );
                                } else {
                                    warn!(
                                        "[BROKER] No shard stream registered yet for client {} input. Dropped (shard not connected yet?).",
                                        client_id
                                    );
                                }
                            }
                        }
                    }

                    //Position updates from shards to the spatial server.
                    BrokerMessage::PositionUpdate { client_id, x, y } => {
                        if let Some(spatial_uuid) = state.spatial_server_uuid {
                            if let Some(spatial_stream) =
                                state.connection_unreliable_streams.get(&spatial_uuid)
                            {
                                let forward_msg =
                                    BrokerMessage::PositionUpdate { client_id, x, y }.to_bytes();
                                let _ = network.peer.send(
                                    &spatial_uuid.into(),
                                    spatial_stream,
                                    Bytes::from(forward_msg),
                                );
                            } else {
                                warn!(
                                    "[BROKER] No spatial server stream registered yet for position update. Dropped (spatial server not connected yet?)."
                                );
                            }
                        } else {
                            warn!(
                                "[BROKER] No spatial server UUID registered yet for position update. Dropped (spatial server not connected yet?)."
                            );
                        }
                    }

                    _ => {
                        warn!(
                            "[BROKER] Received unsupported message type from UUID {:?}. Ignoring.",
                            connection.connection_id
                        );
                    }
                }
            }
            _ => {
                warn!("[BROKER] Received unsupported network event: {:?}", event);
            }
        }
    }
}
