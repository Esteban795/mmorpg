use std::collections::HashSet;

use crate::network::BrokerNetwork;
use crate::state::BrokerState;
use crate::state::Topic;
use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameConnection,GameNetworkEvent, GameStream};
use shared::broker_protocol::{
    BrokerMessage, TAG_CLIENT_TYPE_CHAT_SERVICE, TAG_CLIENT_TYPE_CLIENT,
    TAG_CLIENT_TYPE_SPATIAL_SERVER, string_to_topic, topic_to_string,
};
use shared::{ClientMessage, ServerMessage};
use tracing::{info, warn};

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
                handle_disconnection(&mut state, &mut network, conn);
            }

            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                let buffer = if stream.is_reliable() {
                    state
                        .connection_reliable_buffers
                        .entry(connection.connection_id)
                        .or_default()
                } else {
                    state
                        .connection_unreliable_buffers
                        .entry(connection.connection_id)
                        .or_default()
                };

                buffer.extend_from_slice(&data);
                let messages = BrokerMessage::parse_multiple(buffer);

                if messages.is_empty() {
                    warn!(
                        "[BROKER] Malformed or incomplete message from UUID {:?}, ignoring.",
                        connection.connection_id
                    );
                    continue;
                }

                handle_broker_messages(&mut state, &mut network, connection, &stream, messages);
            }
            _ => {
                warn!("[BROKER] Received unsupported network event: {:?}", event);
            }
        }
    }
}

fn handle_connected(
    state: &mut BrokerState,
    connection: &GameConnection,
    stream: &GameStream,
    client_id: u32,
    client_type: u8,
) {
    info!(
        "[BROKER] Client {} of type {} connected with UUID {:?}.",
        client_id, client_type, connection.connection_id
    );

    match client_type {
        TAG_CLIENT_TYPE_CLIENT => {}
        TAG_CLIENT_TYPE_SPATIAL_SERVER => {
            if state.spatial_server_uuid.is_none() {
                // If this is the first subscription from the spatial server, register its UUID for direct routing of position updates.
                state.spatial_server_uuid = Some(connection.connection_id);
                info!(
                    "[BROKER] Registered spatial server UUID {:?}",
                    connection.connection_id
                );
            }
        }
        TAG_CLIENT_TYPE_CHAT_SERVICE => {
            info!(
                "[BROKER] Chat service connected with UUID {:?}",
                connection.connection_id
            );
            if state.chat_server_uuid.is_none() {
                state.chat_server_uuid = Some(connection.connection_id);
                info!(
                    "[BROKER] Registered chat server UUID {:?}",
                    connection.connection_id
                );
                // Add stream
                state
                    .connection_reliable_streams
                    .insert(connection.connection_id, stream.clone());
            }

            // Create an entry for people to subscribe to the chat topic
            let chat_topic = string_to_topic("chat");
            state.topic_subscribers.insert(chat_topic, HashSet::new());
        }
        _ => {
            warn!(
                "[BROKER] Unknown client type {} connected with UUID {:?}.",
                client_type, connection.connection_id
            );
        }
    }
}

fn handle_disconnection(
    state: &mut BrokerState,
    network: &mut BrokerNetwork,
    conn: GameConnection,
) {
    // Cleanup disconnected clients
    state
        .connection_reliable_streams
        .remove(&conn.connection_id);
    state
        .connection_unreliable_streams
        .remove(&conn.connection_id);

    // Remove any buffers associated with this connection to free memory
    state
        .connection_reliable_buffers
        .remove(&conn.connection_id);
    state
        .connection_unreliable_buffers
        .remove(&conn.connection_id);

    if let Some(id) = state.uuid_to_id.remove(&conn.connection_id) {
        state.id_to_uuid.remove(&id);

        // Notify every shard the client was routed to (covers both normal
        // operation and mid-handoff disconnects where two shards are active).
        if let Some(topics) = state.client_topics.remove(&id) {
            for topic in &topics {
                //Inform the shard of the disconnect so it can remove the player from the AOI and broadcast the update to other clients.
                if let Some(&shard_uuid) = state.topic_to_shard.get(topic) {
                    if let Some(shard_stream) = state.connection_reliable_streams.get(&shard_uuid) {
                        let disconnect_msg = ClientMessage::Disconnect;
                        // Pad the disconnect message to 16 bytes to fit the broker protocol's ClientInput struct, even though the payload is empty for Disconnect.
                        if let Ok(input_bytes) = bincode::serialize(&disconnect_msg) {
                            let mut input_array = [0u8; 16];
                            let len = input_bytes.len().min(16);
                            input_array[..len].copy_from_slice(&input_bytes[..len]);

                            let forward_msg = BrokerMessage::ClientInput {
                                client_id: id,
                                input: input_array,
                            }
                            .to_bytes();

                            let _ = network.peer.send(
                                &shard_uuid.into(),
                                shard_stream,
                                Bytes::from(forward_msg),
                            );
                        }
                    }
                }

                if let Some(subs) = state.topic_subscribers.get_mut(topic) {
                    subs.remove(&id);
                }
            }

            if let Some(spatial_server_uuid) = state.spatial_server_uuid {
                if let Some(spatial_stream) =
                    state.connection_reliable_streams.get(&spatial_server_uuid)
                {
                    let disconnect_msg =
                        BrokerMessage::PlayerDisconnected { client_id: id }.to_bytes();
                    let _ = network.peer.send(
                        &spatial_server_uuid.into(),
                        spatial_stream,
                        Bytes::from(disconnect_msg),
                    );
                }
            }
        }
        info!(
            "[BROKER] Client ID {} (UUID {:?}) disconnected and cleaned up.",
            id, conn.connection_id
        );
    }
}

fn handle_client_input(
    state: &mut BrokerState,
    network: &mut BrokerNetwork,
    connection: &GameConnection,
    stream: &GameStream,
    mut client_id: u32,
    input: [u8; 16],
) {
    // THE HANDSHAKE INTERCEPT (to assign an actual Client ID on Join and spawn them in a default shard/topic)
    if client_id == 0 {
        // Check if its a join message
        if let Ok(ClientMessage::Join { username }) = bincode::deserialize::<ClientMessage>(&input)
        {
            if let Some(&real_id) = state.uuid_to_id.get(&connection.connection_id) {
                client_id = real_id;

                let welcome = ServerMessage::Welcome { player_id: real_id };
                if let Ok(bincode_payload) = bincode::serialize(&welcome) {
                    let welcome_msg = BrokerMessage::Broadcast {
                        payload: bincode_payload,
                    }
                    .to_bytes();
                    // Welcome reply on the same connection / stream as the Join.
                    let _ = network
                        .peer
                        .send(&connection, &stream, Bytes::from(welcome_msg));
                }

                //Assign the new player to a default shard/topic for now as a spawn point.
                let default_topic = string_to_topic(&format!("shard:{}", state.default_shard_id));
                state
                    .client_topics
                    .entry(real_id)
                    .or_default()
                    .insert(default_topic);
                state
                    .topic_subscribers
                    .entry(default_topic)
                    .or_default()
                    .insert(real_id);

                // Subscribe the new client to the chat topic to receive global chat messages.
                let chat_topic = string_to_topic("chat");
                state
                    .client_topics
                    .entry(real_id)
                    .or_default()
                    .insert(chat_topic);

                state
                    .topic_subscribers
                    .entry(chat_topic)
                    .or_default()
                    .insert(real_id);

                info!(
                    "[BROKER] Intercepted Join — assigned ID {} to UUID {:?}, spawned in shard:{}.",
                    real_id, connection.connection_id, state.default_shard_id
                );

                // Forward join message to the chat server
                let mut username_array = [0u8; 32];
                let bytes = &username;
                let len = bytes.len().min(32);
                username_array[..len].copy_from_slice(&bytes[..len]);

                let chat_join_msg = BrokerMessage::ChatJoin {
                    client_id: real_id,
                    username: username_array,
                };

                if let Some(chat_uuid) = state.chat_server_uuid {
                    if let Some(chat_stream) = state.connection_reliable_streams.get(&chat_uuid) {
                        if let Err(e) = network.peer.send(
                            &chat_uuid.into(),
                            chat_stream,
                            Bytes::from(chat_join_msg.to_bytes()),
                        ) {
                            warn!(
                                "[BROKER] Failed to notify Chat Service of new player: {:?}",
                                e
                            );
                        } else {
                            info!(
                                "[BROKER] Sent ChatJoin to Chat Service for client {}",
                                real_id
                            );
                        }
                    }
                } else {
                    warn!(
                        "[BROKER] Chat service is not connected, cannot send ChatJoin for client {}",
                        real_id
                    );
                }
            }
        } else {
            // Wake-up MoveInput or other message with id == 0 before Welcome:
            // resolve the real id from the UUID so the forward below still works.
            if let Some(&real_id) = state.uuid_to_id.get(&connection.connection_id) {
                client_id = real_id;
            }
        }
    }

    // ROUTE INPUT TO THE CORRECT SHARD AND TO THE NEIGHBORING SHARDS IF NECESSARY (for handoff during crossing)
    //Get owning sharrd/topic for this client
    let topics: Vec<Topic> = state
        .client_topics
        .get(&client_id)
        .map(|s| s.iter().copied().collect())
        .unwrap_or_default();

    for topic in topics {
        if let Some(&shard_uuid) = state.topic_to_shard.get(&topic) {
            //use the SHARD's own stream, matching the reliability of the
            // incoming client stream.
            let shard_stream_opt: Option<&GameStream> = if stream.is_reliable() {
                state.connection_reliable_streams.get(&shard_uuid)
            } else {
                state.connection_unreliable_streams.get(&shard_uuid)
            };

            if let Some(shard_stream) = shard_stream_opt {
                let forward_msg = BrokerMessage::ClientInput { client_id, input }.to_bytes();
                let _ =
                    network
                        .peer
                        .send(&shard_uuid.into(), shard_stream, Bytes::from(forward_msg));
            } else {
                warn!(
                    "[BROKER] No stream for shard {:?}, client {} input dropped.",
                    topic_to_string(&topic),
                    client_id
                );
            }
        }
    }
}

fn handle_publish(
    state: &mut BrokerState,
    network: &mut BrokerNetwork,
    connection: GameConnection,
    topic: [u8; 32],
    payload: Vec<u8>,
) {
    // Register Shard
    state.topic_to_shard.insert(topic, connection.connection_id);

    // Broadcast to clients
    if let Some(subscribers) = state.topic_subscribers.get(&topic) {
        let _count = subscribers.len();

        for client_id in subscribers.iter().copied() {
            // Look up the client's UUID
            if let Some(&client_uuid) = state.id_to_uuid.get(&client_id) {
                // Retrieve the client unreliable stream
                if let Some(client_stream) = state.connection_unreliable_streams.get(&client_uuid) {
                    let out_msg = BrokerMessage::Broadcast {
                        payload: payload.clone(),
                    }
                    .to_bytes();
                    //Send the AOI update to the client on the unreliable stream.
                    let _ =
                        network
                            .peer
                            .send(&client_uuid.into(), client_stream, Bytes::from(out_msg));
                } else {
                    warn!(
                        "[BROKER] No unreliable stream yet for client {} (UUID {:?}). AOI dropped.",
                        client_id, client_uuid
                    );
                }
            }
        }
    }
}

fn handle_crossing_alert(
    state: &mut BrokerState,
    network: &mut BrokerNetwork,
    client_id: u32,
    dest_authority_topic: Topic,
    neighbor_topic: Topic,
) {
    info!(
        "[BROKER] CrossingAlert — client {} crossing from {} into {}",
        client_id,
        topic_to_string(&dest_authority_topic),
        topic_to_string(&neighbor_topic)
    );
    // Forward to the authority shard
    if let Some(&auth_uuid) = state.topic_to_shard.get(&dest_authority_topic) {
        if let Some(rel_stream) = state.connection_reliable_streams.get(&auth_uuid) {
            let msg = BrokerMessage::CrossingAlert {
                client_id,
                dest_authority_topic,
                neighbor_topic,
            }
            .to_bytes();
            let _ = network
                .peer
                .send(&auth_uuid.into(), rel_stream, Bytes::from(msg));
        } else {
            warn!(
                "[BROKER] No reliable stream for authority shard {:?}",
                topic_to_string(&dest_authority_topic)
            );
        }
    } else {
        warn!(
            "[BROKER] CrossingAlert: no shard registered for dest_authority_topic {:?}",
            topic_to_string(&dest_authority_topic)
        );
    }

    // Subscribe neighbor shard to this client's inputs
    //  (neighbor starts receiving movement so it can pre-simulate the arriving entity)
    state
        .client_topics
        .entry(client_id)
        .or_default()
        .insert(neighbor_topic);

    // Subscribe client to neighbor shard's broadcasts
    // (client starts seeing AOI from the new shard before the authority fully hands off)
    state
        .topic_subscribers
        .entry(neighbor_topic)
        .or_default()
        .insert(client_id);
}

fn handle_crossing_exit(
    state: &mut BrokerState,
    network: &mut BrokerNetwork,
    client_id: u32,
    obsolete_auth_topic: Topic,
    new_auth_topic: Topic,
) {
    info!(
        "[BROKER] CrossingExit — client {} exiting {} (now obsolete authority) into {}",
        client_id,
        topic_to_string(&obsolete_auth_topic),
        topic_to_string(&new_auth_topic)
    );
    // Unsubscribe client from obsolete shard/topic
    if let Some(subs) = state.topic_subscribers.get_mut(&obsolete_auth_topic) {
        subs.remove(&client_id);
    }

    // Remove the obsolete topic from the client's subscribed topics
    if let Some(topics) = state.client_topics.get_mut(&client_id) {
        topics.remove(&obsolete_auth_topic);
    }

    // Notify the obsolete shard about the client's exit
    if let Some(&obsolete_uuid) = state.topic_to_shard.get(&obsolete_auth_topic) {
        if let Some(rel_stream) = state.connection_reliable_streams.get(&obsolete_uuid) {
            let msg = BrokerMessage::CrossingExit {
                client_id,
                obsolete_auth_topic,
                new_auth_topic,
            }
            .to_bytes();
            let _ = network
                .peer
                .send(&obsolete_uuid.into(), rel_stream, Bytes::from(msg));
        } else {
            warn!(
                "[BROKER] No reliable stream for obsolete authority shard {:?}",
                topic_to_string(&obsolete_auth_topic)
            );
        }
    }

    if let Some(&new_auth_uuid) = state.topic_to_shard.get(&new_auth_topic) {
        // Notify the new authority shard about the client's exit from the old shard (could be used for cleanup, AOI updates, etc.)
        if let Some(rel_stream) = state.connection_reliable_streams.get(&new_auth_uuid) {
            let msg = BrokerMessage::CrossingExit {
                client_id,
                obsolete_auth_topic,
                new_auth_topic,
            }
            .to_bytes();
            let _ = network
                .peer
                .send(&new_auth_uuid.into(), rel_stream, Bytes::from(msg));
        } else {
            warn!(
                "[BROKER] No reliable stream for new authority shard {:?}",
                topic_to_string(&new_auth_topic)
            );
        }
    }
}

pub fn handle_broker_messages(
    state: &mut BrokerState,
    network: &mut BrokerNetwork,
    connection: GameConnection,
    stream: &GameStream,
    broker_messages: Vec<BrokerMessage>,
) {
    // Creates a new buffer to send to the spatial server the same way it received the data
    let mut spatial_batch = Vec::new();

    for msg in broker_messages {
        match msg {
            BrokerMessage::Connected {
                client_id,
                client_type,
            } => {
                handle_connected(state, &connection, stream, client_id, client_type);
            }

            BrokerMessage::Subscribe { client_id, topic } => {
                state
                    .topic_subscribers
                    .entry(topic)
                    .or_default()
                    .insert(client_id);
                state
                    .client_topics
                    .entry(client_id)
                    .or_default()
                    .insert(topic);
                info!("[BROKER] Client {} subscribed to topic.", client_id);
            }

            BrokerMessage::Unsubscribe { client_id, topic } => {
                if let Some(subs) = state.topic_subscribers.get_mut(&topic) {
                    subs.remove(&client_id);
                }
                if let Some(topics) = state.client_topics.get_mut(&client_id) {
                    topics.remove(&topic);
                }
                info!("[BROKER] Client {} unsubscribed.", client_id);
            }

            // Shards publish AOI updates to the Broker with the "Publish" message, and the Broker forwards them to all subscribed clients.
            BrokerMessage::Publish { topic, payload } => {
                handle_publish(state, network, connection, topic, payload);
            }

            // Receive PublishReliable message from shard and forward it to subscribers like a normal Publish,
            // but using the reliable stream instead of the unreliable one.
            // Used for critical updates that must not be dropped, like shard-to-shard handoff coordination messages.
            BrokerMessage::PublishReliable { topic, payload } => {
                if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                    let out_msg = BrokerMessage::Broadcast {
                        payload: payload.clone(),
                    }
                    .to_bytes();
                    let bytes = Bytes::from(out_msg);

                    for client_id in subscribers.iter().copied() {
                        if let Some(&client_uuid) = state.id_to_uuid.get(&client_id) {
                            if let Some(client_stream) =
                                state.connection_reliable_streams.get(&client_uuid)
                            {
                                let _ = network.peer.send(
                                    &client_uuid.into(),
                                    client_stream,
                                    bytes.clone(),
                                );
                            }
                        }
                    }
                }
            }

            // DirectMessageReliable is used for direct shard-to-client messages that must not be dropped, like critical state updates during handoff.
            // The Broker forwards the message directly to the specified client without going through the topic subscription system.
            // Used to send specific messages to a specific client whithout overloading other clients
            // (eg: when a client joins, the shard sends all the food Data to the player then reliable broadcast events updates them)
            BrokerMessage::DirectMessageReliable { client_id, payload } => {
                if let Some(&client_uuid) = state.id_to_uuid.get(&client_id) {
                    if let Some(client_stream) = state.connection_reliable_streams.get(&client_uuid)
                    {
                        let out_msg = BrokerMessage::Broadcast { payload }.to_bytes();
                        let _ = network.peer.send(
                            &client_uuid.into(),
                            client_stream,
                            Bytes::from(out_msg),
                        );
                    }
                }
            }

            BrokerMessage::ClientInput { client_id, input } => {
                handle_client_input(state, network, &connection, stream, client_id, input);
            }

            //Position updates from shards to the spatial server.
            BrokerMessage::PositionUpdate {
                client_id,
                x,
                y,
                score,
            } => {
                let forward_msg = BrokerMessage::PositionUpdate {
                    client_id,
                    x,
                    y,
                    score,
                }
                .to_bytes();
                spatial_batch.extend_from_slice(&forward_msg);
            }

            BrokerMessage::CrossingAlert {
                client_id,
                dest_authority_topic,
                neighbor_topic,
            } => {
                handle_crossing_alert(
                    state,
                    network,
                    client_id,
                    dest_authority_topic,
                    neighbor_topic,
                );
            }

            BrokerMessage::InterShardMessage {
                topic_dest,
                topic_from,
                payload,
            } => {
                // Simple inter-shard messaging forwarded by the Broker, used for shard-to-shard handoff coordination for now but could be used for other cross-shard communication in the future.
                if let Some(&dest_uuid) = state.topic_to_shard.get(&topic_dest) {
                    // Use a reliable stream since these messages are currently only used for handoff coordination, which is critical to get right.
                    if let Some(rel_stream) = state.connection_reliable_streams.get(&dest_uuid) {
                        let msg = BrokerMessage::InterShardMessage {
                            topic_dest,
                            topic_from,
                            payload,
                        }
                        .to_bytes();
                        let _ = network
                            .peer
                            .send(&dest_uuid.into(), rel_stream, Bytes::from(msg));
                    } else {
                        warn!(
                            "[BROKER] No reliable stream for destination shard {:?}",
                            topic_to_string(&topic_dest)
                        );
                    }
                } else {
                    warn!(
                        "[BROKER] InterShardMessage: no shard registered for topic_dest {:?}",
                        topic_to_string(&topic_dest)
                    );
                }
            }

            //Message relayed from spatial server to old_auth_topic.
            BrokerMessage::AuthoritySwitch {
                client_id,
                old_auth_topic,
                new_auth_topic,
            } => {
                info!(
                    "[BROKER] AuthoritySwitch — client {} authority moving from {} to {}",
                    client_id,
                    topic_to_string(&old_auth_topic),
                    topic_to_string(&new_auth_topic)
                );
                if let Some(&old_auth_uuid) = state.topic_to_shard.get(&old_auth_topic) {
                    if let Some(rel_stream) = state.connection_reliable_streams.get(&old_auth_uuid)
                    {
                        let msg = BrokerMessage::AuthoritySwitch {
                            client_id,
                            old_auth_topic,
                            new_auth_topic,
                        }
                        .to_bytes();
                        let _ =
                            network
                                .peer
                                .send(&old_auth_uuid.into(), rel_stream, Bytes::from(msg));
                    } else {
                        warn!(
                            "[BROKER] No reliable stream for old authority shard {:?}",
                            topic_to_string(&old_auth_topic)
                        );
                    }
                } else {
                    warn!(
                        "[BROKER] AuthoritySwitch: no shard registered for old_auth_topic {:?}",
                        topic_to_string(&old_auth_topic)
                    );
                }
            }

            BrokerMessage::CrossingExit {
                client_id,
                obsolete_auth_topic,
                new_auth_topic,
            } => {
                handle_crossing_exit(
                    state,
                    network,
                    client_id,
                    obsolete_auth_topic,
                    new_auth_topic,
                );
            }

            BrokerMessage::ShardReady { shard_id } => {
                info!("[BROKER] Shard with UUID {:?} reports ready.", shard_id);

                if let Some(spatial_uuid) = state.spatial_server_uuid {
                    if let Some(rel_stream) = state.connection_reliable_streams.get(&spatial_uuid) {
                        let msg = BrokerMessage::ShardReady { shard_id }.to_bytes();
                        let _ =
                            network
                                .peer
                                .send(&spatial_uuid.into(), rel_stream, Bytes::from(msg));
                    } else {
                        warn!(
                            "[BROKER] No reliable stream for spatial server to report shard ready."
                        );
                    }
                } else {
                    warn!("[BROKER] No spatial server UUID registered yet to report shard ready.");
                }
            }

            BrokerMessage::NewSpawnShard { new_shard_id } => {
                info!(
                    "[BROKER] Shard with ID {:?} becomes the new spawn shard.",
                    new_shard_id
                );
                state.default_shard_id = new_shard_id;
            }

            BrokerMessage::ClientChatMessage { client_id, msg } => {
                info!("[BROKER] Received chat message from client {}", client_id);
                if let Some(chat_uuid) = state.chat_server_uuid {
                    if let Some(rel_stream) = state.connection_reliable_streams.get(&chat_uuid) {
                        let forward_msg =
                            BrokerMessage::ClientChatMessage { client_id, msg }.to_bytes();
                        let _ = network.peer.send(
                            &chat_uuid.into(),
                            rel_stream,
                            Bytes::from(forward_msg),
                        );
                    } else {
                        warn!(
                            "[BROKER] No reliable stream for chat service to forward client chat message."
                        );
                    }
                } else {
                    warn!(
                        "[BROKER] No chat server UUID registered yet to forward client chat message."
                    )
                }
            }

            BrokerMessage::BroadcastChatMessage { username, msg } => {
                info!(
                    "[BROKER] Broadcasting chat message from client {:?} to all subscribers",
                    username
                );
                // Broadcast the chat message to all subscribed clients
                if let Some(subscribers) = state.topic_subscribers.get(&string_to_topic("chat")) {
                    for subscriber_id in subscribers.iter().copied() {
                        if let Some(&subscriber_uuid) = state.id_to_uuid.get(&subscriber_id) {
                            if let Some(rel_stream) =
                                state.connection_reliable_streams.get(&subscriber_uuid)
                            {
                                let forward_msg =
                                    BrokerMessage::BroadcastChatMessage { username, msg }
                                        .to_bytes();
                                let _ = network.peer.send(
                                    &subscriber_uuid.into(),
                                    rel_stream,
                                    Bytes::from(forward_msg),
                                );
                            }
                        }
                    }
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

    if !spatial_batch.is_empty() {
        if let Some(spatial_uuid) = state.spatial_server_uuid {
            if let Some(spatial_stream) = state.connection_unreliable_streams.get(&spatial_uuid) {
                match network.peer.send(
                    &spatial_uuid.into(),
                    spatial_stream,
                    Bytes::from(spatial_batch),
                ) {
                    Ok(_) => {
                        //debug!("[BROKER] Forwarded batch of {} position updates to spatial server.", pos_updates_count );
                    }
                    Err(e) => {
                        warn!(
                            "[BROKER] Failed to forward batched position updates: {:?}",
                            e
                        );
                    }
                }
            }
        }
    }
}
