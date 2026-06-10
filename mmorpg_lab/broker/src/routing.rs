use crate::network::BrokerNetwork;
use crate::state::Topic;
use crate::state::{BrokerDiagnostics, BrokerState};
use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameNetworkEvent, GameStream};
use shared::broker_protocol::{BrokerMessage, string_to_topic, topic_to_string};
use shared::{ClientMessage, ServerMessage};
use tracing::{debug, info, warn};

pub const DEFAULT_TOPIC: &str = "shard:0";

pub fn process_network_events(
    mut network: ResMut<BrokerNetwork>,
    mut state: ResMut<BrokerState>,
    mut diagnostics: ResMut<BrokerDiagnostics>,
) {
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
                state.connection_buffers.remove(&conn.connection_id);

                if let Some(id) = state.uuid_to_id.remove(&conn.connection_id) {
                    state.id_to_uuid.remove(&id);

                    //If the client was subscribed to a shard/topic
                    if let Some(topic) = state.client_to_topic.remove(&id) {
                        //Inform the shard of the disconnect so it can remove the player from the AOI and broadcast the update to other clients.
                        if let Some(&shard_uuid) = state.topic_to_shard.get(&topic) {
                            if let Some(shard_stream) =
                                state.connection_reliable_streams.get(&shard_uuid)
                            {
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
                let buffer = state
                    .connection_buffers
                    .entry(connection.connection_id)
                    .or_default();

                buffer.extend_from_slice(&data);
                let messages = BrokerMessage::parse_multiple(buffer);

                if messages.is_empty() {
                    warn!(
                        "[BROKER] Malformed or incomplete message from UUID {:?}, ignoring.",
                        connection.connection_id
                    );
                    continue;
                }

                for msg in messages {
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
                            diagnostics.aoi_publishes_received += 1;

                            // Register Shard
                            state.topic_to_shard.insert(topic, connection.connection_id);

                            // Broadcast to clients
                            if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                                let _count = subscribers.len();

                                for client_id in subscribers.iter().copied() {
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
                                            diagnostics.aoi_broadcasts_sent += 1;
                                        } else {
                                            warn!(
                                                "[BROKER] No unreliable stream yet for client {} (UUID {:?}). AOI dropped.",
                                                client_id, client_uuid
                                            );
                                        }
                                    }
                                }
                                // Commented to reduce log noise - AOI publishing is verified by [NETWORK-20Hz] logs
                                // debug!("[BROKER] Published AOI to {} subscriber(s).", count);
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

                            // ROUTE INPUT TO THE CORRECT SHARD AND TO THE NEIGHBORING SHARDS IF NECESSARY (for handoff during crossing)
                            //Get owning sharrd/topic for this client
                            let primary = state.client_to_topic.get(&client_id).copied();
                            //Get neighboring shards/topics for this client (if any)
                            let extras: Vec<Topic> = state
                                .client_handoff_topics
                                .get(&client_id)
                                .map(|s| s.iter().copied().collect())
                                .unwrap_or_default();

                            for topic in primary.into_iter().chain(extras) {
                                if let Some(&shard_uuid) = state.topic_to_shard.get(&topic) {
                                    //use the SHARD's own stream, matching the reliability of the
                                    // incoming client stream.
                                    let shard_stream_opt: Option<&GameStream> =
                                        if stream.is_reliable() {
                                            state.connection_reliable_streams.get(&shard_uuid)
                                        } else {
                                            state.connection_unreliable_streams.get(&shard_uuid)
                                        };

                                    if let Some(shard_stream) = shard_stream_opt {
                                        let forward_msg =
                                            BrokerMessage::ClientInput { client_id, input }
                                                .to_bytes();
                                        let _ = network.peer.send(
                                            &shard_uuid.into(),
                                            shard_stream,
                                            Bytes::from(forward_msg),
                                        );
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

                        //Position updates from shards to the spatial server.
                        BrokerMessage::PositionUpdate { client_id, x, y } => {
                            diagnostics.position_updates_received += 1;

                            if let Some(spatial_uuid) = state.spatial_server_uuid {
                                if let Some(spatial_stream) =
                                    state.connection_unreliable_streams.get(&spatial_uuid)
                                {
                                    let forward_msg =
                                        BrokerMessage::PositionUpdate { client_id, x, y }
                                            .to_bytes();
                                    match network.peer.send(
                                        &spatial_uuid.into(),
                                        spatial_stream,
                                        Bytes::from(forward_msg),
                                    ) {
                                        Ok(_) => {
                                            diagnostics.position_updates_forwarded += 1;
                                            info!(
                                                "[BROKER] Forwarded position update for client {} to spatial server.",
                                                client_id
                                            );
                                        }
                                        Err(e) => {
                                            diagnostics.position_updates_failed += 1;
                                            warn!(
                                                "[BROKER] Failed to forward position update for client {} to spatial server: {:?}",
                                                client_id, e
                                            );
                                        }
                                    }
                                } else {
                                    warn!(
                                        "[BROKER] No spatial server stream registered yet for position update. Dropped (spatial server not connected yet?)."
                                    );
                                    diagnostics.position_updates_failed += 1;
                                }
                            } else {
                                warn!(
                                    "[BROKER] No spatial server UUID registered yet for position update. Dropped (spatial server not connected yet?)."
                                );
                                diagnostics.position_updates_failed += 1;
                            }

                            if diagnostics.position_updates_received % 10 == 0 {
                                debug!(
                                    "[BROKER-DIAG] PositionUpdates: {} received, {} forwarded, {} failed",
                                    diagnostics.position_updates_received,
                                    diagnostics.position_updates_forwarded,
                                    diagnostics.position_updates_failed
                                );
                            }
                        }

                        BrokerMessage::CrossingAlert {
                            client_id,
                            dest_authority_topic,
                            neighbor_topic,
                        } => {
                            info!(
                                "[BROKER] CrossingAlert — client {} crossing from {} into {}",
                                client_id,
                                topic_to_string(&dest_authority_topic),
                                topic_to_string(&neighbor_topic)
                            );
                            // Forward to the authority shard
                            if let Some(&auth_uuid) =
                                state.topic_to_shard.get(&dest_authority_topic)
                            {
                                if let Some(rel_stream) =
                                    state.connection_reliable_streams.get(&auth_uuid)
                                {
                                    let msg = BrokerMessage::CrossingAlert {
                                        client_id,
                                        dest_authority_topic,
                                        neighbor_topic,
                                    }
                                    .to_bytes();
                                    let _ = network.peer.send(
                                        &auth_uuid.into(),
                                        rel_stream,
                                        Bytes::from(msg),
                                    );
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
                                .client_handoff_topics
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

                        BrokerMessage::InterShardMessage {
                            topic_dest,
                            topic_from,
                            payload,
                        } => {
                            // Simple inter-shard messaging forwarded by the Broker, used for shard-to-shard handoff coordination for now but could be used for other cross-shard communication in the future.
                            if let Some(&dest_uuid) = state.topic_to_shard.get(&topic_dest) {
                                // Use a reliable stream since these messages are currently only used for handoff coordination, which is critical to get right.
                                if let Some(rel_stream) =
                                    state.connection_reliable_streams.get(&dest_uuid)
                                {
                                    let msg = BrokerMessage::InterShardMessage {
                                        topic_dest,
                                        topic_from,
                                        payload,
                                    }
                                    .to_bytes();
                                    let _ = network.peer.send(
                                        &dest_uuid.into(),
                                        rel_stream,
                                        Bytes::from(msg),
                                    );
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
                            if let Some(&old_auth_uuid) = state.topic_to_shard.get(&old_auth_topic)
                            {
                                if let Some(rel_stream) =
                                    state.connection_reliable_streams.get(&old_auth_uuid)
                                {
                                    let msg = BrokerMessage::AuthoritySwitch {
                                        client_id,
                                        old_auth_topic,
                                        new_auth_topic,
                                    }
                                    .to_bytes();
                                    let _ = network.peer.send(
                                        &old_auth_uuid.into(),
                                        rel_stream,
                                        Bytes::from(msg),
                                    );
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
                            info!(
                                "[BROKER] CrossingExit — client {} exiting {} (now obsolete authority) into {}",
                                client_id,
                                topic_to_string(&obsolete_auth_topic),
                                topic_to_string(&new_auth_topic)
                            );
                            // Unsubscribe client from obsolete shard/topic
                            if let Some(subs) =
                                state.topic_subscribers.get_mut(&obsolete_auth_topic)
                            {
                                subs.remove(&client_id);
                            }
                            state.client_to_topic.remove(&client_id);

                            // Unsubscribe neighbor shard from this client's inputs
                            if let Some(handoffs) = state.client_handoff_topics.get_mut(&client_id)
                            {
                                handoffs.remove(&obsolete_auth_topic);
                            }
                            if let Some(&obsolete_uuid) =
                                state.topic_to_shard.get(&obsolete_auth_topic)
                            {
                                // Notify the obsolete shard about the client's exit
                                if let Some(rel_stream) =
                                    state.connection_reliable_streams.get(&obsolete_uuid)
                                {
                                    let msg = BrokerMessage::CrossingExit {
                                        client_id,
                                        obsolete_auth_topic,
                                        new_auth_topic,
                                    }
                                    .to_bytes();
                                    let _ = network.peer.send(
                                        &obsolete_uuid.into(),
                                        rel_stream,
                                        Bytes::from(msg),
                                    );
                                } else {
                                    warn!(
                                        "[BROKER] No reliable stream for obsolete authority shard {:?}",
                                        topic_to_string(&obsolete_auth_topic)
                                    );
                                }
                            }

                        if let Some(&new_auth_uuid) = state.topic_to_shard.get(&new_auth_topic) {
                            // Notify the new authority shard about the client's exit from the old shard (could be used for cleanup, AOI updates, etc.)
                            if let Some(rel_stream) =
                                state.connection_reliable_streams.get(&new_auth_uuid)
                            {
                                let msg = BrokerMessage::CrossingExit {
                                    client_id,
                                    obsolete_auth_topic,
                                    new_auth_topic,
                                }
                                .to_bytes();
                                let _ = network.peer.send(
                                    &new_auth_uuid.into(),
                                    rel_stream,
                                    Bytes::from(msg),
                                );
                            } else {
                                warn!(
                                    "[BROKER] No reliable stream for new authority shard {:?}",
                                    topic_to_string(&new_auth_topic)
                                );
                            }
                        }
                    }

                    BrokerMessage::ShardReady { shard_id } => {
                        info!(
                            "[BROKER] Shard with UUID {:?} reports ready.",
                            shard_id
                        );

                        if let Some(spatial_uuid) = state.spatial_server_uuid {
                            if let Some(rel_stream) =
                                state.connection_reliable_streams.get(&spatial_uuid)
                            {
                                let msg = BrokerMessage::ShardReady { shard_id }.to_bytes();
                                let _ = network.peer.send(
                                    &spatial_uuid.into(),
                                    rel_stream,
                                    Bytes::from(msg),
                                );
                            } else {
                                warn!(
                                    "[BROKER] No reliable stream for spatial server to report shard ready."
                                );
                            }
                        } else {
                            warn!(
                                "[BROKER] No spatial server UUID registered yet to report shard ready."
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
