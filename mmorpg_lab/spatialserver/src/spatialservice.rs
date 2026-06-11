use game_sockets::{GameConnection, GameStream, GameStreamReliability};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use crate::quadtree::{QuadTree, SplitData};
use crate::rect::{Rect, Vec2};
use crate::util::{get_added_ids, get_removed_ids};
use bytes::Bytes;
use game_sockets::{GameNetworkEvent, GamePeer, protocols::QuicBackend};
use shared::broker_protocol::{BrokerMessage, string_to_topic};
use shared::orchestrator_protocol::OrchestratorMessage;
use std::time::{Duration, Instant};

use shared::{MAP_BOUND_MIN, MAP_SIZE};

#[derive(Copy, Clone, Debug)]
enum PlayerSplitState {
    EmitCrossingAlert,
    EmitSwitchAuthority,
    EmitCrossingExit,
}

#[derive(Copy, Clone, Debug)]
struct PlayerState {
    client_id: u32,
    parent_shard_id: u32,
    neighbor_shard_id: u32,
    split_state: PlayerSplitState,
}

const MARGIN: f32 = 200.0;
pub struct QuicConnection {
    pub peer: GamePeer,
    pub connection: Option<game_sockets::GameConnection>,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
    pub buffer: Vec<u8>,
}

pub struct SpatialService {
    quad_tree: QuadTree,
    client_shards: HashMap<u32, u32>, // client_id -> shard_id
    client_crossing_state: HashMap<u32, Vec<u32>>,
    player_states: Vec<PlayerState>,

    spawn_shard_id : u32,
    spawn_point: Vec2,

    // QUIC connections to the broker & orchestrator
    pub quic_broker: Option<QuicConnection>,
    pub quic_orchestrator: Option<QuicConnection>,
}

impl SpatialService {
    /// Sets up QUIC connections to the broker and orchestrator, and initializes the QuadTree with the specified bounds and parameters.
    pub fn new(
        broker_addr: &String,
        broker_port: &u16,
        orchestrator_addr: &String,
        orchestrator_port: &u16,
    ) -> Self {
        let broker_peer = GamePeer::new(QuicBackend::new());
        if let Err(e) = broker_peer.connect(broker_addr, *broker_port) {
            error!(
                " ERROR: Failed to connect to broker on address {}:{}: {:?}",
                broker_addr, broker_port, e
            );
        }

        let orchestrator_peer = GamePeer::new(QuicBackend::new());
        if let Err(e) = orchestrator_peer.connect(orchestrator_addr, *orchestrator_port) {
            error!(
                " ERROR: Failed to connect to orchestrator on address {}:{}: {:?}",
                orchestrator_addr, orchestrator_port, e
            );
        }

        let quic_broker = Some(QuicConnection {
            peer: broker_peer,
            connection: None,
            reliable_stream: None,
            unreliable_stream: None,
            buffer: Vec::new(),
        });

        let quic_orchestrator = Some(QuicConnection {
            peer: orchestrator_peer,
            connection: None,
            reliable_stream: None,
            unreliable_stream: None,
            buffer: Vec::new(),
        });

        Self {
            quad_tree: QuadTree::new(
                Rect {
                    x: MAP_BOUND_MIN,
                    y: MAP_BOUND_MIN,
                    width: MAP_SIZE,
                    height: MAP_SIZE,
                },
                0,
                1,
                2,
                0,
            ),
            client_shards: HashMap::new(),
            quic_broker: quic_broker,
            quic_orchestrator: quic_orchestrator,
            client_crossing_state: HashMap::new(),
            player_states: Vec::new(),
            spawn_point: Vec2 {
                x: shared::SPAWN_X,
                y: shared::SPAWN_Y,
            },
            spawn_shard_id: 0,
        }
    }

    fn process_player_states(&mut self) {
        let mut pending_delete = Vec::new();
        for i in 0..self.player_states.len() {
            let player_state = self.player_states.get(i).clone().unwrap();
            let (client_id, old_shard_id, new_shard_id, split_state) = (
                player_state.client_id,
                player_state.parent_shard_id,
                player_state.neighbor_shard_id,
                player_state.split_state,
            );
            match split_state {
                PlayerSplitState::EmitCrossingAlert => {
                    self.emit_crossing_alert(client_id, old_shard_id, new_shard_id);
                    self.player_states[i].split_state = PlayerSplitState::EmitSwitchAuthority;
                }
                PlayerSplitState::EmitSwitchAuthority => {
                    self.emit_switch_authority(client_id, old_shard_id, new_shard_id);
                    self.player_states[i].split_state = PlayerSplitState::EmitCrossingExit;
                }
                PlayerSplitState::EmitCrossingExit => {
                    self.emit_crossing_exit(client_id, old_shard_id, new_shard_id);
                    pending_delete.push(i);
                }
            }
        }

        for index in pending_delete.into_iter().rev() {
            self.player_states.remove(index);
        }
    }

    // NETWORK HANDLING : POLL QUIC CONNECTIONS AND DISPATCH MESSAGES TO APPROPRIATE HANDLERS
    fn poll_broker_events(&mut self) {
        let mut parsed_messages = Vec::new();

        if let Some(quic_broker) = &mut self.quic_broker {
            while let Ok(Some(event)) = quic_broker.peer.poll() {
                match event {
                    GameNetworkEvent::Connected(connection) => {
                        info!(" Connected to broker : {:?}", connection.connection_id);
                        quic_broker.connection = Some(connection);
                        if let Ok(_) = quic_broker
                            .peer
                            .create_stream(connection, GameStreamReliability::Reliable)
                        {
                            info!("Reliable stream created for broker");
                        } else {
                            error!("Failed to create reliable stream for broker");
                            return;
                        }

                        if let Ok(_) = quic_broker
                            .peer
                            .create_stream(connection, GameStreamReliability::Unreliable)
                        {
                            info!("Unreliable stream created for broker");
                        } else {
                            error!("Failed to create unreliable stream for broker");
                            return;
                        }
                    }
                    GameNetworkEvent::StreamCreated(connection, stream) => {
                        info!(
                            " Stream created for broker {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );

                        let dummy_msg = BrokerMessage::Subscribe {
                            client_id: u32::MAX,
                            topic: [0u8; 32],
                        };

                        match stream.is_reliable() {
                            true => {
                                quic_broker.reliable_stream = Some(stream.clone());
                                // Send on reliable stream to register the UUID
                                let _ = quic_broker.peer.send(
                                    &connection,
                                    &stream,
                                    Bytes::from(dummy_msg.to_bytes()),
                                );
                            }

                            false => {
                                quic_broker.unreliable_stream = Some(stream.clone());
                                let _ = quic_broker.peer.send(
                                    &connection,
                                    &stream,
                                    Bytes::from(dummy_msg.to_bytes()),
                                );
                            }
                        }
                    }
                    GameNetworkEvent::StreamClosed(connection, stream) => {
                        info!(
                            " Stream closed for broker {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );
                        match stream.is_reliable() {
                            true => quic_broker.reliable_stream = None,
                            false => quic_broker.unreliable_stream = None,
                        }
                    }
                    GameNetworkEvent::Disconnected(game_connection) => {
                        info!(" Broker disconnected: {:?}", game_connection.connection_id);
                        // SHOULD NOT BE POSSIBLE SINCE
                        if Some(game_connection) == quic_broker.connection {
                            quic_broker.connection = None;
                            quic_broker.reliable_stream = None;
                            quic_broker.unreliable_stream = None;
                            error!(" Disconnected from broker, shutting down service.");
                            return;
                        }
                    }
                    GameNetworkEvent::Message { data, .. } => {
                        // Parse all incoming messages from the broker connection first
                        quic_broker.buffer.extend_from_slice(&data);

                        let mut msgs = BrokerMessage::parse_multiple(&mut quic_broker.buffer);
                        parsed_messages.append(&mut msgs);
                    }
                    GameNetworkEvent::Error { connection, inner } => {
                        warn!(
                            " Error on connection {:?}: {:?}",
                            connection.connection_id, inner
                        );
                    }
                    _ => {} // Ignore other events
                }
            }
        }

        for message in parsed_messages {
            self.handle_broker_message(message);
        }
    }

    fn poll_orchestrator_events(&mut self) {
        if let Some(quic_orchestrator) = &mut self.quic_orchestrator {
            while let Ok(Some(event)) = quic_orchestrator.peer.poll() {
                match event {
                    GameNetworkEvent::Connected(connection) => {
                        info!(" Orchestrator connected : {:?}", connection.connection_id);
                        quic_orchestrator.connection = Some(connection);
                        let _ = quic_orchestrator
                            .peer
                            .create_stream(connection, GameStreamReliability::Reliable);
                        let _ = quic_orchestrator
                            .peer
                            .create_stream(connection, GameStreamReliability::Unreliable);
                    }
                    GameNetworkEvent::StreamCreated(connection, stream) => {
                        info!(
                            " Stream created for orchestrator {:?}, reliable: {}, stream_id: {}",
                            connection.connection_id,
                            stream.is_reliable(),
                            stream.stream_id
                        );
                        match stream.is_reliable() {
                            true => quic_orchestrator.reliable_stream = Some(stream),
                            false => quic_orchestrator.unreliable_stream = Some(stream),
                        }
                    }
                    GameNetworkEvent::StreamClosed(connection, stream) => {
                        info!(
                            " Stream closed for orchestrator {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );
                        match stream.is_reliable() {
                            true => quic_orchestrator.reliable_stream = None,
                            false => quic_orchestrator.unreliable_stream = None,
                        }
                    }
                    GameNetworkEvent::Disconnected(game_connection) => {
                        info!(
                            " Orchestrator disconnected: {:?}",
                            game_connection.connection_id
                        );
                        // SHOULD NEVER HAPPEN SINCE DISCONNECTING MEANS SHUTTING DOWN THE SERVICE
                        if Some(game_connection) == quic_orchestrator.connection {
                            quic_orchestrator.connection = None;
                            quic_orchestrator.reliable_stream = None;
                            quic_orchestrator.unreliable_stream = None;
                            error!(" Disconnected from orchestrator, shutting down service.");
                            return;
                        }
                    }
                    GameNetworkEvent::Message {
                        connection,
                        stream,
                        data,
                    } => {
                        info!(
                            " Unexpected message received from orchestrator {:?} on stream {:?}: {} bytes",
                            connection.connection_id,
                            stream,
                            data.len()
                        );
                    }
                    GameNetworkEvent::Error { connection, inner } => {
                        warn!(
                            " Error on connection {:?}: {:?}",
                            connection.connection_id, inner
                        );
                    }
                }
            }
        }
    }

    fn handle_broker_message(&mut self, message: BrokerMessage) {
        match message {
            BrokerMessage::PositionUpdate { client_id, x, y } => {
                // info!(
                //     "Received PositionUpdate from broker for client {}: x={}, y={}",
                //     client_id, x, y
                // );
                self.handle_position_update(client_id, Vec2 { x, y });
            }
            BrokerMessage::ShardReady { shard_id } => {
                info!(
                    "Received ShardReady from broker for shard {}: activating it and migrating affected players if necessary",
                    shard_id
                );
                self.handle_shard_ready(shard_id);
                self.quad_tree.print_state();
            }
            BrokerMessage::PlayerDisconnected { client_id } => {
                info!(
                    "Received PlayerDisconnected from broker for client {}: removing it from quadtree and cleaning up state",
                    client_id
                );
                self.quad_tree.print_state();
                self.quad_tree.remove_player(client_id);
                self.client_shards.remove(&client_id);
                self.client_crossing_state.remove(&client_id);
                self.quad_tree.print_state();
            }
            _ => {
                warn!(
                    "Received unsupported message type from client: {:?}",
                    message
                );
            }
        }
    }

    pub fn run(&mut self) {
        let mut last_10hz_tick = Instant::now();
        let interval_10hz = Duration::from_millis(500);

        loop {
            self.poll_broker_events();
            self.poll_orchestrator_events();
            // self.quad_tree.print_state();

            let now = Instant::now();

            if (now.duration_since(last_10hz_tick)) >= interval_10hz {
                self.process_player_states();
                last_10hz_tick += interval_10hz;
            }
        }
    }

    pub fn handle_position_update(&mut self, client_id: u32, pos: Vec2) {
        let old_network_shard = self.client_shards.get(&client_id).copied();

        self.quad_tree.remove_player(client_id);

        if let Some(result) = self.quad_tree.insert_player(client_id, pos) {
            // Check if we moved shards and need to update broker subscriptions
            // if insertion required a split, it will NOT send anything on the network before orchestrator tells us the new shard is ready
            if old_network_shard != Some(result.network_shard_id) {
                if let Some(old) = old_network_shard {
                    let old_nearby = self
                        .client_crossing_state
                        .get(&client_id)
                        .cloned()
                        .unwrap_or_default();

                    if old_nearby.contains(&result.network_shard_id) {
                        info!("Normal border cross for {}", client_id);
                        self.emit_switch_authority(client_id, old, result.network_shard_id);
                    } else {
                        info!(
                            "Player {} teleported to {}, forcing safe handoff sequence!",
                            client_id, result.network_shard_id
                        );
                        self.emit_crossing_alert(client_id, old, result.network_shard_id);
                        self.emit_switch_authority(client_id, old, result.network_shard_id);
                        self.emit_crossing_exit(client_id, old, result.network_shard_id);
                    }
                } else {
                    // New player, just subscribe to the new shard
                    info!(
                        "New player {} inserted in shard {}, at pos {} {}, subscribing to it",
                        client_id, result.network_shard_id, pos.x, pos.y
                    );
                    self.send_subscribe(client_id, result.network_shard_id);

                    // When a new player join, insert his first shard to avoid unwanted crossing alerts
                    // THIS DOES NOT PREVENT THE PLAYER FROM HAVING CROSSING ALERTS IF HE SPAWN NEAR BORDERS
                    self.client_crossing_state
                        .insert(client_id, vec![result.network_shard_id]);
                }

                self.client_shards
                    .insert(client_id, result.network_shard_id);
            }

            if let Some(split_data) = result.trigger_orchestrator {
                info!(
                    "Quadtree requires split for shard {}, requesting orchestrator to spawn 4 new servers",
                    split_data.parent_shard_id
                );
                self.request_orchestrator_split(split_data);

                let new_spawn_shard_id_opt = self.quad_tree.shard_for(&self.spawn_point);
                if let Some(new_spawn_shard_id) = new_spawn_shard_id_opt {
                    if new_spawn_shard_id != self.spawn_shard_id {
                        info!(
                            "Spawn shard has changed from {} to {} after split, notifying broker",
                            self.spawn_shard_id, new_spawn_shard_id
                        );
                        self.spawn_shard_id = new_spawn_shard_id;
                        self.send_new_spawn_shard(new_spawn_shard_id);
                    }
                }
            }

            // Crossing alert check :
            // if the player is near the border of its shard, we check if there are nearby shards and send an alert to the broker
            let shards_near = self.quad_tree.shards_near(&pos, MARGIN);

            let old_nearby = self
                .client_crossing_state
                .get(&client_id)
                .cloned()
                .unwrap_or_default();

            if shards_near != old_nearby {
                let added = get_added_ids(&old_nearby, &shards_near);
                let removed = get_removed_ids(&old_nearby, &shards_near);

                for neighbor_shard_id in added {
                    self.emit_crossing_alert(client_id, result.network_shard_id, neighbor_shard_id);
                }
                for neighbor_shard_id in removed {
                    self.emit_crossing_exit(client_id, neighbor_shard_id, result.network_shard_id);
                }
                self.client_crossing_state.insert(client_id, shards_near);
            }
        }
    }

    fn request_orchestrator_split(&self, split_data: SplitData) {
        // Gather new child shard IDs from the split data
        info!(
            "Requesting orchestrator split: parent shard {}, new shards {:?}",
            split_data.parent_shard_id, split_data.new_shards_ids
        );
        let msg = OrchestratorMessage::RequestSplit {
            shard_id: split_data.parent_shard_id,
            new_shards_ids: split_data.new_shards_ids,
        };

        if let Some(quic_orchestrator) = &self.quic_orchestrator {
            if let Some(peer) = Some(&quic_orchestrator.peer) {
                if let Some(connection) = &quic_orchestrator.connection {
                    if let Some(stream) = &quic_orchestrator.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send split request to orchestrator for shard {}: {:?}",
                                split_data.parent_shard_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    pub fn handle_shard_ready(&mut self, ready_child_shard_id: u32) {
        info!(
            "Orchestrator confirmed shard {} is ready, activating it and migrating affected players if necessary",
            ready_child_shard_id
        );

        if let Some((parent_shard_id, updates)) =
            self.quad_tree.commit_child_split(ready_child_shard_id)
        {
            // Partial mass handoff : only move players that are in this shard, old shard is still active and can serve players that are not in the new shard
            for (affected_client, new_network_shard) in updates {
                info!(
                    "Transferring player {} from {} to {}",
                    affected_client, parent_shard_id, new_network_shard
                );

                self.emit_crossing_alert(affected_client, parent_shard_id, new_network_shard);
                self.emit_switch_authority(affected_client, parent_shard_id, new_network_shard);
                self.emit_crossing_exit(affected_client, parent_shard_id, new_network_shard);

                self.client_shards
                    .insert(affected_client, new_network_shard);
                self.client_crossing_state
                    .insert(affected_client, vec![new_network_shard]);
            }
        } else {
            warn!(
                "Orchestrator confirmed shard {} is ready, but it is not found or already active.",
                ready_child_shard_id
            );
        }
    }

    // Communicate with broker
    fn send_unsubscribe(&self, client_id: u32, shard_id: u32) {
        let topic = format!("shard:{}", shard_id);
        info!(client_id, topic, "Unsubscribe");
        let topic_bytes = string_to_topic(&topic);
        let msg = BrokerMessage::Unsubscribe {
            client_id,
            topic: topic_bytes,
        };

        if let Some(broker) = &self.quic_broker {
            if let Some(peer) = Some(&broker.peer) {
                if let Some(connection) = &broker.connection {
                    if let Some(stream) = &broker.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send unsubscribe message for client {}: {:?}",
                                client_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn send_subscribe(&self, client_id: u32, shard_id: u32) {
        let topic = format!("shard:{}", shard_id);

        info!(client_id, topic, "Subscribe");
        let topic_bytes = string_to_topic(&topic);

        let msg = BrokerMessage::Subscribe {
            client_id,
            topic: topic_bytes,
        };

        if let Some(broker) = &self.quic_broker {
            if let Some(peer) = Some(&broker.peer) {
                if let Some(connection) = &broker.connection {
                    if let Some(stream) = &broker.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send subscribe message for client {}: {:?}",
                                client_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn emit_crossing_alert(&self, client_id: u32, owner_shard_id: u32, neighbor_shard_id: u32) {
        info!(client_id, ?neighbor_shard_id, "CrossingAlert");

        let topic = format!("shard:{}", owner_shard_id);
        let topic_bytes = string_to_topic(&topic);

        let neighbor_topic = format!("shard:{}", neighbor_shard_id);
        let neighbor_topic_bytes = string_to_topic(&neighbor_topic);

        let msg = BrokerMessage::CrossingAlert {
            client_id,
            dest_authority_topic: topic_bytes,
            neighbor_topic: neighbor_topic_bytes,
        };

        if let Some(broker) = &self.quic_broker {
            if let Some(peer) = Some(&broker.peer) {
                if let Some(connection) = &broker.connection {
                    if let Some(stream) = &broker.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send crossing alert message for client {}: {:?}",
                                client_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn emit_switch_authority(&self, client_id: u32, owner_shard_id: u32, neighbor_shard_id: u32) {
        info!(
            client_id,
            owner_shard_id, neighbor_shard_id, "SwitchAuthority"
        );

        let topic = format!("shard:{}", owner_shard_id);
        let topic_bytes = string_to_topic(&topic);

        let neighbor_topic = format!("shard:{}", neighbor_shard_id);
        let neighbor_topic_bytes = string_to_topic(&neighbor_topic);

        let msg = BrokerMessage::AuthoritySwitch {
            client_id,
            old_auth_topic: topic_bytes,
            new_auth_topic: neighbor_topic_bytes,
        };

        if let Some(broker) = &self.quic_broker {
            if let Some(peer) = Some(&broker.peer) {
                if let Some(connection) = &broker.connection {
                    if let Some(stream) = &broker.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send switch authority message for client {}: {:?}",
                                client_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn emit_crossing_exit(&self, client_id: u32, old_shard_id: u32, owner_shard_id: u32) {
        info!(client_id, old_shard_id, owner_shard_id, "CrossingExit");

        let old_topic = format!("shard:{}", old_shard_id);
        let new_topic = format!("shard:{}", owner_shard_id);

        let old_topic_bytes = string_to_topic(&old_topic);
        let new_topic_bytes = string_to_topic(&new_topic);

        let msg = BrokerMessage::CrossingExit {
            client_id,
            obsolete_auth_topic: old_topic_bytes,
            new_auth_topic: new_topic_bytes,
        };

        if let Some(broker) = &self.quic_broker {
            if let Some(peer) = Some(&broker.peer) {
                if let Some(connection) = &broker.connection {
                    if let Some(stream) = &broker.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send crossing exit message for client {}: {:?}",
                                client_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn send_new_spawn_shard(&self, new_shard_id: u32) {
        info!(new_shard_id, "New spawn shard after split");

        let msg = BrokerMessage::NewSpawnShard { new_shard_id };

        if let Some(broker) = &self.quic_broker {
            if let Some(peer) = Some(&broker.peer) {
                if let Some(connection) = &broker.connection {
                    if let Some(stream) = &broker.reliable_stream {
                        if let Err(e) = peer.send(connection, stream, Bytes::from(msg.to_bytes())) {
                            error!(
                                " Failed to send new spawn shard message for shard {}: {:?}",
                                new_shard_id, e
                            );
                        }
                    }
                }
            }
        }
    }
}
