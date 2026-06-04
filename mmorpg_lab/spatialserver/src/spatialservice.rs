use game_sockets::{GameConnection, GameStream, GameStreamReliability};
use std::collections::HashMap;
use tracing::{error, info, warn};

use crate::quadtree::{QuadTree, SplitData};
use crate::rect::{Rect, Vec2};
use bytes::Bytes;
use game_sockets::{GameNetworkEvent, GamePeer, protocols::QuicBackend};
use shared::broker_protocol::BrokerMessage;
use shared::orchestrator_protocol::OrchestratorMessage;

pub struct QuicConnection {
    pub peer: GamePeer,
    pub connection: Option<game_sockets::GameConnection>,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
}

pub struct SpatialService {
    quad_tree: QuadTree,
    client_shards: HashMap<u32, u32>, // client_id -> shard_id

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
        });

        let quic_orchestrator = Some(QuicConnection {
            peer: orchestrator_peer,
            connection: None,
            reliable_stream: None,
            unreliable_stream: None,
        });

        Self {
            quad_tree: QuadTree::new(
                Rect {
                    x: -500.0,
                    y: -500.0,
                    width: 1000.0,
                    height: 1000.0,
                },
                0,
                4,
                2,
                1,
            ),
            client_shards: HashMap::new(),
            quic_broker: quic_broker,
            quic_orchestrator: quic_orchestrator,
        }
    }

    // NETWORK HANDLING : POLL QUIC CONNECTIONS AND DISPATCH MESSAGES TO APPROPRIATE HANDLERS
    fn poll_broker_events(&mut self) {
        let mut messages_to_process: Vec<(GameConnection, Bytes)> = Vec::new();
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
                        match stream.is_reliable() {
                            true => quic_broker.reliable_stream = Some(stream),
                            false => quic_broker.unreliable_stream = Some(stream),
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
                    GameNetworkEvent::Message {
                        connection,
                        stream,
                        data,
                    } => {
                        info!(
                            " Message received from broker {:?} on stream {:?}: {} bytes",
                            connection.connection_id,
                            stream,
                            data.len()
                        );
                        messages_to_process.push((connection, data));
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

        for (connection, data) in messages_to_process {
            self.handle_broker_message(&connection, &data);
        }
    }

    fn poll_orchestrator_events(&mut self) {
        let mut messages_to_process: Vec<Bytes> = Vec::new();
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
                        // SHOULD NOT BE POSSIBLE SINCE
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
                            " Message received from orchestrator {:?} on stream {:?}: {} bytes",
                            connection.connection_id,
                            stream,
                            data.len()
                        );
                        messages_to_process.push(data);
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

        for data in messages_to_process {
            self.handle_orchestrator_message(&data);
        }
    }

    fn handle_broker_message(&mut self, connection: &game_sockets::GameConnection, data: &[u8]) {
        if let Some(message) = BrokerMessage::from_bytes(data) {
            match message {
                BrokerMessage::PositionUpdate { client_id, x, y } => {
                    info!(
                        "Received PositionUpdate from broker for client {}: x={}, y={}",
                        client_id, x, y
                    );
                    self.handle_position_update(client_id, Vec2 { x, y });
                }
                _ => {
                    warn!(
                        "Received unsupported message type from client: {:?}",
                        message
                    );
                }
            }
        } else {
            warn!(
                "{} {}",
                connection.connection_id.to_string(),
                "Received invalid message format from client"
            );
        }
    }

    fn handle_orchestrator_message(&mut self, data: &[u8]) {
        if let Some(message) = OrchestratorMessage::from_bytes(data) {
            match message {
                OrchestratorMessage::SplitConfirmation {
                    shard_id,
                    new_shard_id,
                } => {
                    info!(
                        "Received split confirmation from orchestrator for shard {} -> new shard {}",
                        shard_id, new_shard_id
                    );
                    self.handle_orchestrator_shard_ready(new_shard_id);
                    self.quad_tree.print_state();
                }
                _ => {
                    warn!(
                        "Received unsupported message type from orchestrator: {:?}",
                        message
                    );
                }
            }
        }
    }

    pub fn run(&mut self) {
        loop {
            self.poll_broker_events();
            self.poll_orchestrator_events();
            // self.quad_tree.print_state();
        }
    }

    pub fn handle_position_update(&mut self, client_id: u32, pos: Vec2) {
        let old_network_shard = self.client_shards.get(&client_id).copied();

        self.quad_tree.remove_player(client_id);

        if let Some(result) = self.quad_tree.insert_player(client_id, pos) {
            if old_network_shard != Some(result.network_shard_id) {
                if let Some(old) = old_network_shard {
                    self.send_unsubscribe(client_id, old);
                }
                self.send_subscribe(client_id, result.network_shard_id);
                self.client_shards
                    .insert(client_id, result.network_shard_id);
            }

            if let Some(split_data) = result.trigger_orchestrator {
                info!(
                    "Quadtree requires split for shard {}, requesting orchestrator to spawn 4 new servers",
                    split_data.parent_shard_id
                );
                self.request_orchestrator_split(split_data);
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

    pub fn handle_orchestrator_shard_ready(&mut self, ready_child_shard_id: u32) {
        info!(
            "QUIC : L'Orchestrateur confirme que le sous-serveur {} est en ligne. Validation...",
            ready_child_shard_id
        );

        // On tente d'activer cet enfant spécifique
        if let Some((parent_shard_id, updates)) =
            self.quad_tree.commit_child_split(ready_child_shard_id)
        {
            // Mass Handoff PARTIEL : On ne bouge QUE les joueurs de ce quadrant !
            for (affected_client, new_network_shard) in updates {
                // On les désabonne du vieux parent surchargé
                self.send_unsubscribe(affected_client, parent_shard_id);

                // On les abonne à leur tout nouveau serveur tout neuf
                self.send_subscribe(affected_client, new_network_shard);

                self.client_shards
                    .insert(affected_client, new_network_shard);

                info!(
                    "Joueur {} transféré avec succès du parent {} vers l'enfant {}",
                    affected_client, parent_shard_id, new_network_shard
                );
            }
        } else {
            warn!(
                "L'orchestrateur a signalé le shard {} comme prêt, mais il est introuvable ou déjà actif.",
                ready_child_shard_id
            );
        }
    }

    // Communicate with broker
    fn send_unsubscribe(&self, client_id: u32, shard_id: u32) {
        let topic = format!("shard:{}", shard_id);
        info!(client_id, topic, "Unsubscribe");
        let mut topic_bytes = [0u8; 32];
        let bytes = topic.as_bytes();
        let len = bytes.len().min(topic_bytes.len());
        topic_bytes[..len].copy_from_slice(&bytes[..len]);
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
        let mut topic_bytes = [0u8; 32];
        let bytes = topic.as_bytes();
        let len = bytes.len().min(topic_bytes.len());
        topic_bytes[..len].copy_from_slice(&bytes[..len]);

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

    // fn emit_crossing_alert(
    //     &self,
    //     connection: &game_sockets::GameConnection,
    //     client_id: u32,
    //     nearby_shards: Vec<u32>,
    // ) {
    //     info!(client_id, ?nearby_shards, "CrossingAlert émis");
    //     // TODO: Call broker
    //     // FOR ALL  nearby_shards
    //     //     envoyer au broker de subscribe cette shard au topic client:<client_id>
    // }
}
