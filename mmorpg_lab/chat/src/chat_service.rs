use std::collections::HashMap;

use crate::moderator::Moderator;
use bytes::Bytes;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared::broker_protocol::{BrokerMessage, TAG_CLIENT_TYPE_CHAT_SERVICE};
use tracing::{error, info, warn};
pub struct ChatService {
    pub usernames: HashMap<u32, String>,

    pub peer: GamePeer,
    pub conn: Option<GameConnection>,
    pub rel_stream: Option<GameStream>,

    pub moderator: Moderator,
}

impl ChatService {
    pub fn new(moderator: Moderator, peer: GamePeer) -> Self {
        Self {
            usernames: HashMap::new(),
            peer,
            conn: None,
            rel_stream: None,
            moderator,
        }
    }

    pub fn run(&mut self) {
        loop {
            while let Ok(Some(event)) = self.peer.poll() {
                match event {
                    GameNetworkEvent::Connected(connection) => {
                        info!(" Connected to broker : {:?}", connection.connection_id);

                        self.conn = Some(connection);
                        if let Err(e) = self
                            .peer
                            .create_stream(connection, GameStreamReliability::Reliable)
                        {
                            error!("Failed to create reliable stream for broker: {:?}", e);
                            return;
                        }
                    }
                    GameNetworkEvent::StreamCreated(connection, stream) => {
                        info!(
                            " Stream created for broker {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );
                        self.rel_stream = Some(stream.clone());

                        info!(
                            "Sending Connected message to broker {:?} to register spatial server",
                            connection.connection_id
                        );

                        let connected_msg = BrokerMessage::Connected {
                            client_id: 1 as u32, // does not matter for the spatial server
                            client_type: TAG_CLIENT_TYPE_CHAT_SERVICE,
                        };
                        // Send on reliable stream to register the UUID
                        let _ = self.peer.send(
                            &connection,
                            &stream,
                            Bytes::from(connected_msg.to_bytes()),
                        );
                    }
                    GameNetworkEvent::StreamClosed(connection, stream) => {
                        error!(
                            " Stream closed for broker {:?}, reliable: {}",
                            connection.connection_id,
                            stream.is_reliable()
                        );
                    }
                    GameNetworkEvent::Disconnected(game_connection) => {
                        error!(
                            "Disconnected from broker: {:?}",
                            game_connection.connection_id
                        );
                    }
                    GameNetworkEvent::Message {
                        connection,
                        stream,
                        data,
                    } => {
                        let msg_opt = BrokerMessage::from_bytes(data.as_ref());

                        if let Some(msg) = msg_opt {
                            match msg {
                                BrokerMessage::ChatJoin {
                                    client_id,
                                    username,
                                } => {
                                    let username_str = String::from_utf8_lossy(&username);
                                    info!(
                                        "Client {} joined the chat with username: {:?}",
                                        client_id, username_str
                                    );
                                    self.usernames.insert(client_id, username_str.to_string());
                                }

                                BrokerMessage::ClientChatMessage { client_id, msg } => {
                                    let str_msg = String::from_utf8_lossy(&msg);
                                    info!(
                                        "Received chat message from client {}: {:?}",
                                        client_id, str_msg
                                    );

                                    let moderated_msg = self.moderator.moderate_message(&str_msg);

                                    let mut msg = [0u8; 64];
                                    let msg_bytes = moderated_msg.as_bytes();
                                    let len = msg_bytes.len().min(msg.len());
                                    msg[..len].copy_from_slice(&msg_bytes[..len]);

                                    let username_str = self
                                        .usernames
                                        .get(&client_id)
                                        .cloned()
                                        .unwrap_or_else(|| format!("Player{}", client_id));
                                    let username_bytes = username_str.as_bytes();
                                    let mut username = [0u8; 32];
                                    let len = username_bytes.len().min(username.len());
                                    username[..len].copy_from_slice(&username_bytes[..len]);

                                    let broadcast_msg =
                                        BrokerMessage::BroadcastChatMessage { username, msg };

                                    if let Err(e) = self.peer.send(
                                        &connection,
                                        &stream,
                                        Bytes::from(broadcast_msg.to_bytes()),
                                    ) {
                                        error!(
                                            "Failed to send broadcast message to broker: {:?}",
                                            e
                                        );
                                    }
                                }
                                _ => {
                                    warn!(
                                        "Received unsupported message type in chat server: {:?}",
                                        msg
                                    );
                                }
                            }
                        } else {
                            warn!("Received invalid message from broker: {:?}", data);
                        }
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
}
