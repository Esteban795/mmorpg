use bytes::{Buf, BufMut, BytesMut};

// --- MESSAGES TAGS ---
pub const TAG_SUBSCRIBE: u8 = 0x01;
pub const TAG_UNSUBSCRIBE: u8 = 0x02;
pub const TAG_PUBLISH: u8 = 0x03;
pub const TAG_BROADCAST: u8 = 0x04;
pub const TAG_CLIENT_INPUT: u8 = 0x05;
pub const TAG_POSITION_UPDATE: u8 = 0x10;
pub const TAG_SHARD_READY: u8 = 0x11;

// Spatial Server messages
pub const TAG_CROSSING_ALERT: u8 = 0x25;
pub const TAG_AUTHORITY_SWITCH: u8 = 0x26;
pub const TAG_CROSSING_EXIT: u8 = 0x27;

// Inter Shards communication messages (packets)
pub const TAG_INTER_SHARD_MESSAGE: u8 = 0x30;

// Inter Shards handoff messages
pub const TAG_HANDOFF_REQUEST: u8 = 0x20;
pub const TAG_GHOST_UPDATE: u8 = 0x23;
pub const TAG_HANDOFF_COMPLETE: u8 = 0x24;

// --- BINARY PROTOCOL FOR BROKER MESSAGES ---
#[derive(Debug, Clone)]
pub enum BrokerMessage {
    Subscribe {
        client_id: u32,
        topic: [u8; 32],
    },
    Unsubscribe {
        client_id: u32,
        topic: [u8; 32],
    },
    Publish {
        topic: [u8; 32],
        payload: Vec<u8>,
    },
    Broadcast {
        payload: Vec<u8>,
    },
    ClientInput {
        client_id: u32,
        input: [u8; 16],
    },
    PositionUpdate {
        client_id: u32,
        x: f32,
        y: f32,
    },
    ShardReady {
        shard_id: u32,
    },

    // Spatial server messages
    CrossingAlert {
        client_id: u32,
        dest_authority_topic: [u8; 32],
        neighbor_topic: [u8; 32],
    },
    AuthoritySwitch {
        client_id: u32,
        old_auth_topic: [u8; 32],
        new_auth_topic: [u8; 32],
    },
    CrossingExit {
        client_id: u32,
        obsolete_auth_topic: [u8; 32],
        new_auth_topic: [u8; 32],
    },

    // Inter Shard messages envelope
    InterShardMessage {
        topic_dest: [u8; 32],
        topic_from: [u8; 32],
        payload: Vec<u8>,
    },
}

impl BrokerMessage {
    // Serialize Rust struct into bytes to send over QUIC using strict binary format
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        match self {
            BrokerMessage::Subscribe { client_id, topic } => {
                buf.put_u8(TAG_SUBSCRIBE);
                buf.put_u32_le(*client_id);
                buf.put_slice(topic);
            }
            BrokerMessage::Unsubscribe { client_id, topic } => {
                buf.put_u8(TAG_UNSUBSCRIBE);
                buf.put_u32_le(*client_id);
                buf.put_slice(topic);
            }
            BrokerMessage::Publish { topic, payload } => {
                buf.put_u8(TAG_PUBLISH);
                buf.put_slice(topic);
                buf.put_u16_le(payload.len() as u16);
                buf.put_slice(payload);
            }
            BrokerMessage::Broadcast { payload } => {
                buf.put_u8(TAG_BROADCAST);
                buf.put_u16_le(payload.len() as u16);
                buf.put_slice(payload);
            }
            BrokerMessage::ClientInput { client_id, input } => {
                buf.put_u8(TAG_CLIENT_INPUT);
                buf.put_u32_le(*client_id);
                buf.put_slice(input);
            }
            BrokerMessage::PositionUpdate { client_id, x, y } => {
                buf.put_u8(TAG_POSITION_UPDATE);
                buf.put_u32_le(*client_id);
                buf.put_f32_le(*x);
                buf.put_f32_le(*y);
            }
            BrokerMessage::CrossingAlert {
                client_id,
                dest_authority_topic,
                neighbor_topic,
            } => {
                buf.put_u8(TAG_CROSSING_ALERT);
                buf.put_u32_le(*client_id);
                buf.put_slice(dest_authority_topic);
                buf.put_slice(neighbor_topic);
            }
            BrokerMessage::AuthoritySwitch {
                client_id,
                old_auth_topic,
                new_auth_topic,
            } => {
                buf.put_u8(TAG_AUTHORITY_SWITCH);
                buf.put_u32_le(*client_id);
                buf.put_slice(old_auth_topic);
                buf.put_slice(new_auth_topic);
            }
            BrokerMessage::CrossingExit {
                client_id,
                obsolete_auth_topic,
                new_auth_topic,
            } => {
                buf.put_u8(TAG_CROSSING_EXIT);
                buf.put_u32_le(*client_id);
                buf.put_slice(obsolete_auth_topic);
                buf.put_slice(new_auth_topic);
            }
            BrokerMessage::InterShardMessage {
                topic_dest,
                topic_from,
                payload,
            } => {
                buf.put_u8(TAG_INTER_SHARD_MESSAGE);
                buf.put_slice(topic_dest);
                buf.put_slice(topic_from);
                buf.put_u16_le(payload.len() as u16);
                buf.put_slice(payload);
            }
            BrokerMessage::ShardReady { shard_id } => {
                buf.put_u8(TAG_SHARD_READY);
                buf.put_u32_le(*shard_id);
            }
        }
        buf.freeze().to_vec()
    }

    // Deserialize bytes received over QUIC into Rust struct, returning None if format is invalid
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut buf = data;
        let tag = buf.get_u8();

        match tag {
            TAG_SUBSCRIBE => {
                if buf.remaining() < 36 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let mut topic = [0u8; 32];
                buf.copy_to_slice(&mut topic);
                Some(BrokerMessage::Subscribe { client_id, topic })
            }
            TAG_UNSUBSCRIBE => {
                if buf.remaining() < 36 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let mut topic = [0u8; 32];
                buf.copy_to_slice(&mut topic);
                Some(BrokerMessage::Unsubscribe { client_id, topic })
            }
            TAG_PUBLISH => {
                if buf.remaining() < 34 {
                    return None;
                }
                let mut topic = [0u8; 32];
                buf.copy_to_slice(&mut topic);
                let payload_len = buf.get_u16_le() as usize;
                if buf.remaining() < payload_len {
                    return None;
                }
                let mut payload = vec![0u8; payload_len];
                buf.copy_to_slice(&mut payload);
                Some(BrokerMessage::Publish { topic, payload })
            }
            TAG_BROADCAST => {
                if buf.remaining() < 2 {
                    return None;
                }
                let payload_len = buf.get_u16_le() as usize;
                if buf.remaining() < payload_len {
                    return None;
                }
                let mut payload = vec![0u8; payload_len];
                buf.copy_to_slice(&mut payload);
                Some(BrokerMessage::Broadcast { payload })
            }
            TAG_CLIENT_INPUT => {
                if buf.remaining() < 20 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let mut input = [0u8; 16];
                buf.copy_to_slice(&mut input);
                Some(BrokerMessage::ClientInput { client_id, input })
            }
            TAG_POSITION_UPDATE => {
                if buf.remaining() < 12 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let x = buf.get_f32_le();
                let y = buf.get_f32_le();
                Some(BrokerMessage::PositionUpdate { client_id, x, y })
            }
            TAG_CROSSING_ALERT => {
                if buf.remaining() < 68 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let mut dest_authority_topic = [0u8; 32];
                let mut neighbor_topic = [0u8; 32];
                buf.copy_to_slice(&mut dest_authority_topic);
                buf.copy_to_slice(&mut neighbor_topic);
                Some(BrokerMessage::CrossingAlert {
                    client_id,
                    dest_authority_topic,
                    neighbor_topic,
                })
            }
            TAG_AUTHORITY_SWITCH => {
                if buf.remaining() < 68 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let mut old_auth_topic = [0u8; 32];
                let mut new_auth_topic = [0u8; 32];
                buf.copy_to_slice(&mut old_auth_topic);
                buf.copy_to_slice(&mut new_auth_topic);
                Some(BrokerMessage::AuthoritySwitch {
                    client_id,
                    old_auth_topic,
                    new_auth_topic,
                })
            }
            TAG_CROSSING_EXIT => {
                if buf.remaining() < 68 {
                    return None;
                }
                let client_id = buf.get_u32_le();
                let mut obsolete_auth_topic = [0u8; 32];
                let mut new_auth_topic = [0u8; 32];
                buf.copy_to_slice(&mut obsolete_auth_topic);
                buf.copy_to_slice(&mut new_auth_topic);
                Some(BrokerMessage::CrossingExit {
                    client_id,
                    obsolete_auth_topic,
                    new_auth_topic,
                })
            }
            TAG_INTER_SHARD_MESSAGE => {
                if buf.remaining() < 66 {
                    return None;
                }
                let mut topic_dest = [0u8; 32];
                let mut topic_from = [0u8; 32];
                buf.copy_to_slice(&mut topic_dest);
                buf.copy_to_slice(&mut topic_from);
                let payload_len = buf.get_u16_le() as usize;
                if buf.remaining() < payload_len {
                    return None;
                }
                let mut payload = vec![0u8; payload_len];
                buf.copy_to_slice(&mut payload);
                Some(BrokerMessage::InterShardMessage {
                    topic_dest,
                    topic_from,
                    payload,
                })
            }
            TAG_SHARD_READY => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u32_le();
                Some(BrokerMessage::ShardReady { shard_id })
            }
            _ => None, // Unknown tag
        }
    }
}

// Internal Inter-Shard communication payloads (not directly sent by clients, but used by shards to exchange entity state during handoff)
#[derive(Debug, Clone)]
pub enum InterShardPayload {
    HandoffRequest {
        entity_id: u32,
        pos_x: f32,
        pos_y: f32,
        vel_x: f32,
        vel_y: f32,
        state: [u8; 64],
    },
    GhostUpdate {
        entity_id: u32,
        pos_x: f32,
        pos_y: f32,
        vel_x: f32,
        vel_y: f32,
    },
    HandoffComplete {
        entity_id: u32,
    },
}

impl InterShardPayload {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        match self {
            InterShardPayload::HandoffRequest {
                entity_id,
                pos_x,
                pos_y,
                vel_x,
                vel_y,
                state,
            } => {
                buf.put_u8(TAG_HANDOFF_REQUEST);
                buf.put_u32_le(*entity_id);
                buf.put_f32_le(*pos_x);
                buf.put_f32_le(*pos_y);
                buf.put_f32_le(*vel_x);
                buf.put_f32_le(*vel_y);
                buf.put_slice(state);
            }
            InterShardPayload::GhostUpdate {
                entity_id,
                pos_x,
                pos_y,
                vel_x,
                vel_y,
            } => {
                buf.put_u8(TAG_GHOST_UPDATE);
                buf.put_u32_le(*entity_id);
                buf.put_f32_le(*pos_x);
                buf.put_f32_le(*pos_y);
                buf.put_f32_le(*vel_x);
                buf.put_f32_le(*vel_y);
            }
            InterShardPayload::HandoffComplete { entity_id } => {
                buf.put_u8(TAG_HANDOFF_COMPLETE);
                buf.put_u32_le(*entity_id);
            }
        }
        buf.freeze().to_vec()
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let mut buf = data;
        let tag = buf.get_u8();

        match tag {
            TAG_HANDOFF_REQUEST => {
                if buf.remaining() < 84 {
                    return None;
                }
                let entity_id = buf.get_u32_le();
                let pos_x = buf.get_f32_le();
                let pos_y = buf.get_f32_le();
                let vel_x = buf.get_f32_le();
                let vel_y = buf.get_f32_le();
                let mut state = [0u8; 64];
                buf.copy_to_slice(&mut state);
                Some(InterShardPayload::HandoffRequest {
                    entity_id,
                    pos_x,
                    pos_y,
                    vel_x,
                    vel_y,
                    state,
                })
            }
            TAG_GHOST_UPDATE => {
                if buf.remaining() < 20 {
                    return None;
                }
                let entity_id = buf.get_u32_le();
                let pos_x = buf.get_f32_le();
                let pos_y = buf.get_f32_le();
                let vel_x = buf.get_f32_le();
                let vel_y = buf.get_f32_le();
                Some(InterShardPayload::GhostUpdate {
                    entity_id,
                    pos_x,
                    pos_y,
                    vel_x,
                    vel_y,
                })
            }
            TAG_HANDOFF_COMPLETE => {
                if buf.remaining() < 4 {
                    return None;
                }
                let entity_id = buf.get_u32_le();
                Some(InterShardPayload::HandoffComplete { entity_id })
            }
            _ => None,
        }
    }
}

// --- TOPICS UTILS ---
/// Transform a readable string into a 32-byte array topic, padding with zeros (e.g. "shard:1" -> [115, 104, 97, 114, 100, 58, 49, 0, 0, ..., 0])
pub fn string_to_topic(s: &str) -> [u8; 32] {
    let mut topic = [0u8; 32];
    let bytes = s.as_bytes();
    let len = bytes.len().min(32);
    topic[..len].copy_from_slice(&bytes[..len]);
    topic
}

/// Transform a 32-byte array topic back into a readable string, trimming trailing zeros (e.g. [115, 104, 97, 114, 100, 58, 49, 0, 0, ..., 0] -> "shard:1")
pub fn topic_to_string(topic: &[u8; 32]) -> String {
    let len = topic.iter().position(|&c| c == 0).unwrap_or(32);
    String::from_utf8_lossy(&topic[..len]).to_string()
}
