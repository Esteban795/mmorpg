use bytes::{Buf, BufMut, BytesMut};

// --- MESSAGES TAGS ---
pub const TAG_REQUEST_SPLIT: u8 = 0x01;
pub const TAG_SPLIT_CONFIRMATION: u8 = 0x02;
pub const TAG_SPLIT_DONE: u8 = 0x03;

#[derive(Debug, Clone)]
pub enum OrchestratorMessage {
    RequestSplit { shard_id: u32 },
    SplitConfirmation { shard_id: u32, new_shard_id: u32 },
    SplitDone { shard_id: u32, new_shard_id: u32 },
}

impl OrchestratorMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        match self {
            OrchestratorMessage::RequestSplit { shard_id } => {
                buf.put_u8(TAG_REQUEST_SPLIT);
                buf.put_u32_le(*shard_id);
            }
            OrchestratorMessage::SplitConfirmation {
                shard_id,
                new_shard_id,
            } => {
                buf.put_u8(TAG_SPLIT_CONFIRMATION);
                buf.put_u32_le(*shard_id);
                buf.put_u32_le(*new_shard_id);
            }
            OrchestratorMessage::SplitDone {
                shard_id,
                new_shard_id,
            } => {
                buf.put_u8(TAG_SPLIT_DONE);
                buf.put_u32_le(*shard_id);
                buf.put_u32_le(*new_shard_id);
            }
        }
        buf.freeze().to_vec()
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut buf = BytesMut::from(bytes);
        if !buf.has_remaining() {
            return None;
        }
        let tag = buf.get_u8();
        match tag {
            TAG_REQUEST_SPLIT => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u32_le();
                Some(OrchestratorMessage::RequestSplit { shard_id })
            }
            TAG_SPLIT_CONFIRMATION => {
                if buf.remaining() < 8 {
                    return None;
                }
                let shard_id = buf.get_u32_le();
                let new_shard_id = buf.get_u32_le();
                Some(OrchestratorMessage::SplitConfirmation {
                    shard_id,
                    new_shard_id,
                })
            }
            TAG_SPLIT_DONE => {
                if buf.remaining() < 8 {
                    return None;
                }
                let shard_id = buf.get_u32_le();
                let new_shard_id = buf.get_u32_le();
                Some(OrchestratorMessage::SplitDone {
                    shard_id,
                    new_shard_id,
                })
            }
            _ => None,
        }
    }
}
