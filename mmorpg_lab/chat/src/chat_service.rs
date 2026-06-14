use std::collections::HashMap;

use game_sockets::{GameConnection, GamePeer, GameStream};

use crate::moderator::Moderator;

pub struct ChatService {

    pub usernames : HashMap<u32, String>,

    pub peer : GamePeer,
    pub conn : Option<GameConnection>,
    pub rel_stream : Option<GameStream>,

    pub moderator : Moderator,
}