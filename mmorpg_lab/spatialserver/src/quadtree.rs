use crate::rect::{Rect, Vec2};
use std::sync::atomic::{AtomicU32, Ordering};

// Générateur d'ID unique global pour les nouveaux Shards
static NEXT_SHARD_ID: AtomicU32 = AtomicU32::new(1);

pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub max_players_per_shard: usize,
    pub children: Option<Box<[QuadTree; 4]>>,

    // Shard ID only exists if we're in a leaf
    pub shard_id: Option<u32>,

    // Players on this shard (client_id, position)
    pub players: Vec<(u32, Vec2)>,
}

impl QuadTree {
    pub fn new(bounds: Rect, depth: u8, max_depth: u8, max_players: usize) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            max_players_per_shard: max_players,
            children: None,
            shard_id: Some(NEXT_SHARD_ID.fetch_add(1, Ordering::Relaxed)),
            players: Vec::new(),
        }
    }

    /// Retourne le shard_id contenant la position donnée
    pub fn shard_for(&self, pos: &Vec2) -> Option<u32> {
        if !self.bounds.contains(pos) {
            return None;
        }
        if let Some(children) = &self.children {
            for child in children.iter() {
                if let Some(id) = child.shard_for(pos) {
                    return Some(id);
                }
            }
        }
        self.shard_id
    }

    /// Retourne les shard_ids distincts dans un rayon `margin` autour de `pos`.
    /// Utilisé pour détecter l'approche d'une frontière inter-shard.
    pub fn shards_near(&self, pos: &Vec2, margin: f32) -> Vec<u32> {
        let search_area = Rect {
            x: pos.x - margin,
            y: pos.y - margin,
            width: margin * 2.0,
            height: margin * 2.0,
        };

        let mut results = Vec::new();
        self.collect_shards_near(&search_area, &mut results);

        results.sort_unstable();
        results.dedup();
        results
    }

    fn collect_shards_near(&self, search_area: &Rect, results: &mut Vec<u32>) {
        if !self.bounds.intersects(search_area) {
            return;
        }

        if let Some(children) = &self.children {
            for child in children.iter() {
                child.collect_shards_near(search_area, results);
            }
        } else if let Some(shard_id) = self.shard_id {
            results.push(shard_id);
        }
    }

    /// Inserts the player into the QuadTree and returns a list of (client_id, new_shard_id) pairs for ALL players affected by this insertion
    pub fn insert(&mut self, client_id: u32, pos: Vec2) -> Vec<(u32, u32)> {
        let mut changes = Vec::new();
        self.insert_recursive(client_id, pos, &mut changes);
        changes
    }

    fn insert_recursive(
        &mut self,
        client_id: u32,
        pos: Vec2,
        changes: &mut Vec<(u32, u32)>,
    ) -> bool {
        if !self.bounds.contains(&pos) {
            return false;
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if child.insert_recursive(client_id, pos, changes) {
                    return true;
                }
            }
            return false;
        }

        // Does this player exist already? If so, update position. Otherwise, add new player.
        if let Some(existing) = self.players.iter_mut().find(|p| p.0 == client_id) {
            existing.1 = pos;
        } else {
            self.players.push((client_id, pos));
        }

        // Split logic : if we exceed max players and haven't reached max depth, split the node
        if self.players.len() > self.max_players_per_shard && self.depth < self.max_depth {
            self.split(changes);
        } else {
            // If max depth, we don't split more, but we still need to record the shard assignment for this player
            if let Some(shard) = self.shard_id {
                changes.push((client_id, shard));
            }
        }
        true
    }


    /// Split this node into 4 children and redistribute players. Record all player movements in `changes`
    fn split(&mut self, changes: &mut Vec<(u32, u32)>) {

        // Create 4 child quadrants
        let sub_w = self.bounds.width / 2.0;
        let sub_h = self.bounds.height / 2.0;
        let next_depth = self.depth + 1;

        let nw = Rect {
            x: self.bounds.x,
            y: self.bounds.y,
            width: sub_w,
            height: sub_h,
        };
        let ne = Rect {
            x: self.bounds.x + sub_w,
            y: self.bounds.y,
            width: sub_w,
            height: sub_h,
        };
        let sw = Rect {
            x: self.bounds.x,
            y: self.bounds.y + sub_h,
            width: sub_w,
            height: sub_h,
        };
        let se = Rect {
            x: self.bounds.x + sub_w,
            y: self.bounds.y + sub_h,
            width: sub_w,
            height: sub_h,
        };

        let mut children = Box::new([
            QuadTree::new(nw, next_depth, self.max_depth, self.max_players_per_shard),
            QuadTree::new(ne, next_depth, self.max_depth, self.max_players_per_shard),
            QuadTree::new(sw, next_depth, self.max_depth, self.max_players_per_shard),
            QuadTree::new(se, next_depth, self.max_depth, self.max_players_per_shard),
        ]);



        let old_players = std::mem::take(&mut self.players);
        self.shard_id = None; // Mark this node as non-leaf since it now has children

        // Transfer players to the newly created children and record any shard changes
        for (c_id, p_pos) in old_players {
            for child in children.iter_mut() {
                if child.bounds.contains(&p_pos) {
                    child.players.push((c_id, p_pos));
                    // On enregistre le changement pour déclencher le Mass Handoff
                    changes.push((c_id, child.shard_id.unwrap()));
                    break;
                }
            }
        }

        self.children = Some(children);
    }
}
