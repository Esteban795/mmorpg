use crate::rect::{Rect, Vec2};
use std::sync::atomic::{AtomicU32, Ordering};

// Générateur d'ID unique global pour les nouveaux Shards
static NEXT_SHARD_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Debug, Clone, PartialEq)]
pub enum ShardStatus {
    Active,
    Pending { fallback_shard_id: u32 }, // Server is full, waiting for orchestrator to confirm the server is up before handing off
}

pub struct SplitData {
    pub parent_shard_id: u32,
    pub new_shards_ids: [u32; 4], // IDs of newly created shards
}

pub struct InsertResult {
    pub logical_shard_id: u32,
    pub network_shard_id: u32, // Shard ID to use for network routing (can be fallback if pending)
    pub trigger_orchestrator: Option<SplitData>, // Contains information if split happened
}

pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub max_players_per_shard: usize,
    pub children: Option<Box<[QuadTree; 4]>>,

    // Shard ID only exists if we're in a leaf
    pub shard_id: Option<u32>,

    pub status: ShardStatus,

    // Players on this shard (client_id, position)
    pub players: Vec<(u32, Vec2)>,
}

impl QuadTree {
    pub fn new(bounds: Rect, depth: u8, max_depth: u8, max_players: usize, shard_id: u32) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            max_players_per_shard: max_players,
            children: None,
            shard_id: Some(shard_id),
            status: ShardStatus::Active,
            players: Vec::new(),
        }
    }

    pub fn remove_player(&mut self, client_id: u32) -> bool {
        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if child.remove_player(client_id) {
                    return true;
                }
            }
            return false;
        }

        if let Some(pos) = self.players.iter().position(|p| p.0 == client_id) {
            self.players.remove(pos);
            return true;
        }
        false
    }

    /// Retourne le shard_id contenant la position donnée
    pub fn shard_for(&self, pos: &Vec2) -> Option<u32> {
        if !self.bounds.contains(pos) {
            return None;
        }
        if let Some(children) = &self.children {
            for child in children.iter() {
                if let Some(res) = child.shard_for(pos) {
                    return Some(res);
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

    pub fn insert_player(&mut self, client_id: u32, pos: Vec2) -> Option<InsertResult> {
        if !self.bounds.contains(&pos) {
            return None;
        }

        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if let Some(res) = child.insert_player(client_id, pos) {
                    return Some(res);
                }
            }
            return None;
        }

        if self.players.len() >= self.max_players_per_shard
            && self.depth < self.max_depth
            && self.status == ShardStatus::Active
        {
            let split_data = self.split_logically();

            if let Some(mut recursive_result) = self.insert_player(client_id, pos) {
                recursive_result.trigger_orchestrator = Some(split_data);
                return Some(recursive_result);
            }
        }

        self.players.push((client_id, pos));

        let current_shard = self.shard_id.unwrap();
        let network_shard = match self.status {
            ShardStatus::Active => current_shard,
            ShardStatus::Pending { fallback_shard_id } => fallback_shard_id,
        };

        Some(InsertResult {
            logical_shard_id: current_shard,
            network_shard_id: network_shard,
            trigger_orchestrator: None,
        })
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

    /// Split quadtree and marks children as pending with fallback to parent until orchestrator confirms the split and the new servers are up.
    ///  Returns SplitData to send to orchestrator.
    fn split_logically(&mut self) -> SplitData {
        let parent_id = self.shard_id.unwrap();
        let sub_w = self.bounds.width / 2.0;
        let sub_h = self.bounds.height / 2.0;
        let next_depth = self.depth + 1;

        let id_nw = NEXT_SHARD_ID.fetch_add(1, Ordering::Relaxed);
        let id_ne = NEXT_SHARD_ID.fetch_add(1, Ordering::Relaxed);
        let id_sw = NEXT_SHARD_ID.fetch_add(1, Ordering::Relaxed);
        let id_se = NEXT_SHARD_ID.fetch_add(1, Ordering::Relaxed);

        let create_child = |x, y, w, h, id| -> QuadTree {
            let mut child = QuadTree::new(
                Rect {
                    x,
                    y,
                    width: w,
                    height: h,
                },
                next_depth,
                self.max_depth,
                self.max_players_per_shard,
                id,
            );
            // Mark as pending until orchestrator confirmation
            child.status = ShardStatus::Pending {
                fallback_shard_id: parent_id,
            };
            child
        };

        let nw = create_child(self.bounds.x, self.bounds.y, sub_w, sub_h, id_nw);
        let ne = create_child(self.bounds.x + sub_w, self.bounds.y, sub_w, sub_h, id_ne);
        let sw = create_child(self.bounds.x, self.bounds.y + sub_h, sub_w, sub_h, id_sw);
        let se = create_child(
            self.bounds.x + sub_w,
            self.bounds.y + sub_h,
            sub_w,
            sub_h,
            id_se,
        );

        let mut children = Box::new([nw, ne, sw, se]);

        let old_players = std::mem::take(&mut self.players);
        self.shard_id = None;
        self.status = ShardStatus::Active;

        // Dispatch players into the new children
        // (they will still be on the old shard_id until orchestrator confirmation, but at least we know where they belong for when the time comes)
        for (c_id, p_pos) in old_players {
            for child in children.iter_mut() {
                if child.bounds.contains(&p_pos) {
                    child.players.push((c_id, p_pos));
                    break;
                }
            }
        }

        self.children = Some(children);

        SplitData {
            parent_shard_id: parent_id,
            new_shards_ids: [id_nw, id_ne, id_sw, id_se],
        }
    }

    /// Commit split operation for a single server of id `target_child_id`
    pub fn commit_child_split(&mut self, target_child_id: u32) -> Option<(u32, Vec<(u32, u32)>)> {
        let mut changes = Vec::new();
        if let Some(fallback_id) = self.commit_child_recursive(target_child_id, &mut changes) {
            Some((fallback_id, changes))
        } else {
            None // Child not found or already active
        }
    }

    fn commit_child_recursive(
        &mut self,
        target_child: u32,
        changes: &mut Vec<(u32, u32)>,
    ) -> Option<u32> {
        if self.shard_id == Some(target_child) {
            if let ShardStatus::Pending { fallback_shard_id } = self.status {
                self.status = ShardStatus::Active;
                for &(client_id, _) in &self.players {
                    changes.push((client_id, target_child));
                }

                // Return parent id to unsubscribe players from old shard and subscribe to new shard
                return Some(fallback_shard_id);
            }
        }

        // Not found yet, keep exploring tree
        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if let Some(fallback_id) = child.commit_child_recursive(target_child, changes) {
                    return Some(fallback_id);
                }
            }
        }

        None
    }

    pub fn print_state(&self) {
        println!("\n=== ÉTAT DU QUADTREE (Depth 0/{}) ===", self.max_depth);
        self.print_recursive(0);
        println!("=====================================\n");
    }

    /// Fonction utilitaire récursive pour l'affichage
    fn print_recursive(&self, indent_level: usize) {
        // Création de l'indentation visuelle (2 espaces par niveau de profondeur)
        let indent = "  ".repeat(indent_level);
        let prefix = if indent_level > 0 { "└─ " } else { "" };

        // Formatage lisible de la zone géographique
        let bounds_str = format!(
            "[x:{:.0}, y:{:.0}, w:{:.0}, h:{:.0}]",
            self.bounds.x, self.bounds.y, self.bounds.width, self.bounds.height
        );

        if let Some(children) = &self.children {
            // C'est un nœud parent (il n'a plus de shard_id ni de joueurs)
            println!(
                "{}{}📁 Parent Node {} - Depth: {}",
                indent, prefix, bounds_str, self.depth
            );

            // On descend récursivement dans les 4 enfants
            for child in children.iter() {
                child.print_recursive(indent_level + 1);
            }
        } else {
            // C'est une feuille (un vrai serveur/shard en activité ou en attente)
            let shard_str = match self.shard_id {
                Some(id) => format!("Shard {}", id),
                None => "ERREUR_NO_SHARD".to_string(), // Ne devrait jamais arriver sur une feuille
            };

            let status_str = match self.status {
                ShardStatus::Active => "🟢 Active".to_string(),
                ShardStatus::Pending { fallback_shard_id } => {
                    format!(
                        "🟡 Pending (Réseau pointe vers Shard {})",
                        fallback_shard_id
                    )
                }
            };

            // Extraction des IDs des joueurs pour un affichage propre
            let player_ids: Vec<u32> = self.players.iter().map(|p| p.0).collect();

            println!(
                "{}{}📄 {} {} | {} | Joueurs ({}/{}): {:?}",
                indent,
                prefix,
                shard_str,
                bounds_str,
                status_str,
                self.players.len(),
                self.max_players_per_shard,
                player_ids
            );
        }
    }
}
