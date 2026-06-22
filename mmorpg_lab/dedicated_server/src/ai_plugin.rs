use bevy::prelude::*;
use std::collections::HashMap;

use crate::network::{EntityState, PlayerData, PlayerRegistry};
use crate::food::FoodRegistry;
use crate::ServerConfig;
use shared::{MAP_BOUND_MAX, MAP_BOUND_MIN};

const DEFAULT_START_BOT_ID: u32 = 2_000_000_000; // We give bots very high IDs to avoid conflicts with real players, whose IDs are assigned by the broker starting from 1 and incrementing upwards.

const BASE_PLAYER_SPEED: f32 = 5.0; // Match the ones in network.rs
const BASE_PLAYER_RADIUS: f32 = 15.0;
const BIGGER_RADIUS_THRESHOLD: f32 = 1.10;
const SMALLER_RADIUS_THRESHOLD: f32 = 1.10;
const DEFAULT_MAX_SPAWN_MASS: f32 = 30.0;
const DEFAULT_SPAWN_RATE: f32 = 1.5; // How many bots to spawn per second when underpopulated
const DEFAULT_FARWAY_THRESHOLD: f32 = 400.0; // Distance beyond which we consider players to be too far away to interact with, for optimization purposes. This should be at least the diagonal of the shard to avoid blind spots, but can be tweaked for performance if needed.

const DEFAULT_GLOBAL_MAX_BOTS: f32 = 10.0;

#[derive(Clone)]
pub struct BotBrain {
    pub repath_timer: Timer,
    pub target_pos: Option<Vec2>,
}

#[derive(Resource)]
pub struct BotRegistry {
    // Maps the bot's unique client_id to its brain
    pub brains: HashMap<u32, BotBrain>,
    pub next_bot_id: u32,
    pub spawn_timer: Timer,
}

impl Default for BotRegistry {
    fn default() -> Self {
        Self {
            brains: HashMap::new(),
            next_bot_id: 0,
            spawn_timer: Timer::from_seconds(DEFAULT_SPAWN_RATE, TimerMode::Repeating), 
        }
    }
}

pub struct AiPlugin;

impl Plugin for AiPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<BotRegistry>()
            .add_systems(Update, (manage_bot_population, bot_think_system).chain());
    }
}

// -------------------------------------------------------------------------
// The Agario Brain (Think & Move)
// -------------------------------------------------------------------------
fn bot_think_system(
    mut registry: ResMut<PlayerRegistry>,
    mut bot_registry: ResMut<BotRegistry>,
    food_registry: Res<FoodRegistry>,
    time: Res<Time>,
) {
    let mut dead_bots = Vec::new();

    // ==========================================
    // PASS 1: THINK (Read-Only on PlayerRegistry)
    // ==========================================
    for (bot_id, brain) in bot_registry.brains.iter_mut() {
        
        // Get our bot's player data. If it doesn't exist, mark this bot for deletion and skip it.
        let Some(bot_player) = registry.players.get(bot_id) else {
            dead_bots.push(*bot_id);
            continue;
        };
        
        if bot_player.state == EntityState::Ghost {
            continue;
        }

        brain.repath_timer.tick(time.delta());

        //If its time to rethink our strategy, or if we have no target, pick a new one.
        if brain.repath_timer.just_finished() || brain.target_pos.is_none() {
            let my_pos = bot_player.position;
            let my_radius = BASE_PLAYER_RADIUS + bot_player.score;
            
            let mut best_target = None;
            let mut highest_priority = f32::MIN;

            // First we look for players to eat or run from, then we look for food, then we just wander randomly if there is nothing else to do.
            for (&other_id, other_player) in registry.players.iter() {
                //If its ourself, skip
                if other_id == *bot_id {
                    continue;
                }

                //If the player is objectively too far away to interact with, skip them to save CPU
                let dist = my_pos.distance(other_player.position);
                if dist > DEFAULT_FARWAY_THRESHOLD { continue; } 

                let other_radius = BASE_PLAYER_RADIUS + other_player.score;

                //If the other player's radius is BIGGER_RADIUS_THRESHOLD times bigger than my radius, flee
                //If its SMALLER_RADIUS_THRESHOLD times smaller, chase and try to eat them. We also factor in distance to prefer closer targets.
                if other_radius > my_radius * BIGGER_RADIUS_THRESHOLD {
                    let flee_dir = (my_pos - other_player.position).normalize_or_zero();
                    best_target = Some(my_pos + (flee_dir * 500.0));
                    highest_priority = 1000.0 - dist; 
                } 
                else if my_radius > other_radius * SMALLER_RADIUS_THRESHOLD && highest_priority < 500.0 {
                    best_target = Some(other_player.position);
                    highest_priority = 500.0 - dist;
                }
            }

            //If we found nothing (no players to eat or flee from in the radius), look for the closest food
            if best_target.is_none() {
                let mut closest_food_dist = f32::MAX;
                for food in food_registry.food.values() {
                    let food_pos = Vec2::new(food.x, food.y);
                    let dist = my_pos.distance(food_pos);
                    if dist < closest_food_dist {
                        closest_food_dist = dist;
                        best_target = Some(food_pos);
                    }
                }
            }

            //If we still have nothing, just pick a random point in the shard to wander to. 
            if best_target.is_none() {
                let rx = MAP_BOUND_MIN + rand::random::<f32>() * (MAP_BOUND_MAX - MAP_BOUND_MIN);
                let ry = MAP_BOUND_MIN + rand::random::<f32>() * (MAP_BOUND_MAX - MAP_BOUND_MIN);
                best_target = Some(Vec2::new(rx, ry));
            }

            brain.target_pos = best_target;
        }
    }

    // ==========================================
    // PASS 2: ACT (Write-Only on PlayerRegistry)
    // ==========================================
    for (bot_id, brain) in bot_registry.brains.iter() {
        if let Some(target) = brain.target_pos {
            if let Some(bot_player) = registry.players.get_mut(bot_id) {
                //If we are a ghost, our movement doesnt belong to the current shard, so we skip it to avoid weird teleporting bugs when crossing shard boundaries.
                if bot_player.state == EntityState::Ghost {
                    continue;
                }

                let dir = (target - bot_player.position).normalize_or_zero();
                let current_speed = (BASE_PLAYER_SPEED / (1.0 + (bot_player.score * 0.005))).max(1.0);

                bot_player.velocity = Vec2::new(dir.x * current_speed, dir.y * current_speed);

                bot_player.position.x = (bot_player.position.x + bot_player.velocity.x)
                    .clamp(MAP_BOUND_MIN, MAP_BOUND_MAX);
                bot_player.position.y = (bot_player.position.y + bot_player.velocity.y)
                    .clamp(MAP_BOUND_MIN, MAP_BOUND_MAX);
            }
        }
    }

    // Cleanup dead bots
    for id in dead_bots {
        bot_registry.brains.remove(&id);
    }
    
    // ==========================================
    // PASS 3: AUTO-ADOPTION
    // ==========================================
    // If a bot just crossed the border, we need to pass it brains so it continues to function in the new shard instead of becoming a useless NPC.
    for (&player_id, player_data) in registry.players.iter() {
        // TO-DO : make a more robust way to identify bots instead of relying on their username starting with "Bot_". Maybe a specific flag in the PlayerData that only bots have?
        if player_data.username.starts_with("Bot_") 
           && player_data.state != EntityState::Ghost
           && !bot_registry.brains.contains_key(&player_id) {
            
            bot_registry.brains.insert(
                player_id,
                BotBrain {
                    repath_timer: Timer::from_seconds(0.2, TimerMode::Repeating),
                    target_pos: None,
                },
            );
        }
    }
}

// -------------------------------------------------------------------------
// Smart Population Manager (Prevents the QuadTree Bomb)
// -------------------------------------------------------------------------
fn manage_bot_population(
    mut registry: ResMut<PlayerRegistry>,
    mut bot_registry: ResMut<BotRegistry>,
    config: Res<ServerConfig>,
    time: Res<Time>,
) {
    //Count entities and classify their states
    let mut human_count = 0;
    let mut ghost_human_count = 0;
    let mut bot_count = 0;

for (&id, player_data) in registry.players.iter() {
        if id >= DEFAULT_START_BOT_ID {
            //Only count owned bots, global bot count is managed mathematically using the local_bot_cap
            if player_data.state == EntityState::Owned {
                bot_count += 1; 
            }
        } else {
            if player_data.state == EntityState::Ghost {
                ghost_human_count += 1; // A player from a neighbor shard is looking at us
            } else {
                human_count += 1; // A player is fully inside our shard
            }
        }
    }

    // Calculate baseline capacity based on physical shard area
    let total_area = shared::MAP_SIZE * shared::MAP_SIZE;
    let shard_area = config.bounds.width * config.bounds.height;
    let area_ratio = shard_area / total_area;

    let max_global_bots = DEFAULT_GLOBAL_MAX_BOTS;
    let local_bot_cap = (max_global_bots * area_ratio).ceil() as f32;

    // Determine target bot population based on player presence and apply "Crowd Pressure" mechanics
    let target_bots = if human_count > 0 {
        // ACTIVE: Players are here.
        // Apply "Crowd Pressure": Every 1 human takes up the space of 3 bots.
        let human_pressure = (human_count as f32) * 3.0;
        (local_bot_cap - human_pressure).max(0.0) as usize

    } else if ghost_human_count > 0 {
        // WATCHED: Shard is empty, but a player is in the margin looking across the border!
        // Ramp capacity to 50% so they see an active ecosystem.
        (local_bot_cap * 0.50).max(0.0) as usize

    } else {
        // DORMANT: Deep in the map, completely out of sight.
        // Maintain a tiny 15% ambient population.
        (local_bot_cap * 0.15).max(0.0) as usize
    };

    bot_registry.spawn_timer.tick(time.delta());

    // SPAWN: Smoothly spawn 1 bot per tick if we are under target
    if bot_count < target_bots && bot_registry.spawn_timer.just_finished() {
        let base_id = DEFAULT_START_BOT_ID + (config.id * 1_000_000);
        let bot_id = base_id + bot_registry.next_bot_id;
        bot_registry.next_bot_id = bot_registry.next_bot_id.wrapping_add(1);

        let spawn_x = config.bounds.x + (rand::random::<f32>() * config.bounds.width);
        let spawn_y = config.bounds.y + (rand::random::<f32>() * config.bounds.height);

        registry.players.insert(
            bot_id,
            PlayerData {
                username: format!("Bot_{}", bot_id),
                position: Vec2::new(spawn_x, spawn_y),
                velocity: Vec2::new(0.0, 0.0),
                state: EntityState::Owned,
                score: rand::random::<f32>() * DEFAULT_MAX_SPAWN_MASS, // Reduced max spawn mass so they don't instakill
            },
        );

        bot_registry.brains.insert(
            bot_id,
            BotBrain {
                repath_timer: Timer::from_seconds(0.2, TimerMode::Repeating),
                target_pos: None,
            },
        );
    }
    
    // CULLING: Aggressively despawn bots if humans flood the shard
    if bot_count > target_bots + 2 {
        // bot_registry.brains ONLY tracks bots we currently Own.
        if let Some(&bot_id) = bot_registry.brains.keys().next() {
            bot_registry.brains.remove(&bot_id);
            registry.players.remove(&bot_id);
        }
    }
}