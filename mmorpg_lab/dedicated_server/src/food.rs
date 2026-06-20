use bevy::prelude::*;
use shared::FoodData;
use std::collections::HashMap;

use crate::ServerConfig;

const MAX_FOOD_IN_MAP: f32 = 2000.0; // This is the total number of food items we want on the entire map at any time

#[derive(Message)]
pub struct FoodSpawnedMessage(pub FoodData);

#[derive(Message)]
pub struct FoodEatenMessage(pub u32);

#[derive(Resource, Default)]
pub struct FoodRegistry {
    pub food: HashMap<u32, FoodData>,
    pub ordered_ids: Vec<u32>,
    pub next_food_id: u32,
}

pub struct FoodPlugin;

impl Plugin for FoodPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<FoodRegistry>()
            .add_message::<FoodSpawnedMessage>()
            .add_message::<FoodEatenMessage>()
            .add_systems(Update, spawn_food_system);
    }
}

fn spawn_food_system(
    mut registry: ResMut<FoodRegistry>,
    config: Res<ServerConfig>,
    mut spawn_writer: MessageWriter<FoodSpawnedMessage>,
) {
    // Max food items in the shard
    let total_area = shared::MAP_SIZE * shared::MAP_SIZE;
    let shard_area = config.bounds.width * config.bounds.height;

    let area_ratio = shard_area / total_area;

    let max_global_food = MAX_FOOD_IN_MAP;
    let max_local_food = (max_global_food * area_ratio) as usize; // Each shard spawns a proportion of the total food based on its area

    if registry.food.len() < max_local_food {
        // Max 15 food items spawned per tick to avoid lag spikes
        let to_spawn = (max_local_food - registry.food.len()).min(5);

        for _ in 0..to_spawn {
            // Make a unique food ID by combining the shard ID and a local counter
            let food_id = (config.id * 1000000) + registry.next_food_id;
            registry.next_food_id = registry.next_food_id.wrapping_add(1);

            let rx = config.bounds.x + rand::random::<f32>() * config.bounds.width;
            let ry = config.bounds.y + rand::random::<f32>() * config.bounds.height;

            let color_index = rand::random::<u8>() % 5;

            let new_food = FoodData {
                id: food_id,
                x: rx,
                y: ry,
                color_index,
            };
            registry.food.insert(food_id, new_food.clone());
            registry.ordered_ids.push(food_id);
            spawn_writer.write(FoodSpawnedMessage(new_food));
        }
    }
}
