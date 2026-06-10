pub fn get_added_ids(old: &[u32], new: &[u32]) -> Vec<u32> {
    new.iter().filter(|&&id| !old.contains(&id)).cloned().collect()
}

pub fn get_removed_ids(old: &[u32], new: &[u32]) -> Vec<u32> {
    old.iter().filter(|&&id| !new.contains(&id)).cloned().collect()
}