use serde::Serialize;

// Per-user worker ID allocator.
//
// IDs are stored internally as half-step slots (slot = id × 2)
// so that ESP32's 0.5-step and PC's 1.0-step both map to integers.
//
// Free slots are packed into a single u32:
//   bit 31    = ESP32 flag (step=1 if set, step=2 if clear)
//   bits 0-30 = slot value
//
const ESP32_BIT: u32 = 1 << 31;
const SLOT_MASK: u32 = !ESP32_BIT;

fn encode(slot: u32, is_esp32: bool) -> u32 {
    if is_esp32 { slot | ESP32_BIT } else { slot }
}

fn decode(encoded: u32) -> (u32, u32) {
    (encoded & SLOT_MASK, if encoded & ESP32_BIT != 0 { 1 } else { 2 })
}

fn slot_of(encoded: u32) -> u32 {
    encoded & SLOT_MASK
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerAllocator {
    // High-water mark in half-steps. Next fresh slot = next_slot + step.
    next_slot: u32,
    // Recycled slots (packed with step), sorted ascending by slot value.
    free_slots: Vec<u32>,
    // Number of active half-steps (for max_workers check).
    active_half_steps: u32,
}
impl WorkerAllocator {
    pub fn new() -> Self {
        WorkerAllocator {
            next_slot: 0,
            free_slots: Vec::new(),
            active_half_steps: 0,
        }
    }
    // Allocate the lowest available ID.
    // Returns the f32 ID (e.g. 1.0, 1.5, 2.0, ...).
    pub fn allocate(&mut self, is_esp32: bool) -> f32 {
        let step = if is_esp32 { 1u32 } else { 2 };
        self.active_half_steps += step;
        // Grab the smallest recycled slot if one exists
        let slot = if let Some(&encoded) = self.free_slots.first() {
            self.free_slots.remove(0);
            slot_of(encoded)
        } else {
            // Advance the high-water mark
            self.next_slot += step;
            self.next_slot
        };
        slot as f32 / 2.0
    }
    // Release an ID back into the pool.
    pub fn release(&mut self, id: f32, is_esp32: bool) {
        let step = if is_esp32 { 1u32 } else { 2 };
        self.active_half_steps = self.active_half_steps.saturating_sub(step);
        let slot = (id * 2.0) as u32;
        let encoded = encode(slot, is_esp32);
        let pos = self.free_slots.binary_search_by_key(&slot, |&e| slot_of(e))
            .unwrap_or_else(|p| p);
        self.free_slots.insert(pos, encoded);
        self.compact();
    }
    // Swap the miner's current ID for the lowest available free ID,
    // if one is lower. Returns `Some(new_id)` on swap, `None` if
    // the miner already holds the best available ID.
    pub fn try_recycle(&mut self, current_id: f32, is_esp32: bool) -> Option<f32> {
        let current_slot = (current_id * 2.0) as u32;
        // Is there a free slot lower than ours?
        let lowest_free = slot_of(*self.free_slots.first()?);
        if lowest_free >= current_slot {
            return None; // nothing lower available
        }
        // Take the lower slot
        self.free_slots.remove(0);
        // Put our current slot back (sorted insert)
        let encoded = encode(current_slot, is_esp32);
        let pos = self.free_slots.binary_search_by_key(&current_slot, |&e| slot_of(e))
            .unwrap_or_else(|p| p);
        self.free_slots.insert(pos, encoded);
        self.compact();
        Some(lowest_free as f32 / 2.0)
    }
    // Shrink the high-water mark by popping free slots off the top.
    fn compact(&mut self) {
        while let Some(&encoded) = self.free_slots.last() {
            let (slot, step) = decode(encoded);
            if slot == self.next_slot {
                self.free_slots.pop();
                self.next_slot -= step;
            } else {
                break;
            }
        }
    }
    pub fn active_workers(&self) -> f32 {
        self.active_half_steps as f32 / 2.0
    }
    pub fn is_empty(&self) -> bool {
        self.active_half_steps == 0
    }
}
impl Default for WorkerAllocator {
    fn default() -> Self {
        Self::new()
    }
}
