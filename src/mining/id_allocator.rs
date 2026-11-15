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
    if is_esp32 {
        slot | ESP32_BIT
    } else {
        slot
    }
}

fn decode(encoded: u32) -> (u32, u32) {
    (
        encoded & SLOT_MASK,
        if encoded & ESP32_BIT != 0 { 1 } else { 2 },
    )
}

fn slot_of(encoded: u32) -> u32 {
    encoded & SLOT_MASK
}

// Replaces the f32 ID stored in the connection
//
// The packed value remembers how this slot was originally created,
// even when a different type of miner later occupies it
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct WorkerSlot(u32);

impl WorkerSlot {
    pub fn id(self) -> f32 {
        slot_of(self.0) as f32 / 2.0
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkerAllocator {
    // High-water mark in half-steps. Next fresh slot = next_slot + step.
    next_slot: u32,
    // Recycled slots (packed with step), sorted descending by slot value.
    // The lowest slot is at the end so allocation can use pop() instead of remove(0)
    free_slots: Vec<u32>,
    // Number of active half-steps (for max_workers check).
    active_half_steps: u32,
}
impl WorkerAllocator {
    // Allocate the lowest available ID.
    // Returns the f32 ID (e.g. 1.0, 1.5, 2.0, ...).
    pub fn allocate(&mut self, is_esp32: bool) -> WorkerSlot {
        let step = if is_esp32 { 1 } else { 2 };

        let packed = match self.free_slots.pop() {
            // Keep the original slot metadata.
            Some(packed) => packed,

            None => {
                self.next_slot += step;

                // Record the creation step for this new slot.
                encode(self.next_slot, is_esp32)
            }
        };

        self.active_half_steps += step;
        WorkerSlot(packed)
    }
    // Release an ID back into the pool.
    pub fn release(&mut self, slot: WorkerSlot, is_esp32: bool) {
        // Worker count uses the CURRENT miner's type.
        let worker_weight = if is_esp32 { 1 } else { 2 };
        self.active_half_steps = self.active_half_steps.saturating_sub(worker_weight);

        // Compaction uses the slot's ORIGINAL creation step.
        // Return its packed value unchanged.
        self.insert_free(slot.0);
        self.compact();
    }
    // Swap the miner's current ID for the lowest available free ID,
    // if one is lower. Returns `Some(new_id)` on swap, `None` if
    // the miner already holds the best available ID.
    pub fn try_recycle(&mut self, current: WorkerSlot) -> Option<WorkerSlot> {
        let lowest_free = *self.free_slots.last()?;

        if slot_of(lowest_free) >= slot_of(current.0) {
            return None;
        }

        self.free_slots.pop();

        // Return the old slot with its own original metadata.
        self.insert_free(current.0);
        self.compact();

        // The miner now carries the replacement slot's metadata.
        Some(WorkerSlot(lowest_free))
    }

    fn insert_free(&mut self, packed: u32) {
        let slot = slot_of(packed);

        let position = self
            .free_slots
            .binary_search_by_key(&std::cmp::Reverse(slot), |&entry| {
                std::cmp::Reverse(slot_of(entry))
            })
            .unwrap_or_else(|position| position);

        self.free_slots.insert(position, packed);
    }

    // Shrink the high-water mark by popping free slots off the top.
    fn compact(&mut self) {
        let mut freed = 0;
        for &encoded in &self.free_slots {
            let (slot, step) = decode(encoded);
            if slot != self.next_slot {
                break;
            }
            self.next_slot -= step;
            freed += 1;
        }
        // one shift for all of them, a remove(0) each is quadratic for a user with many slots
        self.free_slots.drain(..freed);
    }
    pub fn active_workers(&self) -> f32 {
        self.active_half_steps as f32 / 2.0
    }
    pub fn is_empty(&self) -> bool {
        self.active_half_steps == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{WorkerAllocator, WorkerSlot};

    #[test]
    fn recycles_lowest_slot_first() {
        let mut alloc = WorkerAllocator::default();
        let a = alloc.allocate(false);
        let b = alloc.allocate(false);
        let c = alloc.allocate(false);
        assert_eq!((a.id(), b.id(), c.id()), (1.0, 2.0, 3.0));

        alloc.release(b, false);
        alloc.release(a, false);

        assert_eq!(alloc.allocate(false).id(), 1.0);
        assert_eq!(alloc.allocate(false).id(), 2.0);
    }

    #[test]
    fn mixed_steps_preserve_half_slots() {
        let mut alloc = WorkerAllocator::default();
        let pc = alloc.allocate(false);
        let esp = alloc.allocate(true);
        assert_eq!(pc.id(), 1.0);
        assert_eq!(esp.id(), 1.5);

        alloc.release(pc, false);
        assert_eq!(alloc.try_recycle(esp).map(WorkerSlot::id), Some(1.0));
    }

    #[test]
    fn compact_keeps_fresh_allocations_low_after_releasing_highest_slot() {
        let mut alloc = WorkerAllocator::default();

        assert_eq!(alloc.allocate(false).id(), 1.0);
        assert_eq!(alloc.allocate(false).id(), 2.0);

        let highest = alloc.allocate(false);
        assert_eq!(highest.id(), 3.0);

        alloc.release(highest, false);

        assert_eq!(alloc.allocate(false).id(), 3.0);
    }
}
