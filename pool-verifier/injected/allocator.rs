// This file is not a standalone module. `prepare::append_injected_tests` appends
// it to the end of the disposable copy's `mining/id_allocator.rs`. That location
// gives the tests access to private allocator operations without changing
// production visibility.
#[cfg(test)]
mod verifier_tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    /// A worker the model believes is active.
    #[derive(Clone, Copy)]
    struct Active {
        slot: WorkerSlot,
        esp: bool,
        /// Position in half-steps (production `slot.id()` is `id / 2`).
        id: u32,
        /// Half-steps this position occupies: 1 for ESP, 2 for normal workers.
        /// A reused position keeps the width it was first allocated with.
        width: u32,
    }

    #[test]
    fn seeded_sequences() {
        // The real allocator stores half-worker IDs: an ESP worker advances by one
        // half-step and a normal worker by two. This simple ordered-map model is an
        // independent oracle for the optimized production allocator.
        //
        // `free` maps a released ID to its original width. `high` is the highest
        // half-step ever allocated after trimming free slots at the top.
        for seed in [1u64, 17, 0x12345678, 0xdeadbeef] {
            let mut random = seed;
            let mut alloc = WorkerAllocator::new();
            let mut active: Vec<Active> = vec![];
            let mut free = BTreeMap::<u32, u32>::new();
            let mut high = 0;

            for _ in 0..4000 {
                // A fixed LCG makes the operation sequence repeatable while still
                // exercising thousands of mixed allocate/release/recycle paths.
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let action = (random >> 32) % 3;

                if active.is_empty() || action == 0 {
                    // Allocate: reuse the lowest free hole, or grow at the top.
                    let esp = random & 1 == 0;
                    let step = if esp { 1 } else { 2 };
                    let (id, width) = match free.pop_first() {
                        Some(hole) => hole,
                        None => {
                            high += step;
                            (high, step)
                        }
                    };
                    let slot = alloc.allocate(esp);
                    assert_eq!(slot.id(), id as f32 / 2.0);
                    active.push(Active {
                        slot,
                        esp,
                        id,
                        width,
                    });
                } else {
                    let index = (random as usize) % active.len();
                    let worker = active[index];
                    if action == 1 {
                        // Release.
                        active.swap_remove(index);
                        alloc.release(worker.slot, worker.esp);
                        free.insert(worker.id, worker.width);
                    } else {
                        // Recycling may move an active worker only into a lower
                        // hole. Its old slot then becomes free.
                        let lower_hole = free
                            .first_key_value()
                            .map(|(&id, &width)| (id, width))
                            .filter(|&(hole_id, _)| hole_id < worker.id);
                        let moved = alloc.try_recycle(worker.slot);
                        if let Some((hole_id, hole_width)) = lower_hole {
                            free.remove(&hole_id);
                            free.insert(worker.id, worker.width);
                            let slot = moved.expect("allocator missed a lower free slot");
                            assert_eq!(slot.id(), hole_id as f32 / 2.0);
                            active[index] = Active {
                                slot,
                                id: hole_id,
                                width: hole_width,
                                ..worker
                            };
                        } else {
                            assert!(moved.is_none());
                        }
                    }

                    // Free space at the top is not a reusable hole; it reduces the
                    // model's high-water mark, just as production compaction does.
                    while let Some(width) = free.remove(&high) {
                        high -= width;
                    }
                }

                // Check invariants after every operation so a failure identifies
                // the exact state transition that corrupted the allocator.
                let expected_weight = active
                    .iter()
                    .map(|w| if w.esp { 0.5 } else { 1.0 })
                    .sum::<f32>();
                assert_eq!(alloc.active_workers(), expected_weight);
                assert_eq!(alloc.is_empty(), active.is_empty());
                let ids: HashSet<_> = active.iter().map(|w| w.id).collect();
                assert_eq!(ids.len(), active.len(), "duplicate active worker ID");
            }

            // Releasing everything must reset both accounting and the next normal
            // worker ID, regardless of the preceding random history.
            for worker in active {
                alloc.release(worker.slot, worker.esp);
            }
            assert!(alloc.is_empty());
            assert_eq!(alloc.allocate(false).id(), 1.0);
        }
    }
}
