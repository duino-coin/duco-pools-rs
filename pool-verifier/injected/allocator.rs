// This file is not a standalone module. `prepare::build` appends it to the end of
// the disposable copy's `mining/id_allocator.rs`. That location gives the tests
// access to private allocator operations without changing production visibility.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    #[test]
    fn seeded_sequences() {
        // The real allocator stores half-worker IDs: an ESP worker advances by one
        // half-step and a normal worker by two. This simple ordered-map model is an
        // independent oracle for the optimized production allocator.
        //
        // Each active tuple is (slot, is_esp, half-step ID, allocation width).
        // `free` maps a released ID to its original width. `high` is the highest
        // half-step ever allocated after trimming free slots at the top.
        for seed in [1u64, 17, 0x12345678, 0xdeadbeef] {
            let mut random = seed;
            let mut alloc = WorkerAllocator::new();
            let mut active: Vec<(WorkerSlot, bool, u32, u32)> = vec![];
            let mut free = std::collections::BTreeMap::<u32, u32>::new();
            let mut high = 0;

            for _ in 0..4000 {
                // A fixed LCG makes the operation sequence repeatable while still
                // exercising thousands of mixed allocate/release/recycle paths.
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let action = (random >> 32) % 3;

                if active.is_empty() || action == 0 {
                    let esp = random & 1 == 0;
                    let step = if esp { 1 } else { 2 };
                    let (id, original) = if let Some((&id, &original)) = free.first_key_value() {
                        free.remove(&id);
                        (id, original)
                    } else {
                        high += step;
                        (high, step)
                    };

                    let slot = alloc.allocate(esp);
                    assert_eq!(slot.id(), id as f32 / 2.0);
                    active.push((slot, esp, id, original));
                } else {
                    let index = (random as usize) % active.len();
                    let (slot, esp, id, original) = active[index];
                    if action == 1 {
                        active.swap_remove(index);
                        alloc.release(slot, esp);
                        free.insert(id, original);
                    } else {
                        // Recycling may move an active worker only into a lower
                        // hole. Its old slot then becomes free.
                        let next = free
                            .first_key_value()
                            .map(|(&id, &step)| (id, step))
                            .filter(|(free_id, _)| *free_id < id);
                        let got = alloc.try_recycle(slot);
                        if let Some((next_id, next_step)) = next {
                            free.remove(&next_id);
                            free.insert(id, original);
                            let got = got.expect("allocator missed a lower free slot");
                            assert_eq!(got.id(), next_id as f32 / 2.0);
                            active[index] = (got, esp, next_id, next_step);
                        } else {
                            assert!(got.is_none());
                        }
                    }

                    // Free space at the top is not a reusable hole; it reduces the
                    // model's high-water mark, just as production compaction does.
                    while let Some(step) = free.remove(&high) {
                        high -= step;
                    }
                }

                // Check invariants after every operation so a failure identifies
                // the exact state transition that corrupted the allocator.
                let expected_weight = active
                    .iter()
                    .map(|(_, esp, _, _)| if *esp { 0.5 } else { 1.0 })
                    .sum::<f32>();
                assert_eq!(alloc.active_workers(), expected_weight);
                assert_eq!(alloc.is_empty(), active.is_empty());
                let ids: std::collections::HashSet<_> =
                    active.iter().map(|(_, _, id, _)| *id).collect();
                assert_eq!(ids.len(), active.len(), "duplicate active worker ID");
            }

            // Releasing everything must reset both accounting and the next normal
            // worker ID, regardless of the preceding random history.
            for (slot, esp, _, _) in active {
                alloc.release(slot, esp);
            }
            assert!(alloc.is_empty());
            assert_eq!(alloc.allocate(false).id(), 1.0);
        }
    }
}
