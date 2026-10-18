use std::sync::atomic::{fence, AtomicU64, Ordering};
// written by Opus 5.5, comments were edited because they didnt make sense

// amount of times the reader spins instead of yielding its thread
const BLOCK_HASH_SPINS: u32 = 64;

// hex of the hash the next job builds on, as a seqlock
pub(crate) struct BlockHash {
    sequence: AtomicU64, // odd while a store is writing
    words: [AtomicU64; 5], // the 40 hex bytes
}

impl BlockHash {
    pub(crate) fn new(hex: &[u8; 40]) -> Self {
        // split into 5 chunks
        let (chunks, _) = hex.as_chunks::<8>();
        BlockHash {
            sequence: AtomicU64::new(0),
            words: std::array::from_fn(|i| AtomicU64::new(u64::from_ne_bytes(chunks[i]))),
        }
    }

    pub(crate) fn load(&self) -> [u8; 40] {
        let mut spins = 0;
        loop {
            let before = self.sequence.load(Ordering::Acquire);
            // checks if even
            if before & 1 == 0 {
                let mut hex = [0u8; 40];
                // split `hex` into 5 chunks of 8 bytes and associate each chunk with its word (chunk0, word0; chunk1, word1, ..)
                for (chunk, word) in hex.as_chunks_mut::<8>().0.iter_mut().zip(&self.words) {
                    *chunk = word.load(Ordering::Relaxed).to_ne_bytes();
                }
                fence(Ordering::Acquire);
                // nothing was stored in the meantime, so the words belong together
                if self.sequence.load(Ordering::Relaxed) == before {
                    return hex;
                }
            }
            if spins < BLOCK_HASH_SPINS {
                spins += 1;
                // tell the processor we're waiting
                std::hint::spin_loop();
            } else {
                // tell the os we're waiting (giving up our timeslot)
                std::thread::yield_now();
            }
        }
    }

    pub(crate) fn store(&self, hex: &[u8; 40]) {
        let sequence = self.sequence.load(Ordering::Relaxed);

        // check if a writer is already writing, or if a writer started writing withing the timeframe of us getting the value of `sequence`
        if sequence & 1 == 1
            || self
                .sequence
                .compare_exchange(sequence, sequence + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        fence(Ordering::Release);
        // split `hex` into 5 chunks of 8 bytes and associate each chunk with its word (chunk0, word0; chunk1, word1, ..)
        for (chunk, word) in hex.as_chunks::<8>().0.iter().zip(&self.words) {
            word.store(u64::from_ne_bytes(*chunk), Ordering::Relaxed);
        }
        self.sequence.store(sequence + 2, Ordering::Release);
    }
}
