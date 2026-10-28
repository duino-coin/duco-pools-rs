// gets appended to the microbenchmark copy of `mining/mining.rs`, see `prepare::build_microbench`;
// each ignored test is one sample of one workload: it pushes `Session` through whole shares
// (job request, job, answer, feedback, commit) on the production build, no socket involved,
// and prints one `VERIFY_BENCH {json}` line for `microbench::run` to pick up
#[cfg(test)]
mod verifier_bench {
    use super::*;
    use crate::sync::sync::Curve;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    // shares solved before the timed run, well past WARMUP_SHARES so rewards are paid
    const WARMUP: u64 = 10_000;
    const DEFAULT_SHARES: u64 = 1_000_000;
    // above mcu_pc_step_h and h_ref, a PC miner
    const PC_HASHRATE: f32 = 500_000.0;
    // below mcu_pc_step_h, an ESP32 stays a half worker and goes through the chip id check
    const ESP32_HASHRATE: f32 = 40_000.0;
    // any recent unix time, the clock only moves forward from here
    const START_SECS: f64 = 1_800_000_000.0;

    // the values of config/curve.json, as in the kolka verifier tests
    fn curve() -> Curve {
        Curve {
            pc_base: 3.12,
            mcu_base: 5.91,
            mcu_pc_step_h: 195000.0,
            mcu_pc_step_sharpness: 10.0,
            slow_boost_h: 70.0,
            arduino_h: 340.0,
            cutoff_sharpness: 100.0,
            cutoff_slope: -4.0,
            arduino_bonus: 0.42,
            arduino_bonus_width: 0.2,
            pc_gpu_crossover_h: 2100000.0,
            pc_gpu_crossover_sharpness: 0.9,
            gpu_base: 0.7,
            gpu_growth: 0.9,
            gpu_growth_start: 0.9,
            gpu_growth_softness: 0.2,
            h_ref: 200000.0,
            drop_low: 0.9393,
            drop_high: 0.7456,
            expected_sharetime: 10,
            block_probability: 28110000,
            block_reward: 28.11,
        }
    }

    fn setup(update_miners_stats_every: u16) -> PoolConfig {
        // LAST_BLOCK_HASH starts from the global config
        let _ = CONFIG.set(PoolConfig::default());
        CURVE_VALUES.store(Arc::new(curve()));
        PoolConfig {
            update_miners_stats_every,
            ..PoolConfig::default()
        }
    }

    fn shares() -> u64 {
        std::env::var("VERIFY_BENCH_SHARES")
            .ok()
            .and_then(|shares| shares.parse().ok())
            .unwrap_or(DEFAULT_SHARES)
    }

    fn report(elapsed: Duration, shares: u64) {
        println!(
            "VERIFY_BENCH {{\"ns_per_share\":{},\"shares\":{shares}}}",
            elapsed.as_nanos() as f64 / shares as f64
        );
    }

    /// one miner, driven the way the socket loop in `handle` would; building the answer
    /// line is timed too, but it costs the same on both sides of an A/B run
    struct Miner {
        session: Session,
        stat: MinerStat,
        id: ConnId,
        job_request: String,
        line: String,
        hashrate: f32,
        clock: f64,
    }

    impl Miner {
        fn new(id: u64, software: &str, hashrate: f32) -> Self {
            Miner {
                session: Session::new(),
                stat: MinerStat::new("127.0.0.1".into(), "bench".into()),
                id: ConnId(id),
                job_request: format!("JOB,bench,{software},bench-key"),
                line: String::with_capacity(1024),
                hashrate,
                clock: START_SECS,
            }
        }

        /// requests, solves and answers one job; `correct` picks the right nonce or a
        /// wrong one, and it returns whether the pool took the share
        fn share(&mut self, config: &PoolConfig, correct: bool) -> bool {
            assert!(self
                .session
                .on_job_request(self.id, &mut self.stat, &self.job_request, config)
                .is_ok());
            self.session.write_job(&mut self.line);

            // solved at exactly the miner's hashrate; the clock moves at least a second
            // per share, so the per-minute share limit never kicks in
            let random = self.session.random;
            self.stat.share_time = (random as f32 / self.hashrate).max(MIN_SHARE_TIME);
            self.clock += (self.stat.share_time as f64).max(1.0);

            let nonce = if correct { random } else { random + 1 };
            self.line.clear();
            self.line.push_str(itoa::Buffer::new().format(nonce));
            self.line.push(',');
            self.line.push_str(itoa::Buffer::new().format(self.hashrate as u64));
            self.line.push_str(",Bench Miner 1.0,rig,chip-bench,wallet-bench");

            let Ok(judged) =
                self.session
                    .judge(self.id, &mut self.stat, &self.line, self.clock as u64, config)
            else {
                panic!("answer closed the session");
            };
            let accepted = judged.share_accepted;
            assert!(self
                .session
                .commit(self.id, &mut self.stat, &self.line, &judged, config)
                .is_ok());
            accepted
        }
    }

    fn single(update_miners_stats_every: u16, software: &str, hashrate: f32, correct: bool) {
        let config = setup(update_miners_stats_every);
        let mut miner = Miner::new(0, software, hashrate);
        for _ in 0..WARMUP {
            assert!(miner.share(&config, true));
        }
        let shares = shares();
        let start = Instant::now();
        let mut accepted = 0;
        for _ in 0..shares {
            accepted += miner.share(&config, correct) as u64;
        }
        let elapsed = start.elapsed();
        assert_eq!(accepted, if correct { shares } else { 0 });
        report(elapsed, shares);
    }

    #[test]
    #[ignore]
    fn accepted() {
        single(1, "PC Miner", PC_HASHRATE, true);
    }

    #[test]
    #[ignore]
    fn accepted_stats_every_10() {
        single(10, "PC Miner", PC_HASHRATE, true);
    }

    #[test]
    #[ignore]
    fn rejected() {
        single(1, "PC Miner", PC_HASHRATE, false);
    }

    #[test]
    #[ignore]
    fn esp32() {
        single(1, "ESP32", ESP32_HASHRATE, true);
    }

    /// one miner per logical CPU, all on the same username, so they fight over the same
    /// balance and worker entries; reports wall time per share across all of them
    #[test]
    #[ignore]
    fn contended() {
        let config = setup(1);
        let threads = std::thread::available_parallelism().unwrap().get();
        let shares = shares() / threads as u64;
        let barrier = Barrier::new(threads + 1);
        std::thread::scope(|scope| {
            let miners: Vec<_> = (0..threads)
                .map(|index| {
                    let (config, barrier) = (&config, &barrier);
                    scope.spawn(move || {
                        let mut miner = Miner::new(index as u64, "PC Miner", PC_HASHRATE);
                        for _ in 0..WARMUP {
                            assert!(miner.share(config, true));
                        }
                        barrier.wait();
                        for _ in 0..shares {
                            assert!(miner.share(config, true));
                        }
                        Instant::now()
                    })
                })
                .collect();
            barrier.wait();
            let start = Instant::now();
            let finished = miners
                .into_iter()
                .map(|miner| miner.join().unwrap())
                .max()
                .unwrap();
            report(finished - start, shares * threads as u64);
        });
    }

    /// size of the future that runs one connection, so everything it keeps across
    /// awaits; it's deterministic, so one run per side does it
    #[test]
    #[ignore]
    fn task_size() {
        let _ = CONFIG.set(PoolConfig::default());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let _client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (socket, addr) = listener.accept().await.unwrap();
            let mut conn = Connection::new(socket, addr, CompactString::default(), ConnId(0));
            let task = crate::handler::handler::handle_connection(&mut conn);
            println!(
                "VERIFY_BENCH {{\"bytes\":{}}}",
                std::mem::size_of_val(&task)
            );
        });
    }
}
