// Appended to the disposable copy of `kolka/kolka.rs`; see `prepare::append_injected_tests`.
// These are white-box formula tests, so their expected values are calculated here
// from independent constants rather than by calling another production helper.
#[cfg(test)]
mod verifier_tests {
    use super::*;
    use std::sync::Arc;

    // the values of config/curve.json, spelled out so a changed default can't move the vectors
    fn curve() -> Arc<Curve> {
        Arc::new(Curve {
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
        })
    }

    fn close(actual: f32, expected: f32, label: &str) {
        assert!(
            (actual - expected).abs() <= expected.abs() * 1e-5,
            "{label}: {actual} != {expected}"
        );
    }

    #[test]
    fn reward_vectors() {
        let curve = curve();

        // A difficulty of 1728 averages 1728 * 50 = 86400 hashes per share, a whole
        // day at 1 H/s, so a single worker's share pays exactly one daily reward.
        let day = calculate_daily_reward(1.0, &curve) as f32;
        assert!(day.is_finite() && day > 0.0, "daily reward {day}");
        close(calculate_reward(1.0, 1.0, 1728, &curve), day, "one day");
        close(calculate_reward(1.0, 1.0, 3456, &curve), 2.0 * day, "two days");

        // h_ref itself still uses drop_low, anything faster drop_high. Only whole
        // additional workers compound the drop, so ESP half workers round down.
        for (hashrate, drop) in [(200_000f32, 0.9393f32), (200_001.0, 0.7456)] {
            let single = calculate_reward(hashrate, 1.0, 1000, &curve);
            assert!(single > 0.0, "hashrate={hashrate}: {single}");
            for (workers, exponent) in [(0.5f32, 0), (1.0, 0), (1.5, 0), (2.0, 1), (2.5, 1), (3.0, 2)] {
                close(
                    calculate_reward(hashrate, workers, 1000, &curve),
                    single * drop.powi(exponent),
                    &format!("hashrate={hashrate}, workers={workers}"),
                );
            }
        }

        // A non-finite reward would stick to the balance, so it pays nothing.
        assert_eq!(calculate_reward(f32::NAN, 1.0, 1000, &curve), 0.0);
        assert_eq!(calculate_reward(0.0, 1.0, 1000, &curve), 0.0);
    }

    #[test]
    fn difficulty_vectors() {
        // hashrate * expected_sharetime / 50, rounded half away from zero, at least 1
        let curve = curve();
        for (hashrate, expected) in [
            (1000.0, 200),
            (12.5, 3),
            (12.4, 2),
            (2.0, 1),
            (0.0, 1),
            (f64::MAX, u32::MAX),
        ] {
            assert_eq!(
                calculate_difficulty(hashrate, &curve),
                expected,
                "hashrate={hashrate}"
            );
        }
        let short = Arc::new(Curve {
            expected_sharetime: 1,
            ..*curve
        });
        assert_eq!(calculate_difficulty(1000.0, &short), 20);
    }
}
