// Appended to the disposable copy of `kolka/kolka.rs`; see `prepare::append_injected_tests`.
// These are white-box formula tests, so their expected values are calculated here
// from independent constants rather than by calling another production helper.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    #[test]
    fn reward_vectors() {
        let kolka = Kolka {
            highest_pc_diff: 1000,
            highest_esp_diff: 100,
            highest_avr_diff: 10,
            gpu_mining_percentage: 0.5,
            pc_mining_percentage: 0.6,
            esp_mining_percentage: 0.7,
            avr_mining_percentage: 0.8,
        };

        // Values immediately below, at, and above each difficulty boundary select
        // the expected device-class percentage.
        for (difficulty, percentage) in [
            (1, 0.8f64),
            (10, 0.8),
            (11, 0.7),
            (100, 0.7),
            (101, 0.6),
            (1000, 0.6),
            (1001, 0.5),
        ] {
            // Worker counts cross the two compounding boundaries. Half workers are
            // included because ESP-class miners contribute a weight of 0.5.
            for workers in [0.5f32, 1.0, 1.5, 2.0, 2.5, 3.0] {
                for divisor in [100u32, 1000] {
                    let exponent = match workers {
                        x if x < 2.0 => 0,
                        x if x < 3.0 => 1,
                        _ => 2,
                    };
                    let expected = 2.0 * 1000f64.ln() / divisor as f64 * percentage.powi(exponent);
                    let actual = v1(&kolka, 1000.0, difficulty, workers, divisor) as f64;
                    assert!(
                        (actual - expected).abs() < expected.abs() * 1e-6,
                        "diff={difficulty}, workers={workers}, divisor={divisor}: \
                         {actual} != {expected}"
                    );
                }
            }
        }
    }

    #[test]
    fn tier_transitions() {
        // The explicit table makes asymmetric and terminal transitions visible.
        // In particular, not every tier's down-transition is the previous row.
        for (tier, up, down) in [
            ("AVR", "MEGA", "AVR"),
            ("MEGA", "ARM", "AVR"),
            ("ARM", "DUE", "MEGA"),
            ("DUE", "ESP8266", "ARM"),
            ("ESP8266", "ESP8266H", "ESP8266"),
            ("ESP8266H", "ESP8266H", "ESP8266"),
            ("ESP8266N", "ESP8266NH", "ESP8266N"),
            ("ESP8266NH", "ESP8266NH", "ESP8266N"),
            ("ESP32", "ESP32S", "ESP32"),
            ("ESP32S", "ESP32S", "ESP32S"),
            ("LOW", "MEDIUM", "LOW"),
            ("MEDIUM", "NET", "LOW"),
            ("NET", "EXTREME", "MEDIUM"),
            ("EXTREME", "EXTREME", "NET"),
            ("UNKNOWN", "UNKNOWN", "UNKNOWN"),
        ] {
            assert_eq!(v2(tier), up);
            assert_eq!(v2_reverse(tier), down);
        }
    }

    #[test]
    fn adaptive_vectors() {
        // Pin both sides of the target-time band: fast shares increase difficulty,
        // values inside the band hold it, and slow shares decrease it.
        for (share_time, expected) in [(8.99, 12800), (9.0, 10000), (10.0, 10000), (10.01, 7200)] {
            assert_eq!(
                v3(share_time, 10, 100000.0),
                expected,
                "share_time={share_time}"
            );
        }
        assert_eq!(v3(0.0, 1, 1.0), 5000);
        // Hashrate is floored before scaling, which keeps this at 10,000.
        assert_eq!(v3(9.0, 10, 100099.0), 10000);
    }
}
