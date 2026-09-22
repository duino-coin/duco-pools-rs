// Appended to the disposable copy of `mining/mining.rs`; see `prepare::build`.
// Injection keeps these white-box checks next to private helpers without exporting
// those helpers from the production crate.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    #[test]
    fn fallback_and_sanitizers() {
        // Give each tier a distinctive difficulty so the selected fallback can be
        // identified from the result alone.
        let map = DashMap::new();
        let difficulty = |difficulty| Difficulty {
            difficulty,
            reward: 1000,
            min_hashrate: 1,
            max_hashrate: 10000,
            kolka_decrease_perc: 90,
            expected_share_time: 1,
            block_chance: 1,
        };
        map.insert("NET".into(), difficulty(13));
        map.insert("AVR".into(), difficulty(1));
        map.insert("MEGA".into(), difficulty(2));

        // Selection order is requested tier, explicit override, then NET. Missing
        // all three returns None rather than manufacturing a configuration.
        for (requested, override_tier, expected) in [
            ("AVR", "MEGA", 1),
            ("missing", "MEGA", 2),
            ("missing", "absent", 13),
            ("missing", "", 13),
        ] {
            assert_eq!(
                resolve_reward_info(&map, requested, override_tier)
                    .unwrap()
                    .difficulty,
                expected
            );
        }
        map.clear();
        assert!(resolve_reward_info(&map, "AVR", "MEGA").is_none());

        // Difficulty labels accept exactly 16 characters and clear stale output
        // after rejection. The name cleaners intentionally preserve only the
        // punctuation allowed by the miner protocol.
        let mut out = CompactString::from("stale");
        assert!(set_req_difficulty(&mut out, &"x".repeat(16)));
        assert_eq!(out.len(), 16);
        assert!(!set_req_difficulty(&mut out, &"x".repeat(17)));
        assert!(out.is_empty());
        clean_name_into("A_z!?é.(x)- 1", &mut out);
        assert_eq!(out, "Az.(x)- 1");
        clean_iot_into("T:21@a_!é.(x)-", &mut out);
        assert_eq!(out, "T:21@a.(x)-");

        // Pin both the equal-input special case and asymmetric percent formula.
        assert_eq!(perc_diff(100.0, 100.0), 0.0);
        assert_eq!(perc_diff(100.0, 200.0), 66.0);
    }
}
