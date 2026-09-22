// Appended to the disposable copy of `cfg/config.rs`; see `prepare::build`.
// The internal test process runs in its own empty scenario directory, so relative
// `config/...` paths cannot touch the user's real pool configuration.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    #[test]
    fn defaults_and_roundtrip() {
        // With no file, loading creates a complete config with generated secrets.
        assert!(!Path::new("config/config.json").exists());
        let config = load_config().unwrap();
        assert_eq!(config.max_shares_per_minute, 80);
        assert_eq!(config.update_miners_stats_every, 1);
        assert_eq!(config.dashboard_key.len(), 32);
        assert!(config
            .dashboard_key
            .bytes()
            .all(|byte| (33..=126).contains(&byte)));

        // This is a collision smoke test, not a proof of key entropy.
        let keys: std::collections::HashSet<_> = (0..32).map(|_| generate_key()).collect();
        assert_eq!(keys.len(), 32);

        // Saving and loading must preserve the serialized schema exactly.
        let mut value = serde_json::to_value(&config).unwrap();
        value["poolName"] = serde_json::json!("fixture");
        let changed: PoolConfig = serde_json::from_value(value.clone()).unwrap();
        save_config(&changed);
        assert_eq!(serde_json::to_value(load_config().unwrap()).unwrap(), value);

        // Optional fields regain defaults when omitted; required fields and
        // malformed JSON must fail instead of silently producing partial config.
        value.as_object_mut().unwrap().remove("maxSharesPerMinute");
        value.as_object_mut().unwrap().remove("dashboard_key");
        std::fs::write("config/config.json", serde_json::to_vec(&value).unwrap()).unwrap();
        let config = load_config().unwrap();
        assert_eq!(config.max_shares_per_minute, 80);
        assert_eq!(config.dashboard_key.len(), 32);

        value.as_object_mut().unwrap().remove("host");
        std::fs::write("config/config.json", serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(load_config().is_err());
        std::fs::write("config/config.json", "{").unwrap();
        assert!(load_config().is_err());
    }
}
