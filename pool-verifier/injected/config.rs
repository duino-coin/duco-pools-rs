// Appended to the disposable copy of `cfg/config.rs`; see `prepare::append_injected_tests`.
// The internal test process runs in its own empty scenario directory, so relative
// `config/...` paths cannot touch the user's real pool configuration.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    const CONFIG_PATH: &str = "config/config.json";

    fn write_config(value: &serde_json::Value) {
        std::fs::write(CONFIG_PATH, serde_json::to_vec(value).unwrap()).unwrap();
    }

    #[test]
    fn defaults_and_roundtrip() {
        // With no file, loading writes the default config, but refuses to start
        // with its empty pool name.
        assert!(!Path::new(CONFIG_PATH).exists());
        assert!(load_config().is_err());

        // A pool name is all the written default lacks.
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(CONFIG_PATH).unwrap()).unwrap();
        assert_eq!(value["poolName"], "");
        value["poolName"] = serde_json::json!("fixture");
        write_config(&value);
        let config = load_config().unwrap();
        assert_eq!(config.max_shares_per_minute, 80);
        assert_eq!(config.update_miners_stats_every, 1);
        // Key generation is disabled; an empty key keeps the dashboard off.
        assert!(config.dashboard_key.is_empty());

        // Saving and loading must preserve the serialized schema exactly.
        assert_eq!(serde_json::to_value(&config).unwrap(), value);
        save_config(&config);
        assert_eq!(serde_json::to_value(load_config().unwrap()).unwrap(), value);

        // Optional fields regain defaults when omitted; required fields and
        // malformed JSON must fail instead of silently producing partial config.
        value.as_object_mut().unwrap().remove("dashboard_key");
        write_config(&value);
        let config = load_config().unwrap();
        assert_eq!(config.max_shares_per_minute, 80);
        assert!(config.dashboard_key.is_empty());

        value.as_object_mut().unwrap().remove("host");
        write_config(&value);
        assert!(load_config().is_err());
        std::fs::write(CONFIG_PATH, "{").unwrap();
        assert!(load_config().is_err());
    }
}
