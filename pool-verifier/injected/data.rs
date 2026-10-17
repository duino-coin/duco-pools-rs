// Appended to the disposable copy of `data/data.rs`; see `prepare::append_injected_tests`.
// Its test process has an isolated current directory, so these relative writes are
// fixtures and never the production pool's ban file.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    #[tokio::test]
    async fn bans_persistence() {
        std::fs::create_dir_all("config").unwrap();

        // A missing file is a valid empty ban list.
        let mut bans = Bans::load_bans().unwrap();
        assert!(bans.banned_ips.is_empty());

        // Both independent sets must survive a save/load round trip.
        bans.banned_ips.insert(CompactString::from("127.0.0.2"));
        bans.banned_usernames.insert("alice".into());
        bans.save_bans().await.unwrap();
        let bans = Bans::load_bans().unwrap();
        assert!(bans.banned_ips.contains("127.0.0.2"));
        assert!(bans.banned_usernames.contains("alice"));

        // Production intentionally treats a corrupt ban file as empty instead of
        // preventing pool startup; pin that recovery behavior.
        std::fs::write("config/bans.json", "invalid").unwrap();
        assert!(Bans::load_bans().unwrap().banned_ips.is_empty());
    }
}
