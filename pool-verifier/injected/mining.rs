// Appended to the disposable copy of `mining/mining.rs`; see `prepare::append_injected_tests`.
// Injection keeps these white-box checks next to private helpers without exporting
// those helpers from the production crate.
#[cfg(test)]
mod verifier_tests {
    use super::*;

    #[test]
    fn sanitizers() {
        // Passwords up to MAX_PASSWORD_BYTES are stored base64 encoded. A longer
        // one is rejected and leaves the previous value untouched.
        let mut out = CompactString::from("stale");
        assert!(!encode_password(&"x".repeat(97), &mut out));
        assert_eq!(out, "stale");
        assert!(encode_password(&"x".repeat(96), &mut out));
        assert_eq!(out, "eHh4".repeat(32));

        // The name cleaners intentionally preserve only the punctuation allowed
        // by the miner protocol.
        clean_name_into("A_z!?é.(x)- 1", &mut out);
        assert_eq!(out, "Az.(x)- 1");
        clean_iot_into("T:21@a_!é.(x)-", &mut out);
        assert_eq!(out, "T:21@a.(x)-");
    }
}
