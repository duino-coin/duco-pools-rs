# Runtime coverage inventory

asserted means an explicit oracle exists, not necessarily that it passes; see run reports for results.

Every scoped behavior below is classified. This is an assertion inventory, not a measured line/branch-coverage percentage. Each executable functional scenario is mapped by a unit-tested registry. Multiple values and operation sequences may be checked inside one scenario.

The separate opt-in `benchmark` suite measures accepted, rejected and mixed traffic using the pool's `bench` feature against a recorded local baseline. Its six workloads and cost-weighted metrics are documented in [README.md](README.md#benchmarks-and-measured-baseline); they do not expand the production correctness claims below.

## Connections, cleanup and allocation lifetime

**asserted** - Greeting, exact active counts/weights, empty maps after graceful close, RST, early disconnect, overlapping churn, and fresh IDs after reconnect.

Scenarios: `connections/greeting-motd-idle`, `connections/disconnect-before-job`, `connections/disconnect-before-share`, `connections/active-disconnect`, `connections/abrupt-reset`, `connections/reconnect-cycles`, `connections/concurrent-churn`, `kolka/overlapping-churn`.

## Real read deadlines

**asserted** - Idle connection and allocated unanswered job remain open at least 85 seconds and close within 100 seconds, releasing state.

Scenarios: `connections/idle-timeout`, `connections/active-job-timeout`.

## Framing and input rejection

**partial** - Independent SHA-1 solver; feedback/recovery; LF/CRLF/legacy and coalesced/fragmented frames; 1023/1024/1025 byte boundaries; empty/invalid UTF-8; initial field limits and nonce overflow. Arbitrarily delayed fragments are not specified by the legacy idle-framing protocol.

Scenarios: `protocol/good-bad-recovery`, `protocol/malformed-answer`, `protocol/missing-username`, `protocol/framing`, `protocol/coalesced-frames`, `protocol/oversized-frame`, `protocol/invalid-utf8`, `protocol/fragmented-frames`, `protocol/frame-boundaries`, `protocol/empty-frame`, `protocol/field-count-limits`, `protocol/nonce-overflow`, `protocol/motd-version`.

## Share and reward conservation

**asserted** - Intervals 1/2/5, rejected shares across publication boundaries, per-share reward sum, partial disconnect/shutdown and warmup. Reporting frequency must not discard earned work.

Scenarios: `counters/accepted-shares`, `counters/rejected-share-accounting`, `counters/reward-warmup`, `counters/interval-two-rewards`, `counters/interval-five-rewards`, `counters/partial-disconnect`, `counters/partial-shutdown`, `counters/publication-rejection-boundaries`.

## Reward formula and blocks

**asserted** - Independent formula vectors across all four threshold branches, distinct decay/divisor constants and fractional workers; independent reward-curve evaluation of actual mining inputs; forced blocks and hash chain.

Scenarios: `protocol/forced-block`, `internal/reward-vectors`, `counters/reward-formula-mining`, `protocol/hash-chain-block-details`.

## Difficulty mapping and adaptation

**asserted** - Curve difficulty rounding and minimum vectors (internal); initial difficulty 100, curve difficulty from the last hashrate and the exponential hashrate average.

Scenarios: `internal/difficulty-vectors`, `lifecycle/adaptive-mining-average`.

## Hashrate validation and estimation

**asserted** - Zero/negative/nonfinite rejection without a difficulty change, valid recovery and missing/malformed estimation.

Scenarios: `protocol/invalid-hashrates`, `protocol/estimated-hashrates`.

## Miner metadata and serialization

**asserted** - Name/IoT sanitization, defaults, 48-byte IoT and 96-byte key limits (explicit `BAD,Password too long` rejection for both the job password and the legacy `rig:key` key), legacy keys (blanked on `/data`, base64 in the sync upload), dashboard field types and numeric values.

Scenarios: `internal/sanitizers`, `protocol/metadata-sanitization`, `protocol/metadata-defaults`, `protocol/iot-boundaries`, `protocol/password-boundaries`, `protocol/legacy-key`, `lifecycle/dashboard-schema`.

## Worker limits and allocator model

**asserted** - PC/ESP weights, worker-limit rejection/recovery, ESP32 downgrade above mcu_pc_step_h, both cross-type recycling directions, user isolation, and 16000 seeded operations against an independent ordered-map model.

Scenarios: `counters/worker-limit`, `counters/weighted-worker-limit`, `kolka/pc-compaction`, `kolka/esp-compaction`, `kolka/mixed-reuse`, `kolka/user-isolation`, `internal/seeded-allocator`, `kolka/reverse-mixed-reuse`, `kolka/worker-limit-recovery`, `kolka/esp32-downgrade`.

## Chip ID reward eligibility

**asserted** - At or below mcu_pc_step_h, a share after the warmup earns a regular reward only with the chip ID of the first share; a changed, empty or initially empty one counts as rejected and earns only block rewards. Above the limit the chip ID is not checked.

Scenarios: `counters/chip-id-mismatch`.

## Sync snapshots, retry and staging

**asserted** - Multipart fields/metadata, failure retention (503, and 200 with `success: false` or a non-JSON body), success subtraction, concurrent new-user work, cleanup failures, transport disconnect/timeouts, staging writes, retry and total preservation.

Scenarios: `sync/payload-and-clearing`, `sync/failed-upload-retry`, `sync/disconnected-worker`, `sync/inflight-shares`, `sync/inflight-rewards`, `sync/inflight-failed-retry`, `sync/cleanup-missing-rewards`, `sync/cleanup-missing-workers`, `sync/transport-disconnect-retry`, `sync/transport-timeout-retry`, `sync/staging-workers-write-failure`, `sync/staging-rewards-write-failure`, `sync/rejected-sync-reply`.

## Pending rewards across restarts

**asserted** - Rewards a failed final sync left in the staging file are pending again after a restart, go out with the startup upload and are acknowledged exactly once; a corrupt file stops startup.

Scenarios: `sync/pending-rewards-reload`, `lifecycle/startup-corrupt-rewards`.

## IP discovery and fallback

**asserted** - Startup failure without IP, cached-IP sync fallback, failed initial sync with no cache followed by valid retry.

Scenarios: `lifecycle/startup-ip-disconnect`, `sync/ip-fallback`, `sync/ip-no-cache`.

## Configuration and dashboard authentication

**asserted** - Default creation, field defaults, generated key shape/uniqueness, roundtrip, malformed/missing fields, guessed port persisted and used for login, disabled dashboard and 20/21-byte authentication boundary.

Scenarios: `lifecycle/startup-and-auth`, `internal/config-defaults-roundtrip`, `lifecycle/dashboard-disabled`, `lifecycle/dashboard-key-boundary`, `lifecycle/guessed-port`, `lifecycle/startup-invalid-config`, `lifecycle/startup-missing-field`.

## Startup network failures

**asserted** - Master rejection/disconnect/invalid UTF-8 fail and bounded delayed greeting succeeds.

Scenarios: `lifecycle/startup-master-reject`, `lifecycle/startup-master-disconnect`, `lifecycle/startup-master-invalid-utf8`, `lifecycle/startup-master-delay`.

## Bans and refresh operation

**asserted** - Persist/load/corrupt-file fallback, pre-existing and newly saved IP bans, localhost exemption, direct production curve refresh updates ban enforcement.

Scenarios: `lifecycle/banned-user`, `internal/bans-persistence`, `lifecycle/preexisting-ip-ban`, `lifecycle/ban-reconnect`, `lifecycle/localhost-exemption`, `lifecycle/refreshed-bans`.

## Reward curve validation and fallback

**asserted** - Malformed, incomplete, out-of-range and error-page curves and every non-positive divisor, sharetime or block probability are rejected by name on refresh, keeping the curve in use and the saved config/curve.json; a valid curve is applied and saved. Without the master's curve at startup the saved one is used, without either startup fails; a bans outage at startup or on refresh neither stops startup nor discards the refreshed curve.

Scenarios: `curve/invalid-refresh-rejected`, `curve/saved-curve-fallback`, `curve/startup-without-curve`, `curve/bans-outage`.

## Shutdown with active work

**partial** - Final sync, logout and miner disconnect; a held upload finishes before the final snapshot, which conserves pending work. Remote exactly-once delivery after ambiguous acknowledgement is not guaranteed.

Scenarios: `lifecycle/shutdown-final-sync`, `sync/shutdown-held-upload`.

## Rate window

**asserted** - The fixed limit of 80 shares per minute rejects excess shares and first valid share after real 61-second expiry is accepted.

Scenarios: `lifecycle/share-rate-limit`, `lifecycle/rate-window-reset`.

## Hourly scheduler timing

**unverified** - Refresh operation is directly invoked in the disposable copy; the real one-hour scheduler wait is not exercised.

## OS resource and I/O exhaustion

**unverified** - Accept failures, write-side 90-second backpressure, file-descriptor exhaustion, clock rollback and kernel/process crashes are not deterministically injected.

## Malformed numerical configuration

**partial** - Malformed JSON/missing fields are covered; zero/overflow difficulty, reward divisor, sync/stats intervals and every nonsensical numeric combination are not fully classified.

## Dashboard routing variants

**partial** - Authentication and schema are asserted; every HTTP method, encoded query variant and route rejection combination is not enumerated.

## Deployment and inactive branches

**excluded** - Updater/restart helpers, real firewall effects, ngrok, unreachable AVR binary-nonce mode and bench/dummy_sync configurations are outside this runtime audit.

## Capacity and portability

**excluded** - Sustained load, memory/resource performance, and Linux/macOS execution are not established by Windows functional results.

## Interpretation

Known failing assertions remain classified as asserted: the test exists and identifies a defect. A PASS in one scenario is not evidence for behavior explicitly marked partial or unverified. Retained observations help diagnose results but do not by themselves establish coverage.

Read-only copy adapters and injected test templates are in the verifier; production APIs and sources stay unchanged. The hourly refresh operation is tested directly, not by altering the one-hour scheduler.
