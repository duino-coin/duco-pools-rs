# Pool verifier
(vibe-coded (read: I didn't read the code, but verified it against a known-working build of the pools) with GPT-6 Astra) <br/>
A standalone Rust CLI that checks the current Rust pool working tree against isolated loopback services. It contains **95 scenarios**: 88 process scenarios and 7 injected internal-test scenarios. The default runs 92; `--slow` includes three real-time checks. These are functional checks, not exhaustive proof. An additional opt-in benchmark suite measures throughput against a saved, measured baseline using the pool's `bench` feature.

## Run

From the repository with Rust/Cargo and native build tools installed:

```text
cargo run --manifest-path pool-verifier/Cargo.toml
cargo run --manifest-path pool-verifier/Cargo.toml -- --list
cargo run --manifest-path pool-verifier/Cargo.toml -- --suite sync
cargo run --manifest-path pool-verifier/Cargo.toml -- --suite internal
cargo run --manifest-path pool-verifier/Cargo.toml -- --case protocol/invalid-hashrates
cargo run --manifest-path pool-verifier/Cargo.toml -- --slow
```

`--suite` may be repeated for functional suites. The `benchmark` suite runs separately and is never included by default. `--case` accepts a unique name or `suite/name` and intersects suite selection. Unknown/empty selections fail rather than silently running zero checks. `--list` and `--help` do not build or start a pool.

`--slow` adds the real 90-second idle and unanswered-job deadlines and the real 61-second rate-window expiry. Production timeouts are never shortened. The adaptive mining scenario independently solves 55 jobs and can take tens of seconds.

Exit codes remain **0** all selected checks pass, **1** assertions/cleanup fail, **2** setup/build error, **130** interrupted. New pool defects stay ordinary failures; the verifier does not hide expected failures or fix production code.

It is not part of the root Cargo workspace or release CI.

## Benchmarks and measured baseline

The additional `benchmark` suite uses **only `cargo build --release --locked --features bench`** for the pool. The feature fixes the nonce at 1 and removes share-rate throttling. The verifier itself must also run in release mode. There is no production-mode benchmark or baseline.

Record the current implementation once, then compare subsequent runs with those saved measurements:

```text
cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite benchmark --record-baseline
cargo run --release --manifest-path pool-verifier/Cargo.toml -- --suite benchmark
cargo run --manifest-path pool-verifier/Cargo.toml -- --suite benchmark --list
```

`--record-baseline` explicitly creates or replaces `benchmarks/baseline.json`. Baseline artifact paths are relative to that file (for example, `../runs/...`), and machine hostnames are omitted. A normal comparison never overwrites it. A missing, incomplete, or incompatible baseline is an error; the suite never fabricates a reference value. Failed or interrupted runs retain the previous baseline. The recorded source fingerprint includes the actual working tree, including uncommitted pool changes, rather than relying only on the Git revision.

Six workloads combine single-client (`serial-*`) and 16-client (`concurrent-*`) runs with `accepted`, `rejected`, and `mixed` traffic. All clients share one username. Each sample launches a fresh isolated pool. Miners complete 100 accepted warmup shares each before a synchronized start, then submit 10,000 measured shares each without artificial sleeps. Accepted traffic submits nonce 1; rejected traffic submits nonce 0 and must receive `BAD,Incorrect result`; mixed traffic alternates accepted/rejected submissions (an odd final share is accepted). Every advertised job must match nonce 1. Unexpected feedback, timeouts and disconnects fail the sample rather than inflating throughput. Five samples per workload are recorded, alternating workload order. Startup, connection setup, warmup, diagnostics and shutdown are outside the timed interval.

Each sample also measures the pool process's resident memory (RSS). `baseline_memory_bytes` is the median of five RSS reads after startup and before miner connections. `connected_memory_bytes` is measured the same way after every miner has connected and completed warmup, while the sockets remain open. `memory_per_connection_bytes` is `(connected - baseline) / clients`, with negative sampling noise clamped to zero. The headline per-connection result uses concurrent samples, where the delta is large enough to avoid single-page rounding noise. Memory sampling finishes before the synchronized timed workload begins, so it does not reduce measured throughput.

**Both accepted and rejected shares count.** Raw throughput is `(accepted + rejected) / seconds`. A completed baseline calibrates a rejected-share cost weight from the two serial workloads: `weight = median accepted-only throughput / median rejected-only throughput`, equivalent to rejected cost/share divided by accepted cost/share. The accepted-share weight is 1. Weighted throughput is `(accepted + weight * rejected) / seconds`. Future runs reuse the baseline weight, so a change in the current implementation cannot move its own reference. The weight estimates measured end-to-end cost, including protocol and scheduling, not isolated CPU cost or a universal cost for every kind of rejection. Warmup puts even rejected-only miners into an established session with stats publication active. Raw counts and throughput remain available alongside weighted results.

Optional workload controls (use the same values when recording and comparing):

| Option |               Default |        Allowed |
| --- |----------------------:|---------------:|
| `--benchmark-samples` |                     5 |          3-100 |
| `--benchmark-shares` |     10 000 per client | 10 000-500 000 |
| `--benchmark-warmup` |        100 per client |         1-1000 |
| `--benchmark-clients` | 16 concurrent clients |          2-256 |

Benchmark builds use a separate `work/benchmark` source/build cache. They redirect external services to loopback fixtures and add the shutdown control channel, but omit the functional verifier's per-share observation instrumentation and injected tests. The benchmark miner does not use functional-test delays or transcript logging. Pool logic and configuration still control accounting and responses; `dummy_sync` is not enabled. All workloads use stats cadence 1 and disable periodic sync during the short measurement window.

Artifacts are stored in `runs/benchmark-<timestamp>-<pid>/`. `benchmark.json` contains every sample's elapsed time, accepted/rejected counts, raw throughput, p50/p95 round-trip latency, baseline/connected RSS, memory per connection and diagnostic directory, plus the source fingerprint, toolchain, platform, build mode and workload settings. `summary.json` includes raw and weighted medians, outcome counts, calibrated weights, median baseline RSS and median bytes per connection. `comparison.json` compares throughput, latency, baseline RSS and bytes per connection with the saved baseline; the same current values, baseline values and percentage changes are printed to stdout. A throughput ratio above 1 means faster; memory and latency ratios below 1 mean lower usage. `metadata.json` is saved before building, and `failure.json` records sample failures. `work/latest-run.txt` locates the latest run. Earlier complete runs retain a copy of their measurements even after re-recording the baseline.

These are end-to-end loopback measurements, including client validation and OS scheduling; they are not a guarantee of maximum deployed capacity. Keep other machine load and power settings consistent. Settings, platform, logical CPU count, Rust/Cargo versions, local Cargo configuration and measurement-harness changes must match the baseline. Compare only on the same machine. Throughput changes are reported without an arbitrary performance pass/fail threshold; exit 0 means measurement/comparison succeeded, not that performance improved. The suite cannot be mixed with functional suites, `--case`, `--slow`, or `--mutation`.

## Coverage

[COVERAGE.md](COVERAGE.md) describes each behavior, its oracle, associated scenarios, and remaining limits. [coverage.json](coverage.json) is the machine-readable inventory; a verifier unit test ensures every functional scenario appears and all inventory IDs exist. **Asserted means a correctness check exists, not that the pool passes it.**

Coverage includes:

- Connections, early/active disconnects, resets, overlapping churn, worker counts and real read deadlines.
- SHA-1 solving, framing and size limits, malformed input, hashrate rejection/estimation, metadata sanitization, mining keys, blocks and hash chaining.
- Full accounting with stats intervals 1, 2 and 5, reward warmup, chip eligibility, partial disconnect/shutdown and rejection boundaries.
- Independent reward formula vectors, an independent reward-curve evaluation of real mining, tier mappings, adaptive thresholds, curve difficulty and the hashrate average.
- Worker limits/recovery, both mixed-slot reuse directions and 16,000 seeded allocator operations against a separate ordered-map model.
- Multipart sync, immutable snapshots, in-flight work, transport failure/retry, staging-file failures, cached IPs and shutdown during an upload.
- Configuration creation/defaults/persistence, dashboard authentication/schema, startup failures, ban persistence/enforcement and directly triggered reward/ban refresh.

Deployment/updater/restart helper execution, real firewall effects, sustained production load, build features other than the opt-in benchmark suite's `bench` feature, and inactive branches are excluded. The actual hourly refresh scheduler is unverified: refresh tests call its production operation directly. Windows runs do not establish Linux/macOS coverage. See the inventory for partial coverage and OS failure paths that are not injected.

## Isolation and test-copy access

Every run refreshes `work/source-next` from current `src`, `Cargo.toml`, `Cargo.lock` and `build.rs`, then mirrors it to `work/source`. Removed files disappear; unchanged timestamps and `work/target` build artifacts are reused. Selected functional runtime scenarios build a production **release** binary without `bench` or `dummy_sync`. Selected internal scenarios also build a release test executable.

Adapters operate only in the disposable copy:

1. Redirect public-IP and sync URLs to loopback mocks. Existing configuration redirects reward-curve/ban downloads and master TCP requests.
2. Add authenticated read-only observations: pending/total counters, balances, hash state, difficulty caches, and mining calculation inputs/results captured at iteration boundaries. The observations do not recalculate or replace mining outcomes. Observation overhead can affect timing; tests do not claim performance equivalence.
3. Accept `shutdown` on stdin through the existing shutdown channel. A `refresh` test command invokes the actual `resync_curve` operation; it does not change scheduler timing or refresh behavior.
4. Append verifier-owned `injected/*.rs` tests to corresponding modules. Each internal scenario runs exactly one named test in its own process and directory, avoiding shared-global and current-directory contamination.

Replacement anchors must match once; drift is a setup error. Update the adapter instead of changing pool behavior. The old hard-coded and current configurable sync URL forms remain supported.

Each process has generated fake credentials/configuration and private data directories. Updates/restarts/firewall commands are disabled and HTTP proxies bypassed. No production configuration or credentials are copied. Banned-client tests bind to `127.0.0.2` because the pool exempts `127.0.0.1`.

Fixtures support configuration overrides (null removes a field), initial files, mutable rewards/bans, queued HTTP status/body responses, TCP disconnect/delay faults and master greeting/reply variations. A front proxy supplies real transport faults; responses close their connections so a later scripted fault cannot be bypassed by connection reuse. Held uploads use explicit gates. The default HTTP 503 retains accounting state until a case requests success.

Assertions use observable conditions and deadlines. Internal formula tests use independent vectors and distinct branch constants. Mining reward expectations use observed calculation inputs; they do not call the production formula as their oracle. Full-accounting checks sum every observed earned reward and require retention independently of stats cadence.

Each process is shut down and reaped, with forced termination on cleanup timeout. Only one run may own the copy/cache: `work/run.lock` enforces this. After a forced verifier termination, check that its processes stopped before removing a stale lock.

## Reports and sensitivity checks

`work/latest-run.txt` points to `runs/<timestamp>-<pid>`. Reports include per-case status, diagnostic, duration and artifact paths. Runtime cases retain `pool.log`, `miners.log`, `requests.json`, the last available `state.json`, generated configuration and staging files. Internal cases retain `internal.log`. `metadata.json` records selection, platform and any deliberate mutation. Setup failure artifacts may be limited to config and pool logs.

Compilation logs are `work/build.log` and `work/internal-build.jsonl`. Historical reports remain immutable. A source refresh on the next run removes any deliberate mutation.

To check the verifier itself:

```text
cargo test --manifest-path pool-verifier/Cargo.toml
cargo fmt --manifest-path pool-verifier/Cargo.toml -- --check
cargo clippy --manifest-path pool-verifier/Cargo.toml --all-targets -- -D warnings
cargo build --manifest-path pool-verifier/Cargo.toml
python pool-verifier/check_sensitivity.py
```

The Python script runs four explicit disposable-copy mutations: double regular rewards, discard in-flight pending shares, bypass authentication, and suppress worker release. Each must produce an assertion failure in its designated scenario, not a setup error. It saves `work/sensitivity.json`, restores the normal copy, checks internal and connection scenarios, and verifies production source hashes remain unchanged. It requires Python 3.9+ and can take several minutes because mutated release binaries are rebuilt.

For an individual sensitivity check:

```text
cargo run --manifest-path pool-verifier/Cargo.toml -- --mutation reward --case internal/reward-vectors
```

A detected deliberate mutation returns exit code 1. These runs are not evidence of an unmodified production bug. `python pool-verifier/check_sensitivity.py --only release` reruns a selected mutation while preserving earlier results and still restores/checks the normal copy.
