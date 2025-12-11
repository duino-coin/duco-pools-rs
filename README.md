# duco-pools-rs
duino-coin pools, but in rust

# Usage
There's three ways to build and run a pool:
- Deploying a production pool: <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Build the pool via `cargo build --release` and grab the executable from `target/release`
- Running a local (testing) pool: <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Run `./build-local.sh` or use `cargo build --profile local` and grab the executable from `target/local`
- Running a benchmark pool (sets the nonce to stay at one; used to make it easier for test-miners to submit as many shares as possible) <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Run `./build-bench.sh` or use `cargo build --profile local --features bench` and grab the executable from `target/local`

Keep in mind, that only the production pool connects to and syncs with the master-server. The other two options are entirely local and therefore isolated.

# TODO
- implement ngrok (tbd)
