# duco-pools-rs

duino-coin pools, but in rust

# Usage

There's three ways to build and run a pool:

- Deploying a production pool: <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Build the pool via `cargo build --release` and grab the executable from
  `target/release`
- Running a local (testing) pool: <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Run `./build-local.sh` or use `cargo build --profile local` and grab the
  executable from `target/local`
- Running a benchmark pool (sets the nonce to stay at one; used to make it easier for test-miners to submit as many
  shares as possible) <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Run `./build-bench.sh` or use `cargo build --profile local --features bench` and
  grab the executable from `target/local`

Keep in mind, that only the production pool connects to and syncs with the master-server. The other two options are
entirely local and therefore isolated.

## Docker
(currently untested)
Config and sync data live in the `pool-config` and `pool-data` volumes. Inside a container the image is the update
mechanism, so keep `auto_update` off. The image has no shell or firewall tools either, so leave `firewall_cmd`,
`restart_script` and `update_restart_script` empty and set `base_sync_folder` to `./dashboard/`.

The first start writes a default `config.json` into `pool-config` and exits until `poolName` is set. Copy your own
config in, the pool picks it up on its next restart:

```bash
docker compose cp config.json pool:/app/config/config.json
```

- Building it yourself: <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `docker compose up -d --build` builds the checked out tree.
  `POOL_BUILD=release POOL_TAG=v0.2.1 docker compose up -d --build` uses that signed release instead and refuses to
  build if its signature or hashes don't check out.
- Pulling the published image: <br/>
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Every signed release is published as `ghcr.io/duino-coin/duco-pools-rs:<tag>`
  (and `:latest`) for amd64 and arm64. It's built from the same release assets after their signature has been checked,
  and the signed release manifest is attached to the image. Verify an image before running it:

  ```bash
  RELEASE_KEY_FINGERPRINT=SHA256:JIF3znWiNSXyMv9blziUtAdRb+on4a/4hN9q6NGEPdg ./docker-verify.sh ghcr.io/duino-coin/duco-pools-rs:v0.2.2
  ```

  This checks that the image comes from this repository's `docker.yml`, that the attached manifest is signed by the
  release key and that the binary of every platform in the image is the one that manifest lists. It needs `cosign`
  (v3), `docker` (or `DOCKER=podman`), `jq`, `ssh-keygen` and `sha256sum`. Compare the fingerprint against one you
  got from somewhere other than this repository. Then run the image by the digest the script prints, with the same
  settings as `compose.yaml` and `image: ghcr.io/duino-coin/duco-pools-rs@sha256:...` in place of the `build` section.

Use `network_mode: host` on Linux, otherwise miners may all show up with the same IP and a single ban hits everyone.

# Important
`server_endpoint` must be the master server's IP, otherwise syncing won't work.

# TODO

- implement ngrok (tbd)

# License
Copyright © 2026 [profpandaa](https://github.com/profpandaa) from the Duino-Coin team.

This project is [AGPL-3.0](https://github.com/duino-coin/duco-pools-rs/blob/main/LICENSE) licensed.
