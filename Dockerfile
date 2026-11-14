# syntax=docker/dockerfile:1
# two ways to get the pool binary, picked with --target:
#   source   builds the checked out tree (default)
#   release  downloads the signed release POOL_TAG and checks it against src/updater/release_key.pub,
#            same as the updater does
ARG RELEASE_REPO=duino-coin/duco-pools-rs

FROM rust:1.99.1-alpine AS build-source
# static musl binary, same target as the musl release assets
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY rust-toolchain.toml Cargo.toml Cargo.lock build.rs ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release && cp target/release/duco-pool-rs /duco-pool-rs

# only downloads and checks, so it runs natively; TARGETARCH still picks the asset
FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS build-release
ARG RELEASE_REPO
ARG POOL_TAG
# optional; pins the trust root outside of the checked out tree, docker.yml always sets it
ARG RELEASE_KEY_FINGERPRINT
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl jq openssh-client \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /release
COPY src/updater/release_key.pub ./
# github_token is optional and only needed while the release repo is private
RUN --mount=type=secret,id=github_token <<EOF
set -eu
: "${POOL_TAG:?set POOL_TAG to a release tag, e.g. v0.2.1}"
case "$TARGETARCH" in
    amd64) target=x86_64-unknown-linux-musl ;;
    arm64) target=aarch64-unknown-linux-musl ;;
    *) echo "no release asset for $TARGETARCH" >&2; exit 1 ;;
esac
# removed in the same step, so the token never ends up in a layer
trap 'rm -f headers' EXIT
: >headers
[ ! -s /run/secrets/github_token ] ||
    printf 'Authorization: Bearer %s\n' "$(cat /run/secrets/github_token)" >headers
api="https://api.github.com/repos/$RELEASE_REPO"
curl -fsSL -H @headers -o release.json "$api/releases/tags/$POOL_TAG"
# via the API, plain download links don't work for private repos; curl drops the token on the
# redirect to the storage host
fetch() {
    id=$(jq -er --arg name "$1" '.assets[] | select(.name == $name) | .id' release.json)
    curl -fsSL -H @headers -H "Accept: application/octet-stream" -o "$1" "$api/releases/assets/$id"
}
fetch manifest.json
fetch manifest.json.sig

# only type and key of release_key.pub count, the comment is dropped
key=$(tr -d '\r' <release_key.pub | cut -d ' ' -f 1,2)
if [ -n "${RELEASE_KEY_FINGERPRINT:-}" ]; then
    fingerprint=$(printf '%s\n' "$key" | ssh-keygen -lf - | cut -d ' ' -f 2)
    if [ "$fingerprint" != "$RELEASE_KEY_FINGERPRINT" ]; then
        echo "release_key.pub is $fingerprint, expected $RELEASE_KEY_FINGERPRINT" >&2
        exit 1
    fi
fi
namespace="duco-pool-rs-release"
printf 'release namespaces="%s" %s\n' "$namespace" "$key" >allowed_signers
ssh-keygen -Y verify -f allowed_signers -I release -n "$namespace" -s manifest.json.sig <manifest.json
# a validly signed manifest of another release must not pass for this one
[ "$(jq -r .tag manifest.json)" = "$POOL_TAG" ]

entry=$(jq -ec --arg t "$target" '.assets[] | select(.target == $t)' manifest.json)
asset=$(printf '%s' "$entry" | jq -r .asset)
fetch "$asset"
printf '%s  %s\n' "$(printf '%s' "$entry" | jq -r .archive_sha256)" "$asset" | sha256sum -c -
tar -xzf "$asset" duco-pool-rs
printf '%s  duco-pool-rs\n' "$(printf '%s' "$entry" | jq -r .binary_sha256)" | sha256sum -c -
mv duco-pool-rs /duco-pool-rs
EOF

# the manifest and signature checked above; docker.yml attaches them to the published image, so
# they can be checked again without trusting CI (docker-verify.sh)
FROM scratch AS release-manifest
COPY --from=build-release /release/manifest.json /release/manifest.json.sig /

# distroless has no shell, so the writable dirs are prepared here
FROM --platform=$BUILDPLATFORM busybox:1.37 AS layout
# podman leaves the volume of an empty dir owned by root, the placeholder gets it handed to nonroot
RUN mkdir -p /out/config /out/dashboard && touch /out/dashboard/.keep
COPY config/curve.json /out/config/

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=layout --chown=nonroot:nonroot /out/ /app/
EXPOSE 6000 6030
VOLUME ["/app/config", "/app/dashboard"]
ENTRYPOINT ["/app/duco-pool-rs"]

FROM runtime AS release
COPY --from=build-release /duco-pool-rs /app/duco-pool-rs

FROM runtime AS source
COPY --from=build-source /duco-pool-rs /app/duco-pool-rs
