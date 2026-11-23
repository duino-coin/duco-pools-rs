#!/usr/bin/env sh
# checks a published pool image without trusting CI:
#   1. the image was signed by docker.yml of REPO (cosign, keyless)
#   2. the release manifest attached to it is signed by the release key
#   3. the binary of every platform in the image is the one that manifest lists
#
#   ./docker-verify.sh <image>    e.g. ghcr.io/duino-coin/duco-pools-rs:v0.2.2
#
# everything is checked against the image digest resolved in step 1, so a tag moving in between
# doesn't matter; run the image by the printed digest afterwards
#
# needs: cosign (v3), docker or podman, jq, ssh-keygen, sha256sum
# environment:
#   REPO                     GitHub repository publishing the image (default: duino-coin/duco-pools-rs)
#   RELEASE_KEY              release public key to trust (default: src/updater/release_key.pub)
#   RELEASE_KEY_FINGERPRINT  if set, RELEASE_KEY must have this fingerprint
#   DOCKER                   container CLI (default: docker)
set -eu

REPO="${REPO:-duino-coin/duco-pools-rs}"
RELEASE_KEY="${RELEASE_KEY:-$(dirname "$0")/src/updater/release_key.pub}"
DOCKER="${DOCKER:-docker}"
# keep in sync with docker.yml
PREDICATE_TYPE="https://github.com/duino-coin/duco-pools-rs/release-manifest/v1"
# the updater has to check signatures against the same namespace
SIGNATURE_NAMESPACE="duco-pool-rs-release"
# platforms published by docker.yml and the release targets their binaries come from
PLATFORMS="amd64:x86_64-unknown-linux-musl arm64:aarch64-unknown-linux-musl"
ISSUER="https://token.actions.githubusercontent.com"
# a release publishes from its tag, a manual run from main
IDENTITY="^https://github.com/$(printf '%s' "$REPO" | sed 's/\./\\./g')/\.github/workflows/docker\.yml@refs/(tags/v[0-9]+\.[0-9]+\.[0-9]+|heads/main)$"

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <image>" >&2
    exit 1
fi
IMAGE="$1"

# repository without tag or digest, and the tag if one was given
IMAGE_TAG=""
case "$IMAGE" in
    *@*) NAME="${IMAGE%@*}" ;;
    *)
        NAME="$IMAGE"
        case "${IMAGE##*/}" in
            *:*)
                NAME="${IMAGE%:*}"
                IMAGE_TAG="${IMAGE##*:}"
                ;;
        esac
        ;;
esac

TMP=$(mktemp -d)
CONTAINER=""
cleanup() {
    [ -z "$CONTAINER" ] || "$DOCKER" rm "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$TMP"
}
trap cleanup EXIT

echo "== image signature"
DIGEST=$(cosign verify --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$IDENTITY" \
    --output json "$IMAGE" | jq -er '.[0].critical.image."docker-manifest-digest"')
REF="$NAME@$DIGEST"
echo "signed by docker.yml of $REPO: $REF"

echo "== release manifest"
cosign verify-attestation --certificate-oidc-issuer "$ISSUER" --certificate-identity-regexp "$IDENTITY" \
    --type "$PREDICATE_TYPE" "$REF" >"$TMP/attestations"
# one envelope per line; a republished release can carry several, all of them are signed by CI
# and checked the same way below, so the first one does
jq -r '.payload' "$TMP/attestations" | head -n 1 | base64 -d |
    jq -e --arg type "$PREDICATE_TYPE" 'select(.predicateType == $type) | .predicate' >"$TMP/predicate.json"
jq -er .manifest "$TMP/predicate.json" | base64 -d >"$TMP/manifest.json"
jq -er .signature "$TMP/predicate.json" | base64 -d >"$TMP/manifest.json.sig"

echo "== release signature"
# only type and key count, the comment is dropped
KEY=$(tr -d '\r' <"$RELEASE_KEY" | cut -d ' ' -f 1,2)
FINGERPRINT=$(printf '%s\n' "$KEY" | ssh-keygen -lf - | cut -d ' ' -f 2)
if [ -n "${RELEASE_KEY_FINGERPRINT:-}" ] && [ "$FINGERPRINT" != "$RELEASE_KEY_FINGERPRINT" ]; then
    echo "$RELEASE_KEY is $FINGERPRINT, expected $RELEASE_KEY_FINGERPRINT" >&2
    exit 1
fi
printf 'release namespaces="%s" %s\n' "$SIGNATURE_NAMESPACE" "$KEY" >"$TMP/allowed_signers"
ssh-keygen -Y verify -f "$TMP/allowed_signers" -I release -n "$SIGNATURE_NAMESPACE" \
    -s "$TMP/manifest.json.sig" <"$TMP/manifest.json"
TAG=$(jq -er .tag "$TMP/manifest.json")
# a validly signed manifest of another release must not pass for this one
if [ "$(jq -r .tag "$TMP/predicate.json")" != "$TAG" ]; then
    echo "attestation is for $(jq -r .tag "$TMP/predicate.json"), manifest for $TAG" >&2
    exit 1
fi
case "$IMAGE_TAG" in
    v*)
        if [ "$IMAGE_TAG" != "$TAG" ]; then
            echo "$IMAGE carries the manifest of $TAG" >&2
            exit 1
        fi
        ;;
esac

echo "== binaries"
# each platform goes by its own manifest digest out of the index; creating the index digest for a
# second platform makes the containerd image store fail with "cannot overwrite digest"
# the client checks the index against DIGEST since it's fetched by digest, so the children are covered
"$DOCKER" manifest inspect "$REF" >"$TMP/index.json"
for platform in $PLATFORMS; do
    arch="${platform%%:*}"
    expected=$(jq -er --arg target "${platform#*:}" \
        '.assets[] | select(.target == $target) | .binary_sha256' "$TMP/manifest.json")
    platform_digest=$(jq -er --arg arch "$arch" \
        '[.manifests[] | select(.platform.os == "linux" and .platform.architecture == $arch)] |
            if length == 1 then .[0].digest else error("expected one linux/\($arch) manifest") end' \
        "$TMP/index.json")
    CONTAINER=$("$DOCKER" create --platform "linux/$arch" "$NAME@$platform_digest")
    "$DOCKER" cp "$CONTAINER:/app/duco-pool-rs" "$TMP/duco-pool-rs-$arch" >/dev/null
    "$DOCKER" rm "$CONTAINER" >/dev/null
    CONTAINER=""
    printf '%s  %s\n' "$expected" "$TMP/duco-pool-rs-$arch" | sha256sum -c -
done

echo "== ok: $REF is release $TAG, signed with $FINGERPRINT"
