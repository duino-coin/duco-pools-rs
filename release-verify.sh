#!/usr/bin/env sh
# verifies a CI release by rebuilding it locally, then writes (and optionally signs) a manifest
#
#   ./release-verify.sh <tag>          wait for CI, download, rebuild, compare, write manifest
#   ./release-verify.sh <tag> --sign   also sign the manifest with the SSH release key, upload
#                                      it and publish the release if it's still a draft
#   ./release-verify.sh --tag [--sign] tag HEAD as v<version from Cargo.toml> and push it, so CI
#                                      builds the draft release, then continue as above
#
# the tag gets checked out into its own git worktree, so local changes never end up in the
# rebuild; hashes only match as long as both sides build the same way (committed Cargo.lock,
# toolchain from rust-toolchain.toml, cross version from .github/workflows/rust.yml, so keep
# the local cross at that version)
#
# needs: git, gh (logged in), cross + podman, sha256sum, tar; for --sign also ssh-keygen
# environment:
#   REPO          GitHub repository with the releases (default: duino-coin/duco-pools-rs); with
#                 --tag, the tag gets pushed to the git remote pointing at it
#   SIGNING_KEY   SSH key to sign with (default: ~/.ssh/release_sk)
#   SSH_KEYGEN    ssh-keygen to sign with (default: the Windows system one if present)
set -eu

#REPO="${REPO:-duino-coin/duco-pools-rs}"
REPO="${REPO:-profpandaa/duco-pools-rs-copy}"
SIGNING_KEY="${SIGNING_KEY:-$HOME/.ssh/release_sk}"
if [ -z "${SSH_KEYGEN:-}" ]; then
    SSH_KEYGEN="ssh-keygen"
    [ ! -x /c/Windows/System32/OpenSSH/ssh-keygen.exe ] ||
        SSH_KEYGEN="/c/Windows/System32/OpenSSH/ssh-keygen.exe"
fi
# the updater has to check signatures against the same namespace
SIGNATURE_NAMESPACE="duco-pool-rs-release"
WORKFLOW="rust.yml"
EXECUTABLE="duco-pool-rs"
# keep in sync with the matrix in .github/workflows/rust.yml
TARGETS="x86_64-unknown-linux-musl aarch64-unknown-linux-musl aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu"

cd "$(dirname "$0")"

usage() {
    echo "usage: $0 <tag> [--sign]"
    echo "       $0 --tag [--sign]"
    exit 1
}

# git remote that pushes to $REPO on GitHub
remote_for_repo() {
    for remote in $(git remote); do
        case "$(git remote get-url --push "$remote")" in
            *github.com[:/]"$REPO" | *github.com[:/]"$REPO".git)
                echo "$remote"
                return
                ;;
        esac
    done
}

# release asset name for a target, same naming as BUILD_TARGET in build.rs
asset_name() {
    case "$1" in
        x86_64-unknown-linux-gnu) echo "$EXECUTABLE-Linux-gnu-x86_64.tar.gz" ;;
        x86_64-unknown-linux-musl) echo "$EXECUTABLE-Linux-musl-x86_64.tar.gz" ;;
        aarch64-unknown-linux-gnu) echo "$EXECUTABLE-Linux-gnu-arm64.tar.gz" ;;
        aarch64-unknown-linux-musl) echo "$EXECUTABLE-Linux-musl-arm64.tar.gz" ;;
        *)
            echo "no asset name known for target $1" >&2
            exit 1
            ;;
    esac
}

sha256() {
    sha256sum "$1" | cut -d ' ' -f 1
}

TAG=""
SIGN=0
CREATE_TAG=0
for arg in "$@"; do
    case "$arg" in
        --sign) SIGN=1 ;;
        --tag) CREATE_TAG=1 ;;
        -*) usage ;;
        *)
            [ -z "$TAG" ] || usage
            TAG="$arg"
            ;;
    esac
done
if [ "$CREATE_TAG" -eq 1 ]; then
    [ -z "$TAG" ] || usage
else
    [ -n "$TAG" ] || usage
fi

if [ "$CREATE_TAG" -eq 1 ]; then
    # first version line is the one under [package]
    TAG="v$(sed -n 's/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -n 1)"
    REMOTE=$(remote_for_repo)
    if [ -z "$REMOTE" ]; then
        echo "no git remote pushes to $REPO" >&2
        exit 1
    fi
    echo "== tagging $TAG"
    # only tracked files end up in the build, so untracked ones don't matter
    if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
        echo "uncommitted changes, commit or stash them first" >&2
        exit 1
    fi
    if git rev-parse --quiet --verify "refs/tags/$TAG" >/dev/null; then
        echo "$TAG already exists locally, bump the version in Cargo.toml first" >&2
        exit 1
    fi
    if git ls-remote --exit-code --tags "$REMOTE" "refs/tags/$TAG" >/dev/null; then
        echo "$TAG already exists on $REMOTE, bump the version in Cargo.toml first" >&2
        exit 1
    fi
    # the tagged commit has to be on main already, otherwise the release isn't reproducible
    # from it
    git fetch --quiet "$REMOTE" main
    if [ "$(git rev-parse HEAD)" != "$(git rev-parse FETCH_HEAD)" ]; then
        echo "HEAD is not $REMOTE/main, push or pull first" >&2
        exit 1
    fi
    printf 'tag %s as %s and push it to %s? [y/N] ' "$(git rev-parse --short HEAD)" "$TAG" "$REMOTE"
    read -r answer
    [ "$answer" = "y" ] || exit 1
    # annotated, so tag.gpgsign gets honoured (a plain `git tag` fails with it set)
    git tag -m "$TAG" "$TAG"
    if ! git push --quiet "$REMOTE" "refs/tags/$TAG"; then
        # drop it again, so a rerun doesn't trip over the local tag
        git tag -d "$TAG" >/dev/null
        exit 1
    fi
fi

COMMIT=$(git rev-parse --verify "$TAG^{commit}")
VERSION="${TAG#v}"
OUT="target/release-verify/$TAG"
WORKTREE="$OUT/src"
rm -rf "$OUT"
mkdir -p "$OUT/ci" "$OUT/ci-bin"

cleanup() {
    git worktree remove --force "$WORKTREE" 2>/dev/null || true
}
trap cleanup EXIT

echo "== $TAG ($COMMIT)"

echo "== waiting for CI"
# a freshly pushed tag takes a moment until its run shows up
ATTEMPTS=1
[ "$CREATE_TAG" -eq 0 ] || ATTEMPTS=24
while :; do
    RUN_ID=$(gh run list --repo "$REPO" --workflow "$WORKFLOW" --commit "$COMMIT" --event push \
        --limit 1 --json databaseId --jq '.[0].databaseId // empty')
    [ -z "$RUN_ID" ] || break
    ATTEMPTS=$((ATTEMPTS - 1))
    if [ "$ATTEMPTS" -le 0 ]; then
        echo "no $WORKFLOW run found for $COMMIT" >&2
        exit 1
    fi
    sleep 5
done
gh run watch "$RUN_ID" --repo "$REPO" --exit-status >/dev/null

echo "== downloading release assets"
for target in $TARGETS; do
    gh release download "$TAG" --repo "$REPO" --dir "$OUT/ci" --pattern "$(asset_name "$target")"
done

echo "== rebuilding locally"
git worktree add --detach "$WORKTREE" "$COMMIT" >/dev/null
if [ "$SIGN" -eq 1 ]; then
    # never sign a build that trusts a different key than the one signing it (e.g. a swapped
    # release_key.pub that got merged); only type and key are compared, not the comment
    key_of() {
        tr -d '\r' <"$1" | cut -d ' ' -f 1,2
    }
    if [ ! -f "$WORKTREE/src/updater/release_key.pub" ]; then
        echo "$TAG has no src/updater/release_key.pub, not signing" >&2
        exit 1
    fi
    if [ "$(key_of "$WORKTREE/src/updater/release_key.pub")" != "$(key_of "$SIGNING_KEY.pub")" ]; then
        echo "release key embedded in $TAG differs from $SIGNING_KEY.pub, not signing" >&2
        exit 1
    fi
fi
podman machine start 2>/dev/null || true
for target in $TARGETS; do
    (cd "$WORKTREE" && cross build --locked --release --target "$target")
done

echo "== comparing"
MISMATCH=0
ENTRIES=""
for target in $TARGETS; do
    asset=$(asset_name "$target")
    mkdir -p "$OUT/ci-bin/$target"
    tar -xzf "$OUT/ci/$asset" -C "$OUT/ci-bin/$target" "$EXECUTABLE"
    ci_hash=$(sha256 "$OUT/ci-bin/$target/$EXECUTABLE")
    local_hash=$(sha256 "$WORKTREE/target/$target/release/$EXECUTABLE")
    if [ "$ci_hash" = "$local_hash" ]; then
        echo "ok        $target $ci_hash"
    else
        echo "MISMATCH  $target ci=$ci_hash local=$local_hash"
        MISMATCH=1
    fi
    [ -z "$ENTRIES" ] || ENTRIES="$ENTRIES,"
    ENTRIES="$ENTRIES
    {\"target\": \"$target\", \"asset\": \"$asset\", \"archive_sha256\": \"$(sha256 "$OUT/ci/$asset")\", \"binary_sha256\": \"$ci_hash\"}"
done
if [ "$MISMATCH" -ne 0 ]; then
    # keep the local build for inspection (e.g. diffoscope against $OUT/ci-bin)
    trap - EXIT
    echo "builds differ, nothing written or signed. local build kept in $WORKTREE" >&2
    echo "remove it afterwards with: git worktree remove --force $WORKTREE" >&2
    exit 1
fi

MANIFEST="$OUT/manifest.json"
printf '{\n  "version": "%s",\n  "tag": "%s",\n  "commit": "%s",\n  "assets": [%s\n  ]\n}\n' \
    "$VERSION" "$TAG" "$COMMIT" "$ENTRIES" >"$MANIFEST"
echo "== wrote $MANIFEST"

if [ "$SIGN" -eq 0 ]; then
    echo "dry run, not signing. rerun with --sign to sign and upload"
    exit 0
fi

echo "== signing"
rm -f "$MANIFEST.sig"
"$SSH_KEYGEN" -Y sign -f "$SIGNING_KEY" -n "$SIGNATURE_NAMESPACE" "$MANIFEST"
"$SSH_KEYGEN" -Y check-novalidate -n "$SIGNATURE_NAMESPACE" -s "$MANIFEST.sig" <"$MANIFEST"
gh release upload "$TAG" --repo "$REPO" --clobber "$MANIFEST" "$MANIFEST.sig"
echo "== uploaded manifest.json and manifest.json.sig"

if [ "$(gh release view "$TAG" --repo "$REPO" --json isDraft --jq .isDraft)" = "true" ]; then
    printf 'publish draft release %s? [y/N] ' "$TAG"
    read -r answer
    if [ "$answer" = "y" ]; then
        gh release edit "$TAG" --repo "$REPO" --draft=false >/dev/null
        echo "== published $TAG"
    fi
else
    echo "note: $TAG was already public before the signature was attached"
fi
