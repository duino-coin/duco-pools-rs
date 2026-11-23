#!/bin/sh
# POSIX sh, so it also runs on busybox ash (e.g. alpine without bash) and dash
# no pipefail; dash (debian's sh) rejects it and nothing here is piped
set -eu
trap '' HUP

: "${UPDATE_PARENT_PID:?Missing parent PID}"
: "${UPDATE_SOURCE:?Missing update path}" # path to the updated executable
: "${UPDATE_EXECUTABLE:?Missing executable path}" # corresponds to the current executable path
: "${UPDATE_WORKING_DIR:?Missing working directory}"

case "$UPDATE_PARENT_PID" in
    '' | 0* | *[!0-9]*) exit 1 ;;
esac
# one test per line; set -e ignores failures on the left of && and ||
[ -s "$UPDATE_SOURCE" ]
[ -f "$UPDATE_EXECUTABLE" ]
[ "$UPDATE_SOURCE" != "$UPDATE_EXECUTABLE" ]
[ -w "$(dirname -- "$UPDATE_EXECUTABLE")" ]
kill -0 "$UPDATE_PARENT_PID"
chmod +x -- "$UPDATE_SOURCE"
cd -- "$UPDATE_WORKING_DIR"

# stdout is only the readiness channel; subsequent output goes to the log
printf 'UPDATE_READY\n'
exec 1>&2
while kill -0 "$UPDATE_PARENT_PID" 2>/dev/null; do
    sleep 0.2
done

mv -f -- "$UPDATE_SOURCE" "$UPDATE_EXECUTABLE" # the pool is shutdown, so we can override it's executable
#exec "$UPDATE_EXECUTABLE"
screen -dmS pool "$UPDATE_EXECUTABLE"