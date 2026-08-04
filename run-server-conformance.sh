#!/usr/bin/env bash
# Runs the server conformance suite against a locally built testserver.
#
# Usage: ./run-server-conformance.sh <memory|badger> <port> [vitest args...]
#
# The server runs as a child of this script and is terminated by the trap on
# every exit path (success, test failure, or Ctrl-C), so no process is left
# holding the port. SIGTERM lets the server shut down gracefully and remove its
# temporary data directory.
#
# This lives in a script rather than inline in Taskfile.yml because Task's
# built-in shell does not support `trap` for INT/TERM.

set -euo pipefail

STORAGE="${1:?usage: run-server-conformance.sh <memory|badger> <port> [vitest args...]}"
PORT="${2:?usage: run-server-conformance.sh <memory|badger> <port> [vitest args...]}"
shift 2

if [[ "$STORAGE" != "memory" && "$STORAGE" != "badger" ]]; then
    printf '[ERROR] Unknown storage %s; want memory or badger\n' "$STORAGE" >&2
    exit 2
fi
if [[ ! "$PORT" =~ ^[0-9]+$ ]] || ((10#$PORT < 1 || 10#$PORT > 65535)); then
    printf '[ERROR] Invalid port %s; want an integer from 1 to 65535\n' "$PORT" >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(mktemp -d)"
SERVER_PID=""
SERVER_URL="http://127.0.0.1:${PORT}"

probe_server() {
    # Bound every probe: a process that accepts TCP but never responds must not
    # make startup or cleanup hang indefinitely.
    curl --silent --output /dev/null --connect-timeout 0.2 --max-time 0.5 \
        "${SERVER_URL}/v1/stream/_readycheck"
}

cleanup() {
    # Clear the global before signaling. INT/TERM are followed by EXIT, and an
    # idempotent trap must never signal the same numeric PID twice after it has
    # been reaped (and possibly reused by an unrelated process).
    local pid="$SERVER_PID"
    SERVER_PID=""
    if [[ -n "$pid" ]]; then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf "$BIN_DIR"
}

on_signal() {
    local status="$1"
    # Do cleanup exactly once and preserve the conventional signal exit code.
    trap - EXIT INT TERM
    cleanup
    exit "$status"
}

trap cleanup EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

echo "[INFO] Installing pinned conformance dependencies..." >&2
npm ci --prefix "$SCRIPT_DIR/conformance" --ignore-scripts --no-audit --no-fund

# Refuse to borrow a responder that was already on the requested port. Without
# this check, curl could declare readiness after our child lost the bind race,
# and the suite would silently run against an unrelated service.
if probe_server; then
    echo "[ERROR] Port ${PORT} is already serving HTTP; refusing to reuse it" >&2
    exit 1
fi

echo "[INFO] Building testserver..." >&2
go build -o "$BIN_DIR/ds-testserver" "$SCRIPT_DIR/cmd/testserver"

"$BIN_DIR/ds-testserver" -port "$PORT" -storage "$STORAGE" &
SERVER_PID=$!

# Wait for the port to accept requests. curl without -f exits 0 on any HTTP
# response, including the expected 404 for a missing stream.
for ((attempt = 0; attempt < 50; attempt++)); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "[ERROR] Server exited during startup" >&2
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
        exit 1
    fi
    if probe_server; then
        # Check ownership again after the successful probe. In particular, do
        # not treat an old service as ours if the child has just lost a bind race.
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "[ERROR] Server exited during startup" >&2
            wait "$SERVER_PID" 2>/dev/null || true
            SERVER_PID=""
            exit 1
        fi
        break
    fi
    sleep 0.2
done

if ! kill -0 "$SERVER_PID" 2>/dev/null || ! probe_server; then
    echo "[ERROR] Server did not become ready on port ${PORT}" >&2
    exit 1
fi

echo "[INFO] Server ready on port ${PORT} (${STORAGE} storage)" >&2

cd "$SCRIPT_DIR/conformance"
CONFORMANCE_TEST_URL="$SERVER_URL" npm run test:server -- "$@"
