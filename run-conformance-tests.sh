#!/bin/bash
# Wrapper script to run client conformance tests for the local Go client
# Builds and runs the local conformance adapter from this repository

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="${SCRIPT_DIR}/cmd/conformance-adapter"
ADAPTER_BIN="/tmp/durable-streams-go-conformance-adapter"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() { echo -e "${GREEN}[INFO]${NC} $1" >&2; }
echo_warn() { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
echo_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# Build the local conformance adapter
build_adapter() {
    echo_info "Building local conformance adapter..."
    go build -o "$ADAPTER_BIN" "$ADAPTER_DIR"
    echo_info "Built adapter at: $ADAPTER_BIN"
}

# Run the conformance tests
run_tests() {
    build_adapter

    echo_info "Running client conformance tests..."
    echo_info "Adapter: $ADAPTER_DIR (local)"

    npx @durable-streams/client-conformance-tests --run "$ADAPTER_BIN" "$@"
}

# Main
run_tests "$@"
