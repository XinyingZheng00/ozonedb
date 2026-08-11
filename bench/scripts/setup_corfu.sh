#!/usr/bin/env bash
#
# Compatibility shim for the CorfuDB node. The real work lives in setup.sh:
#
#   bash bench/scripts/setup.sh --role corfu-server
#
# Beyond what this script used to do (apt deps + clone + mvn install), the role
# also creates $CORFU_DATA_DIR/{load,run_batch}, which run_ycsb_with_corfu.sh
# has always assumed exists and which nothing previously created.
#
# CLONE_DIR is still honoured.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

args=(--role corfu-server)
[[ -n "${CLONE_DIR:-}" ]] && args+=(--corfu-dir "$CLONE_DIR")

exec bash "$SCRIPT_DIR/setup.sh" "${args[@]}" "$@"
