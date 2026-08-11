#!/usr/bin/env bash
#
# Compatibility shim. The real work lives in setup.sh, which is idempotent and
# role-based:
#
#   bash bench/scripts/setup.sh --role client
#
# This wrapper stays because older docs and muscle memory say `. setup_node.sh`.
# Sourcing is no longer required -- setup.sh writes ~/.ozonedb.env and wires it
# into ~/.profile and ~/.bashrc -- but sourcing still works and additionally
# loads the environment into the current shell.

_ozonedb_setup_node() {
  local script_dir
  if [ -n "${BASH_SOURCE[0]:-}" ]; then
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  else
    script_dir="$(cd "$(dirname "$0")" && pwd)"
  fi

  bash "$script_dir/setup.sh" --role client "$@" || return $?

  # Convenience when sourced: make the new environment live immediately.
  if [ -f "$HOME/.ozonedb.env" ]; then
    # shellcheck disable=SC1090
    . "$HOME/.ozonedb.env"
  fi
}

_ozonedb_setup_node "$@"
