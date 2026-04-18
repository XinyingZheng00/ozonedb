#!/usr/bin/env bash
set -euo pipefail

# Orchestrate a Corfu server on a remote node while sweeping YCSB workloads
# and writer-process counts on this machine.
#
# Per iteration:
#   1. start corfu on $REMOTE_HOST:
#        cd $CORFU_DIR && rm -rf /data/run_batch/corfu \
#          && cp -r /data/load_batch/corfu /data/run_batch \
#          && JAVA_ARGS="-Xmx120g" ./bin/corfu_server \
#                 -l /data/run_batch -s -a $REMOTE_HOST $CORFU_PORT
#   2. run run_local_ycsb_multiproc.py with --workloads=<wl> --num_writers=<n>
#   3. stop corfu
# After the full sweep, corfu is started one more time (so the node is left
# ready for a subsequent manual run).
#
# On error before the final start, corfu is stopped so nothing leaks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"
REMOTE_HOST="10.10.1.2"
REMOTE_USER="${SSH_USER:-$USER}"
SSH_KEY=""
CORFU_DIR="~/CorfuDB"
CORFU_PORT=9000
CORFU_LOG="/tmp/corfu_server.log"
WORKLOADS="a b c d f"
WRITERS_LIST="1 2 4 8"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH         ycsb.yaml (default: $CONFIG)
  --remote HOST         corfu host (default: $REMOTE_HOST)
  --user USER           ssh user on remote (default: $REMOTE_USER)
  --ssh-key PATH        ssh private key (optional)
  --corfu-dir PATH      corfu repo on remote (default: $CORFU_DIR)
  --corfu-port PORT     corfu port (default: $CORFU_PORT)
  --workloads "a b c"   space-separated workload letters to sweep
                        (default: "$WORKLOADS")
  --writers-list "1 2"  space-separated num_writers values to sweep
                        (default: "$WRITERS_LIST")
  -h | --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --config)
    CONFIG="$2"
    shift 2
    ;;
  --remote)
    REMOTE_HOST="$2"
    shift 2
    ;;
  --user)
    REMOTE_USER="$2"
    shift 2
    ;;
  --ssh-key)
    SSH_KEY="$2"
    shift 2
    ;;
  --corfu-dir)
    CORFU_DIR="$2"
    shift 2
    ;;
  --corfu-port)
    CORFU_PORT="$2"
    shift 2
    ;;
  --workloads)
    WORKLOADS="$2"
    shift 2
    ;;
  --writers-list)
    WRITERS_LIST="$2"
    shift 2
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown option: $1" >&2
    usage
    exit 1
    ;;
  esac
done

read -r -a WORKLOADS_ARR <<<"$WORKLOADS"
read -r -a WRITERS_ARR <<<"$WRITERS_LIST"
if [[ ${#WORKLOADS_ARR[@]} -eq 0 || ${#WRITERS_ARR[@]} -eq 0 ]]; then
  echo "workloads and writers-list must each contain at least one value" >&2
  exit 1
fi

SSH_OPTS=(-o BatchMode=yes -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new)
[[ -n "$SSH_KEY" ]] && SSH_OPTS+=(-i "$SSH_KEY")
SSH_TARGET="${REMOTE_USER}@${REMOTE_HOST}"

remote_sh() {
  ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "$1"
}

port_open() {
  (exec 3<>/dev/tcp/"$1"/"$2") >/dev/null 2>&1
}

start_corfu() {
  echo "[corfu] starting on $SSH_TARGET ($REMOTE_HOST:$CORFU_PORT)"
  # The trailing `&` must apply ONLY to nohup, not to the full `cd && rm &&
  # cp && nohup ...` chain. Without the parentheses bash backgrounds the
  # whole chain in one subshell that then runs the chain sequentially --
  # the subshell would wait synchronously on corfu_server, holding ssh's
  # stdout/stderr pipes open, and ssh would hang forever.
  remote_sh "cd $CORFU_DIR && rm -rf /data/run_batch/corfu && cp -r /data/load_batch/corfu /data/run_batch && ( setsid nohup env CORFUDB_HEAP=122880 ./bin/corfu_server -l /data/run_batch -s -a $REMOTE_HOST $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
  sleep 2
  remote_sh "pgrep -af 'org.corfudb.infrastructure.CorfuServer' | head -n1 || echo '[corfu] WARNING: no CorfuServer process found after start'"
}

stop_corfu() {
  echo "[corfu] stopping on $SSH_TARGET"
  # bin/corfu_server is a shell wrapper that execs java, so the live process
  # cmdline is `java ... org.corfudb.infrastructure.CorfuServer ...` with
  # no "corfu_server" substring. Match on the java main class instead, plus
  # the port as a belt-and-braces second pattern. Go straight to SIGKILL --
  # we reset /data/run_batch on every start so graceful shutdown buys us
  # nothing.
  remote_sh "pkill -KILL -f 'org.corfudb.infrastructure.CorfuServer' 2>/dev/null || true; pkill -KILL -f '\-a $REMOTE_HOST $CORFU_PORT' 2>/dev/null || true; command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null || true"
  for _ in $(seq 1 30); do
    if ! port_open "$REMOTE_HOST" "$CORFU_PORT"; then
      echo "[corfu] down"
      return 0
    fi
    sleep 1
  done
  echo "[corfu] ERROR: $REMOTE_HOST:$CORFU_PORT still reachable after SIGKILL" >&2
  remote_sh "pgrep -af CorfuServer 2>/dev/null || true; ss -ltnp 2>/dev/null | grep :${CORFU_PORT} || netstat -ltnp 2>/dev/null | grep :${CORFU_PORT} || true"
  return 1
}

wait_for_corfu() {
  echo "[corfu] waiting for $REMOTE_HOST:$CORFU_PORT"
  for _ in $(seq 1 120); do
    if port_open "$REMOTE_HOST" "$CORFU_PORT"; then
      echo "[corfu] up"
      sleep 10
      return 0
    fi
    sleep 1
  done
  echo "[corfu] ERROR: never became reachable (tail $CORFU_LOG on $REMOTE_HOST)" >&2
  return 1
}

FINAL_STATE_LEFT_RUNNING=0
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ "$FINAL_STATE_LEFT_RUNNING" -eq 0 ]]; then
    echo "[orchestrator] cleanup (rc=$rc) -- stopping remote corfu"
    stop_corfu || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

BENCH_SCRIPT="$OZONEDB_HOME/bench/scripts/local/run_local_ycsb_multiproc.py"
RESULTS=()

echo "=== sweep: workloads=(${WORKLOADS_ARR[*]}) writers=(${WRITERS_ARR[*]}) ==="

for WL in "${WORKLOADS_ARR[@]}"; do
  for NW in "${WRITERS_ARR[@]}"; do
    echo ""
    echo "### iteration: workload=$WL writers=$NW ###"

    stop_corfu || true
    start_corfu
    wait_for_corfu

    echo "[bench] python3 $BENCH_SCRIPT --config $CONFIG --workloads $WL --num_writers $NW"
    set +e
    python3 "$BENCH_SCRIPT" --config "$CONFIG" --workloads "$WL" --num_writers "$NW"
    rc=$?
    set -e
    RESULTS+=("workload=$WL writers=$NW rc=$rc")
    echo "[bench] workload=$WL writers=$NW rc=$rc"

    # Best-effort: next iteration's defensive stop_corfu will retry anyway,
    # and we'd rather continue the sweep than abort on a flaky teardown.
    stop_corfu || echo "[orchestrator] warning: stop_corfu failed; continuing sweep"
  done
done

echo ""
echo "=== final restart: corfu on $REMOTE_HOST:$CORFU_PORT ==="
start_corfu
wait_for_corfu
FINAL_STATE_LEFT_RUNNING=1

echo ""
echo "=== sweep summary ==="
for line in "${RESULTS[@]}"; do
  echo "  $line"
done

# nonzero exit if any iteration failed
for line in "${RESULTS[@]}"; do
  if [[ "$line" != *"rc=0"* ]]; then
    exit 1
  fi
done
exit 0
