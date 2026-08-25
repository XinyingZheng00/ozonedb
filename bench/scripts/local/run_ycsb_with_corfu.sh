#!/usr/bin/env bash
set -euo pipefail

# Orchestrate a Corfu server on a remote node while sweeping YCSB workloads
# and writer-process counts on this machine.
#
# Loop order: trials -> workloads -> writers. Corfu is restarted on every
# (trial, workload, writers) iteration. All iterations share a single
# OZONEDB_RUN_TAG so their result files land under one timestamped dir.
#
# Per iteration:
#   1. start corfu on $REMOTE_HOST:
#        cd $CORFU_DIR && rm -rf /mnt/corfu/run_batch/corfu \
#          && cp -r /mnt/corfu/load/corfu /mnt/corfu/run_batch \
#          && JAVA_ARGS="-Xmx120g" ./bin/corfu_server \
#                 -l /mnt/corfu/run_batch -s -a $BIND_HOST $CORFU_PORT
#   2. run run_local_ycsb_multiproc.py with
#      --workloads=<wl> --num_writers=<n> --trial=<k>
#   3. stop corfu
# After the full sweep, corfu is started one more time (so the node is left
# ready for a subsequent manual run).
#
# On error before the final start, corfu is stopped so nothing leaks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"
# Empty: resolved from ycsb.yaml after argument parsing.
REMOTE_HOST=""
BIND_HOST=""
REMOTE_USER="${SSH_USER:-}"
SSH_KEY=""
CORFU_DIR="~/CorfuDB"
CORFU_PORT=""
CORFU_LOG="/tmp/corfu_server.log"
WORKLOADS="a b c d f"
WRITERS_LIST="1 2 4 8"
TRIALS=1
DURATION=""
LINEARIZABLE=0
BENCH_SCRIPT_OVERRIDE=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH         ycsb.yaml (default: $CONFIG)
  --remote HOST         corfu host (default: nodes.log.ssh from ycsb.yaml)
  --user USER           ssh user on remote (default: nodes.ssh_user from ycsb.yaml)
  --ssh-key PATH        ssh private key (optional)
  --corfu-dir PATH      corfu repo on remote (default: $CORFU_DIR)
  --corfu-port PORT     corfu port (default: corfu.port from ycsb.yaml)
  --workloads "a b c"   space-separated workload letters to sweep
                        (default: "$WORKLOADS")
  --writers-list "1 2"  space-separated num_writers values to sweep
                        (default: "$WRITERS_LIST")
  --trials N            number of trials per (workload, writers) iteration
                        (default: $TRIALS)
  --duration SECONDS    cap each YCSB iteration at this many seconds via
                        YCSB's maxexecutiontime property (default: from
                        ycsb.yaml local.run.max_exec_time, else uncapped)
  --linearizable        strict reads: linearizable_reads=true +
                        trust_background_tail=false in every writer's
                        generated shared_config, and result files labelled
                        ozonedb-corfu-linearizable instead of ozonedb-corfu.
                        Use this rather than editing ycsb.yaml.
  --script PATH         override the inner python bench script
                        (default: \$OZONEDB_HOME/bench/scripts/local/run_local_ycsb_multiproc.py).
                        The override must accept the same flags this script
                        passes: --config --workloads --num_writers --trial
                        [--max_exec_time] [--linearizable].
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
  --trials)
    TRIALS="$2"
    shift 2
    ;;
  --duration)
    DURATION="$2"
    shift 2
    ;;
  --linearizable)
    LINEARIZABLE=1
    shift
    ;;
  --script)
    BENCH_SCRIPT_OVERRIDE="$2"
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
if ! [[ "$TRIALS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--trials must be a positive integer (got: $TRIALS)" >&2
  exit 1
fi
if [[ -n "$DURATION" ]] && ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]]; then
  echo "--duration must be a positive integer (got: $DURATION)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Corfu node resolved from ycsb.yaml's `nodes:` block, not a hardcoded constant.
# BIND (nodes.log.lan) is what corfu_server -a binds and what clients dial;
# SSH (nodes.log.ssh, falling back to lan) is how this script reaches the box.
# --host sets both, as it did before.
# ---------------------------------------------------------------------------
cfg() { python3 "$OZONEDB_HOME/bench/scripts/ycsb_config.py" --config "$CONFIG" "$@"; }
[[ -n "$BIND_HOST"   ]] || BIND_HOST="$(cfg --node log --field lan)"  || exit 1
[[ -n "$REMOTE_HOST" ]] || REMOTE_HOST="$(cfg --node log --field ssh)" || exit 1
[[ -n "$REMOTE_USER" ]] || REMOTE_USER="$(cfg --get nodes.ssh_user)"   || exit 1
[[ -n "$SSH_KEY"     ]] || SSH_KEY="$(cfg --get nodes.ssh_private_key_path)" || exit 1
[[ -n "$CORFU_PORT"  ]] || CORFU_PORT="$(cfg --get corfu.port)"        || exit 1
echo "[corfu] node: ssh=$REMOTE_HOST bind=$BIND_HOST port=$CORFU_PORT (from $CONFIG)"

SSH_OPTS=(-o BatchMode=yes -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new)
[[ -n "$SSH_KEY" ]] && SSH_OPTS+=(-i "$SSH_KEY")
SSH_TARGET="${REMOTE_USER}@${REMOTE_HOST}"

remote_sh() {
  ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "$1"
}

# Probed ON the node against its own bind address -- see the multi-node wrapper.
port_open() {
  remote_sh "bash -c '(exec 3<>/dev/tcp/$1/$2) >/dev/null 2>&1'" >/dev/null 2>&1
}

start_corfu() {
  echo "[corfu] starting on $SSH_TARGET ($REMOTE_HOST:$CORFU_PORT)"
  # The trailing `&` must apply ONLY to nohup, not to the full `cd && rm &&
  # cp && nohup ...` chain. Without the parentheses bash backgrounds the
  # whole chain in one subshell that then runs the chain sequentially --
  # the subshell would wait synchronously on corfu_server, holding ssh's
  # stdout/stderr pipes open, and ssh would hang forever.
  remote_sh "cd $CORFU_DIR && rm -rf /mnt/corfu/run_batch/ && cp -r /mnt/corfu/load/ /mnt/corfu/run_batch/ && ( setsid nohup env CORFUDB_HEAP=122880 ./bin/corfu_server -l /mnt/corfu/run_batch -s -a $BIND_HOST $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
  sleep 10
  remote_sh "pgrep -af 'org.corfudb.infrastructure.CorfuServer' | head -n1 || echo '[corfu] WARNING: no CorfuServer process found after start'"
}

stop_corfu() {
  echo "[corfu] stopping on $SSH_TARGET"
  # bin/corfu_server is a shell wrapper that execs java, so the live process
  # cmdline is `java ... org.corfudb.infrastructure.CorfuServer ...` with
  # no "corfu_server" substring. Match on the java main class instead, plus
  # the port as a belt-and-braces second pattern. Go straight to SIGKILL --
  # we reset /mnt/corfu/run_batch on every start so graceful shutdown buys us
  # nothing.
  remote_sh "pkill -KILL -f 'org.corfudb.infrastructure.CorfuServer' 2>/dev/null || true; pkill -KILL -f '\-a $BIND_HOST $CORFU_PORT' 2>/dev/null || true; command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null || true"
  for _ in $(seq 1 30); do
    if ! port_open "$BIND_HOST" "$CORFU_PORT"; then
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
    if port_open "$BIND_HOST" "$CORFU_PORT"; then
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

if [[ -n "$BENCH_SCRIPT_OVERRIDE" ]]; then
  BENCH_SCRIPT="$BENCH_SCRIPT_OVERRIDE"
else
  BENCH_SCRIPT="$OZONEDB_HOME/bench/scripts/local/run_local_ycsb_multiproc.py"
fi
if [[ ! -f "$BENCH_SCRIPT" ]]; then
  echo "bench script not found: $BENCH_SCRIPT" >&2
  exit 1
fi
RESULTS=()

# Shared timestamped results directory across all (trial, workload, writers)
# iterations of this sweep. Per-trial result files are differentiated by the
# _trial{N} suffix that the python script appends.
export OZONEDB_RUN_TAG="$(date +%Y%m%d-%H%M%S)"
READ_MODE=default
[[ "$LINEARIZABLE" -eq 1 ]] && READ_MODE=linearizable
echo "=== sweep: workloads=(${WORKLOADS_ARR[*]}) writers=(${WRITERS_ARR[*]}) trials=$TRIALS read_mode=$READ_MODE run_tag=$OZONEDB_RUN_TAG ==="

for ((TRIAL = 1; TRIAL <= TRIALS; TRIAL++)); do
  for WL in "${WORKLOADS_ARR[@]}"; do
    for NW in "${WRITERS_ARR[@]}"; do
      echo ""
      echo "### iteration: trial=$TRIAL/$TRIALS workload=$WL writers=$NW ###"

      stop_corfu || true
      start_corfu
      wait_for_corfu

      bench_args=(--config "$CONFIG" --workloads "$WL" --num_writers "$NW" --trial "$TRIAL")
      [[ -n "$DURATION" ]] && bench_args+=(--max_exec_time "$DURATION")
      [[ "$LINEARIZABLE" -eq 1 ]] && bench_args+=(--linearizable)
      echo "[bench] python3 $BENCH_SCRIPT ${bench_args[*]}"
      set +e
      python3 "$BENCH_SCRIPT" "${bench_args[@]}"
      rc=$?
      set -e
      RESULTS+=("trial=$TRIAL workload=$WL writers=$NW rc=$rc")
      echo "[bench] trial=$TRIAL workload=$WL writers=$NW rc=$rc"

      # Best-effort: next iteration's defensive stop_corfu will retry anyway,
      # and we'd rather continue the sweep than abort on a flaky teardown.
      stop_corfu || echo "[orchestrator] warning: stop_corfu failed; continuing sweep"
    done
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
