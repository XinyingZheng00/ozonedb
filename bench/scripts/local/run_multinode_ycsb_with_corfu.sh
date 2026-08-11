#!/usr/bin/env bash
set -euo pipefail

# Orchestrate a single Corfu server on one remote node while sweeping YCSB
# workloads and writers-per-host on N other client nodes via the Python
# multi-node orchestrator (run_multinode_ycsb.py).
#
# Same shape as run_ycsb_with_corfu.sh, except:
#   - YCSB does not run on this machine. It runs on $CLIENT_HOSTS (each host
#     spawns writers_per_host processes via run_local_ycsb_multiproc.py).
#   - Corfu still runs on a single dedicated $CORFU_HOST. All YCSB writers
#     across all client hosts share that one Corfu instance and stream.
#
# Loop order: trials -> workloads -> writers_per_host. Corfu is restarted on
# every (trial, workload, writers_per_host) iteration. All iterations share
# one OZONEDB_RUN_TAG so result files land in one tagged dir.
#
# Per iteration:
#   1. stop corfu on $CORFU_HOST (best-effort)
#   2. start corfu on $CORFU_HOST (clean /mnt/corfu/run_batch from /mnt/corfu/load)
#   3. wait until $CORFU_HOST:$CORFU_PORT is reachable
#   4. run run_multinode_ycsb.py with
#        --workloads=<wl> --writers_per_host=<n> --trial=<k>
#      (this SSHes to every client host and launches its writer slice)
#   5. stop corfu
# After the full sweep, corfu is started one more time so the cluster is
# left ready for a subsequent manual run.
#
# PRECONDITIONS
#   - Each client host's ycsb.yaml has corfu.endpoint set to
#     $CORFU_HOST:$CORFU_PORT (so its writer processes connect to the same
#     instance this script restarts).
#   - cloudlab.hosts in ycsb.yaml lists the client hosts (or pass
#     --client-hosts).
#   - cloudlab.ssh_user / cloudlab.ssh_private_key_path resolve for the
#     client hosts (or pass --client-user / --client-ssh-key).
#   - For non-corfu db_names (rocksdb, ozonedb local, sqlite) the loader
#     load_local_ycsb_multiproc.py must already have produced the per-writer
#     cached_data dirs on each client host. ozonedb-corfu does not need this.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"

CORFU_HOST="10.10.1.2"
CORFU_USER="${SSH_USER:-$USER}"
CORFU_SSH_KEY="~/.ssh/cloudlab"
CORFU_DIR="~/CorfuDB"
CORFU_PORT=9090
CORFU_LOG="/tmp/corfu_server.log"

CLIENT_HOSTS=""
CLIENT_USER=""
CLIENT_SSH_KEY=""

WORKLOADS="a b c d f"
WRITERS_LIST="1 2 4 8"
TRIALS=1

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH             ycsb.yaml (default: $CONFIG)

  Corfu server (one node):
  --corfu-host HOST         (default: $CORFU_HOST)
  --corfu-user USER         (default: $CORFU_USER)
  --corfu-ssh-key PATH      (default: $CORFU_SSH_KEY)
  --corfu-dir PATH          corfu repo on remote (default: $CORFU_DIR)
  --corfu-port PORT         (default: $CORFU_PORT)

  YCSB client nodes (orchestrated via run_multinode_ycsb.py):
  --client-hosts "h1,h2"    comma-separated. Default: read cloudlab.hosts
                            from ycsb.yaml.
  --client-user USER        default: read cloudlab.ssh_user from ycsb.yaml.
  --client-ssh-key PATH     default: read cloudlab.ssh_private_key_path
                            from ycsb.yaml.

  Sweep:
  --workloads "a b c"       space-separated workload letters
                            (default: "$WORKLOADS")
  --writers-list "1 2"      space-separated writers_per_host values
                            (default: "$WRITERS_LIST")
  --trials N                trials per (workload, writers_per_host)
                            (default: $TRIALS)

  -h | --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --config)         CONFIG="$2"; shift 2 ;;
  --corfu-host)     CORFU_HOST="$2"; shift 2 ;;
  --corfu-user)     CORFU_USER="$2"; shift 2 ;;
  --corfu-ssh-key)  CORFU_SSH_KEY="$2"; shift 2 ;;
  --corfu-dir)      CORFU_DIR="$2"; shift 2 ;;
  --corfu-port)     CORFU_PORT="$2"; shift 2 ;;
  --client-hosts)   CLIENT_HOSTS="$2"; shift 2 ;;
  --client-user)    CLIENT_USER="$2"; shift 2 ;;
  --client-ssh-key) CLIENT_SSH_KEY="$2"; shift 2 ;;
  --workloads)      WORKLOADS="$2"; shift 2 ;;
  --writers-list)   WRITERS_LIST="$2"; shift 2 ;;
  --trials)         TRIALS="$2"; shift 2 ;;
  -h | --help)      usage; exit 0 ;;
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

CORFU_SSH_OPTS=(-o BatchMode=yes -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new)
[[ -n "$CORFU_SSH_KEY" ]] && CORFU_SSH_OPTS+=(-i "$CORFU_SSH_KEY")
CORFU_TARGET="${CORFU_USER}@${CORFU_HOST}"

corfu_sh() {
  ssh "${CORFU_SSH_OPTS[@]}" "$CORFU_TARGET" "$1"
}

port_open() {
  (exec 3<>/dev/tcp/"$1"/"$2") >/dev/null 2>&1
}

start_corfu() {
  echo "[corfu] starting on $CORFU_TARGET ($CORFU_HOST:$CORFU_PORT)"
  # Trailing & must apply only to nohup, not to the whole chain — otherwise
  # ssh holds stdout open on a foregrounded subshell and hangs.
  corfu_sh "cd $CORFU_DIR && rm -rf /mnt/corfu/run_batch/ && cp -r /mnt/corfu/load/ /mnt/corfu/run_batch/ && ( setsid nohup env CORFUDB_HEAP=122880 ./bin/corfu_server -l /mnt/corfu/run_batch -s -a $CORFU_HOST $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
  sleep 10
  corfu_sh "pgrep -af 'org.corfudb.infrastructure.CorfuServer' | head -n1 || echo '[corfu] WARNING: no CorfuServer process found after start'"
}

stop_corfu() {
  echo "[corfu] stopping on $CORFU_TARGET"
  # bin/corfu_server execs java, so the live cmdline matches the main class,
  # not the wrapper name. SIGKILL directly — we wipe /mnt/corfu/run_batch
  # next start, so graceful shutdown buys nothing.
  corfu_sh "pkill -KILL -f 'org.corfudb.infrastructure.CorfuServer' 2>/dev/null || true; pkill -KILL -f '\-a $CORFU_HOST $CORFU_PORT' 2>/dev/null || true; command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null || true"
  for _ in $(seq 1 30); do
    if ! port_open "$CORFU_HOST" "$CORFU_PORT"; then
      echo "[corfu] down"
      return 0
    fi
    sleep 1
  done
  echo "[corfu] ERROR: $CORFU_HOST:$CORFU_PORT still reachable after SIGKILL" >&2
  corfu_sh "pgrep -af CorfuServer 2>/dev/null || true; ss -ltnp 2>/dev/null | grep :${CORFU_PORT} || netstat -ltnp 2>/dev/null | grep :${CORFU_PORT} || true"
  return 1
}

wait_for_corfu() {
  echo "[corfu] waiting for $CORFU_HOST:$CORFU_PORT"
  for _ in $(seq 1 120); do
    if port_open "$CORFU_HOST" "$CORFU_PORT"; then
      echo "[corfu] up"
      sleep 10
      return 0
    fi
    sleep 1
  done
  echo "[corfu] ERROR: never became reachable (tail $CORFU_LOG on $CORFU_HOST)" >&2
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

BENCH_SCRIPT="$OZONEDB_HOME/bench/scripts/local/run_multinode_ycsb.py"
RESULTS=()

# All trials/workloads/writers iterations land under this single tag.
# run_multinode_ycsb.py also passes it through to each client host so
# every node writes into the same tag dir.
export OZONEDB_RUN_TAG="$(date +%Y%m%d-%H%M%S)"

# Common args to every run_multinode_ycsb.py invocation.
COMMON_ARGS=(--config "$CONFIG" --run_tag "$OZONEDB_RUN_TAG")
[[ -n "$CLIENT_HOSTS"   ]] && COMMON_ARGS+=(--hosts "$CLIENT_HOSTS")
[[ -n "$CLIENT_USER"    ]] && COMMON_ARGS+=(--ssh_user "$CLIENT_USER")
[[ -n "$CLIENT_SSH_KEY" ]] && COMMON_ARGS+=(--ssh_key "$CLIENT_SSH_KEY")

echo "=== sweep: workloads=(${WORKLOADS_ARR[*]}) writers_per_host=(${WRITERS_ARR[*]}) trials=$TRIALS run_tag=$OZONEDB_RUN_TAG ==="
echo "=== corfu server: $CORFU_TARGET:$CORFU_PORT ==="
if [[ -n "$CLIENT_HOSTS" ]]; then
  echo "=== client hosts (override): $CLIENT_HOSTS ==="
else
  echo "=== client hosts: from cloudlab.hosts in $CONFIG ==="
fi

for ((TRIAL = 1; TRIAL <= TRIALS; TRIAL++)); do
  for WL in "${WORKLOADS_ARR[@]}"; do
    for NW in "${WRITERS_ARR[@]}"; do
      echo ""
      echo "### iteration: trial=$TRIAL/$TRIALS workload=$WL writers_per_host=$NW ###"

      stop_corfu || true
      start_corfu
      wait_for_corfu

      echo "[bench] python3 $BENCH_SCRIPT ${COMMON_ARGS[*]} --workloads $WL --writers_per_host $NW --trial $TRIAL"
      set +e
      python3 "$BENCH_SCRIPT" "${COMMON_ARGS[@]}" \
        --workloads "$WL" \
        --writers_per_host "$NW" \
        --trial "$TRIAL"
      rc=$?
      set -e
      RESULTS+=("trial=$TRIAL workload=$WL writers_per_host=$NW rc=$rc")
      echo "[bench] trial=$TRIAL workload=$WL writers_per_host=$NW rc=$rc"

      stop_corfu || echo "[orchestrator] warning: stop_corfu failed; continuing sweep"
    done
  done
done

echo ""
echo "=== final restart: corfu on $CORFU_HOST:$CORFU_PORT ==="
start_corfu
wait_for_corfu
FINAL_STATE_LEFT_RUNNING=1

echo ""
echo "=== sweep summary ==="
for line in "${RESULTS[@]}"; do
  echo "  $line"
done

for line in "${RESULTS[@]}"; do
  if [[ "$line" != *"rc=0"* ]]; then
    exit 1
  fi
done
exit 0
