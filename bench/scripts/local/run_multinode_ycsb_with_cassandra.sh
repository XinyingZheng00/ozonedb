#!/usr/bin/env bash
set -euo pipefail

# The Cassandra twin of run_multinode_ycsb_with_corfu.sh: sweep YCSB workloads
# and writers-per-host over the client nodes against the Cassandra baseline
# server(s), restoring the loaded snapshot before every trial.
#
# Loop order: trials -> workloads -> writers_per_host. Per iteration:
#   1. on every server: cassandra_ctl.sh restore-load  (stop, data.load -> data)
#   2. on every server: cassandra_ctl.sh start && wait
#   3. run_multinode_ycsb.py --db_name cassandra [--cassandra_consistency MODE]
#        --workloads <wl> --writers_per_host <n> --trial <k> [--max_exec_time <s>]
#      (SSHes to every client host and launches its writer slice)
#   4. on every server: cassandra_ctl.sh stop
# After the sweep the servers are started once more and left running.
#
# The consistency mode is a flag, never a ycsb.yaml edit, exactly like
# --linearizable on the corfu path: --consistency is forwarded to every client
# and lands in the result filenames as cassandra-one / -quorum / -serial.
# The frontier is three invocations of this script with the same --run-tag:
#   --consistency one     eventual        (== quorum at RF=1)
#   --consistency quorum  strong-ish
#   --consistency serial  linearizable    (LWT writes + SERIAL reads)
#
# PRECONDITIONS
#   - setup.sh --role cassandra ran on every nodes.cassandra entry.
#   - load_multinode_cassandra.sh produced data.load on every server
#     (or pass --no-restore to run against whatever is loaded now).
#   - cloudlab.hosts / nodes.ssh_user / nodes.ssh_private_key_path resolve.
#   - Corfu is not being benchmarked at the same time: by default the
#     Cassandra server shares the log node.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"
CTL_SRC="${OZONEDB_HOME}/bench/scripts/cassandra_ctl.sh"
CQL_SRC="${OZONEDB_HOME}/bench/scripts/cassandra_cql.py"

CLIENT_HOSTS=""
CLIENT_USER=""
CLIENT_SSH_KEY=""

WORKLOADS="a b c d f"
WRITERS_LIST="1 2 4 8"
TRIALS=1
TRIALS_SET=0
TRIAL=""
DURATION=""
CONSISTENCY=""   # --consistency one|quorum|serial; default: each client's cassandra.consistency
RUN_TAG=""
RESTORE=1        # --no-restore: skip restore-load (no snapshot yet, or deliberate warm state)
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH             ycsb.yaml (default: $CONFIG)

  YCSB client nodes (orchestrated via run_multinode_ycsb.py):
  --client-hosts "h1,h2"    comma-separated (default: cloudlab.hosts from ycsb.yaml)
  --client-user USER        default: nodes.ssh_user
  --client-ssh-key PATH     default: nodes.ssh_private_key_path

  Sweep:
  --workloads "a b c"       space-separated workload letters (default: "$WORKLOADS")
  --writers-list "1 2"      space-separated writers_per_host values (default: "$WRITERS_LIST")
  --trials N                run trials 1..N (default: $TRIALS)
  --trial N                 run exactly trial N (campaign-driver form; excludes --trials)
  --duration SECONDS        cap every YCSB run (forwarded as --max_exec_time)
  --consistency MODE        one | quorum | serial: forwarded to every client as
                            --cassandra_consistency; result files are labelled
                            cassandra-MODE. Default: each client's ycsb.yaml.
  --run-tag TAG             results dir bench/results/local/TAG everywhere (default: timestamp)
  --no-restore              do not restore the load snapshot before each trial
  --dry-run                 print the plan and the exact run_multinode_ycsb.py
                            command per iteration; no ssh, no server restart
  -h | --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --config)         CONFIG="$2"; shift 2 ;;
  --client-hosts)   CLIENT_HOSTS="$2"; shift 2 ;;
  --client-user)    CLIENT_USER="$2"; shift 2 ;;
  --client-ssh-key) CLIENT_SSH_KEY="$2"; shift 2 ;;
  --workloads)      WORKLOADS="$2"; shift 2 ;;
  --writers-list)   WRITERS_LIST="$2"; shift 2 ;;
  --trials)         TRIALS="$2"; TRIALS_SET=1; shift 2 ;;
  --trial)          TRIAL="$2"; shift 2 ;;
  --duration)       DURATION="$2"; shift 2 ;;
  --consistency)    CONSISTENCY="$2"; shift 2 ;;
  --run-tag)        RUN_TAG="$2"; shift 2 ;;
  --no-restore)     RESTORE=0; shift ;;
  --dry-run)        DRY_RUN=1; shift ;;
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
if [[ -n "$TRIAL" ]]; then
  if [[ "$TRIALS_SET" -eq 1 ]]; then
    echo "--trial and --trials are mutually exclusive" >&2
    exit 1
  fi
  if ! [[ "$TRIAL" =~ ^[1-9][0-9]*$ ]]; then
    echo "--trial must be a positive integer (got: $TRIAL)" >&2
    exit 1
  fi
  TRIAL_SEQ=("$TRIAL")
else
  read -r -a TRIAL_SEQ <<<"$(seq 1 "$TRIALS" | tr '\n' ' ')"
fi
if [[ -n "$DURATION" ]] && ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]]; then
  echo "--duration must be a positive integer (got: $DURATION)" >&2
  exit 1
fi
if [[ -n "$CONSISTENCY" ]] && ! [[ "$CONSISTENCY" =~ ^(one|quorum|serial)$ ]]; then
  echo "--consistency must be one|quorum|serial (got: $CONSISTENCY)" >&2
  exit 1
fi
if [[ -n "$RUN_TAG" ]] && ! [[ "$RUN_TAG" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "--run-tag is a directory name and must match [A-Za-z0-9._-]+ (got: $RUN_TAG)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Servers from ycsb.yaml's nodes.cassandra (fallback nodes.log), never a
# hardcoded constant -- the same rule the corfu wrapper follows.
# ---------------------------------------------------------------------------
cfg() { python3 "$OZONEDB_HOME/bench/scripts/ycsb_config.py" --config "$CONFIG" "$@"; }
SSH_USER="$(cfg --get nodes.ssh_user)"
SSH_KEY="$(cfg --get nodes.ssh_private_key_path)"
SSH_KEY="${SSH_KEY/#\~/$HOME}"
INSTALL_DIR="$(cfg --get cassandra.install_dir)"
CONTACT_POINTS="$(cfg --get cassandra.contact_points)"
read -r -a SERVERS <<<"$(cfg --list cassandra --field ssh | tr '\n' ' ')"
[[ ${#SERVERS[@]} -gt 0 ]] || { echo "no cassandra servers resolved from $CONFIG" >&2; exit 1; }
echo "[cassandra] servers: ssh=(${SERVERS[*]}) contact_points=$CONTACT_POINTS (from $CONFIG)"

SSH_OPTS=(-o BatchMode=yes -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new -i "$SSH_KEY")
CTL="$INSTALL_DIR/cassandra_ctl.sh"

rsh() { ssh "${SSH_OPTS[@]}" "$SSH_USER@$1" "$2"; }
push_ctl() {
  scp "${SSH_OPTS[@]}" -q "$CTL_SRC" "$SSH_USER@$1:$CTL"
  scp "${SSH_OPTS[@]}" -q "$CQL_SRC" "$SSH_USER@$1:$INSTALL_DIR/cassandra_cql.py"
}
ctl_all() {
  local s
  for s in "${SERVERS[@]}"; do
    echo "[cassandra:$s] $CTL $*"
    rsh "$s" "$CTL $*"
  done
}

start_servers() {
  if [[ "$RESTORE" -eq 1 ]]; then
    ctl_all restore-load
  else
    ctl_all stop
  fi
  ctl_all start
  ctl_all wait
}

stop_servers() { ctl_all stop; }

FINAL_STATE_LEFT_RUNNING=0
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ "$FINAL_STATE_LEFT_RUNNING" -eq 0 ]]; then
    echo "[orchestrator] cleanup (rc=$rc) -- stopping cassandra"
    stop_servers || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

BENCH_SCRIPT="$OZONEDB_HOME/bench/scripts/local/run_multinode_ycsb.py"
RESULTS=()

export OZONEDB_RUN_TAG="${RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"

COMMON_ARGS=(--config "$CONFIG" --run_tag "$OZONEDB_RUN_TAG" --db_name cassandra)
[[ -n "$CLIENT_HOSTS"   ]] && COMMON_ARGS+=(--hosts "$CLIENT_HOSTS")
[[ -n "$CLIENT_USER"    ]] && COMMON_ARGS+=(--ssh_user "$CLIENT_USER")
[[ -n "$CLIENT_SSH_KEY" ]] && COMMON_ARGS+=(--ssh_key "$CLIENT_SSH_KEY")
[[ -n "$DURATION"       ]] && COMMON_ARGS+=(--max_exec_time "$DURATION")
[[ -n "$CONSISTENCY"    ]] && COMMON_ARGS+=(--cassandra_consistency "$CONSISTENCY")

echo "=== sweep: workloads=(${WORKLOADS_ARR[*]}) writers_per_host=(${WRITERS_ARR[*]}) trials=(${TRIAL_SEQ[*]}) consistency=${CONSISTENCY:-yaml} duration=${DURATION:-yaml} restore=$RESTORE run_tag=$OZONEDB_RUN_TAG ==="
if [[ -n "$CLIENT_HOSTS" ]]; then
  echo "=== client hosts (override): $CLIENT_HOSTS ==="
else
  echo "=== client hosts: from cloudlab.hosts in $CONFIG ==="
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  for TRIAL in "${TRIAL_SEQ[@]}"; do
    for WL in "${WORKLOADS_ARR[@]}"; do
      for NW in "${WRITERS_ARR[@]}"; do
        echo "[dry-run] trial=$TRIAL workload=$WL writers_per_host=$NW: on each server $CTL $([[ $RESTORE -eq 1 ]] && echo restore-load || echo stop) && start && wait, then"
        echo "[dry-run]   python3 $BENCH_SCRIPT ${COMMON_ARGS[*]} --workloads $WL --writers_per_host $NW --trial $TRIAL"
      done
    done
  done
  FINAL_STATE_LEFT_RUNNING=1
  exit 0
fi

for s in "${SERVERS[@]}"; do push_ctl "$s"; done

for TRIAL in "${TRIAL_SEQ[@]}"; do
  for WL in "${WORKLOADS_ARR[@]}"; do
    for NW in "${WRITERS_ARR[@]}"; do
      echo ""
      echo "### iteration: trial=$TRIAL (of: ${TRIAL_SEQ[*]}) workload=$WL writers_per_host=$NW consistency=${CONSISTENCY:-yaml} ###"

      start_servers

      echo "[bench] python3 $BENCH_SCRIPT ${COMMON_ARGS[*]} --workloads $WL --writers_per_host $NW --trial $TRIAL"
      set +e
      python3 "$BENCH_SCRIPT" "${COMMON_ARGS[@]}" \
        --workloads "$WL" \
        --writers_per_host "$NW" \
        --trial "$TRIAL"
      rc=$?
      set -e
      RESULTS+=("trial=$TRIAL workload=$WL writers_per_host=$NW consistency=${CONSISTENCY:-yaml} rc=$rc")
      echo "[bench] trial=$TRIAL workload=$WL writers_per_host=$NW rc=$rc"

      stop_servers || echo "[orchestrator] warning: stop failed; continuing sweep"
    done
  done
done

echo ""
echo "=== final restart: cassandra on ${SERVERS[*]} ==="
RESTORE=0 start_servers
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
