#!/usr/bin/env bash
set -euo pipefail

# Load the YCSB dataset into the Corfu shared log, then snapshot it to
# /mnt/corfu/load so that run_multinode_ycsb_with_corfu.sh restores an
# identical stream before every trial (it does cp -r /mnt/corfu/load ->
# run_batch on each start).
#
# There was no scripted corfu load before this: the run wrapper assumed
# /mnt/corfu/load already held a dataset. This is the OzoneDB twin of
# load_multinode_cassandra.sh.
#
# Steps (corfu server = nodes.log, load host = first client):
#   1. stop any corfu on the log node (best effort -- takes the node over)
#   2. wipe /mnt/corfu/run_batch, start a FRESH corfu bound to nodes.log.lan
#   3. run load_local_ycsb_multiproc.py --db_name ozonedb-corfu on ONE client
#      with --writers procs (all share the one stream; record_cnt / key_size
#      come from local.load in ycsb.yaml)
#   4. stop corfu (flush to /mnt/corfu/run_batch on disk)
#   5. snapshot: rm -rf /mnt/corfu/load && cp -r run_batch -> load
#
# Check that the cluster is free first: this KILLS any running corfu on the
# log node and overwrites /mnt/corfu/{run_batch,load}.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"
CORFU_DIR="~/CorfuDB"
CORFU_LOG="/tmp/corfu_server_load.log"
CORFU_DATA="/mnt/corfu"
CORFU_HEAP="${CORFU_HEAP:-122880}"

WRITERS=8
LOAD_HOST=""
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH     ycsb.yaml (default: $CONFIG)
  --writers N       loader processes on the load host (default: $WRITERS)
  --load-host HOST  client that runs the loader (default: first cloudlab.hosts)
  --corfu-dir PATH  corfu repo on the log node (default: $CORFU_DIR)
  --dry-run         print the plan; no ssh
  -h | --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --config) CONFIG="$2"; shift 2 ;;
  --writers) WRITERS="$2"; shift 2 ;;
  --load-host) LOAD_HOST="$2"; shift 2 ;;
  --corfu-dir) CORFU_DIR="$2"; shift 2 ;;
  --dry-run) DRY_RUN=1; shift ;;
  -h | --help) usage; exit 0 ;;
  *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done
[[ "$WRITERS" =~ ^[1-9][0-9]*$ ]] || { echo "--writers must be a positive integer" >&2; exit 1; }

cfg() { python3 "$OZONEDB_HOME/bench/scripts/ycsb_config.py" --config "$CONFIG" "$@"; }
SSH_USER="$(cfg --get nodes.ssh_user)"
SSH_KEY="$(cfg --get nodes.ssh_private_key_path)"; SSH_KEY="${SSH_KEY/#\~/$HOME}"
CORFU_SSH="$(cfg --ssh-target log)"                 # user@public
CORFU_BIND="$(cfg --node log --field lan)"          # what corfu_server -a binds
CORFU_PORT="$(cfg --get corfu.port)"
[[ -n "$LOAD_HOST" ]] || LOAD_HOST="$(cfg --get cloudlab.hosts | python3 -c 'import json,sys; print(json.load(sys.stdin)[0])')"

SSH_OPTS=(-o BatchMode=yes -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new -i "$SSH_KEY")
corfu_sh() { ssh "${SSH_OPTS[@]}" "$CORFU_SSH" "$1"; }
port_open() { corfu_sh "bash -c '(exec 3<>/dev/tcp/$CORFU_BIND/$CORFU_PORT) >/dev/null 2>&1'" >/dev/null 2>&1; }

stop_corfu() {
  echo "[corfu] stop on $CORFU_SSH"
  corfu_sh "pkill -KILL -f 'org.corfudb.infrastructure.CorfuServer' 2>/dev/null || true; command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null || true"
  local i
  for i in $(seq 1 30); do port_open || { echo "[corfu] down"; return 0; }; sleep 1; done
  echo "[corfu] ERROR: still reachable after SIGKILL" >&2; return 1
}

start_fresh_corfu() {
  echo "[corfu] start FRESH (empty run_batch) on $CORFU_BIND:$CORFU_PORT"
  corfu_sh "cd $CORFU_DIR && rm -rf $CORFU_DATA/run_batch && mkdir -p $CORFU_DATA/run_batch && ( setsid nohup env CORFUDB_HEAP=$CORFU_HEAP JAVA_ARGS=-Xmx120g ./bin/corfu_server -l $CORFU_DATA/run_batch -s -a $CORFU_BIND $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
  local i
  for i in $(seq 1 120); do port_open && { echo "[corfu] up"; sleep 10; return 0; }; sleep 1; done
  echo "[corfu] ERROR: never came up (tail $CORFU_LOG on the log node)" >&2; return 1
}

snapshot() {
  echo "[corfu] snapshot run_batch -> load"
  corfu_sh "rm -rf $CORFU_DATA/load && cp -r $CORFU_DATA/run_batch $CORFU_DATA/load && du -sh $CORFU_DATA/load"
}

LOAD_CMD="cd \$OZONEDB_HOME && python3 bench/scripts/local/load_local_ycsb_multiproc.py --db_name ozonedb-corfu --num_writers $WRITERS"

echo "=== corfu load: server=$CORFU_SSH ($CORFU_BIND:$CORFU_PORT) load_host=$LOAD_HOST writers=$WRITERS ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] stop corfu; wipe run_batch; start corfu -l $CORFU_DATA/run_batch"
  echo "[dry-run] $LOAD_HOST: bash -lc '$LOAD_CMD'"
  echo "[dry-run] stop corfu; cp -r $CORFU_DATA/run_batch -> $CORFU_DATA/load"
  exit 0
fi

stop_corfu || true
start_fresh_corfu
echo "[load:$LOAD_HOST] $LOAD_CMD"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$LOAD_HOST" "bash -lc '$LOAD_CMD'"
stop_corfu
snapshot
echo "=== corfu load done; snapshot at $CORFU_DATA/load on $CORFU_SSH ==="
