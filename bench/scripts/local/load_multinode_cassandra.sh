#!/usr/bin/env bash
set -euo pipefail

# Load the YCSB dataset into the Cassandra baseline, then snapshot it.
#
# The Cassandra analogue of populating /mnt/corfu/load: afterwards every
# run_multinode_ycsb_with_cassandra.sh trial restores this snapshot
# (cassandra_ctl.sh restore-load) so trials start from identical state.
#
# Steps (all servers = cassandra.ssh_hosts from ycsb.yaml):
#   1. push bench/scripts/cassandra_ctl.sh to every server (scp)
#   2. wipe every server (stop + empty data tree), start them, wait for CQL
#   3. create the keyspace/table on the first server
#      (rf = cassandra.replication_factor, columns = cassandra.fields)
#   4. run load_local_ycsb_multiproc.py --db_name cassandra on ONE client
#      host with --writers parallel processes (keys are partitioned per
#      process; local.load.record_cnt / key_size come from that client's
#      ycsb.yaml, like every other load)
#   5. save-load on every server (drain, stop, copy data -> data.load)
#   6. start the servers again and leave them running
#
# Check that the cluster is free first: this wipes the Cassandra data tree
# and runs a long JVM on a client. It never touches Corfu or MinIO, but it
# shares the box with them by default.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"
CTL_SRC="${OZONEDB_HOME}/bench/scripts/cassandra_ctl.sh"
CQL_SRC="${OZONEDB_HOME}/bench/scripts/cassandra_cql.py"

WRITERS=8
LOAD_HOST=""   # default: first cloudlab.hosts entry
CONSISTENCY="" # default: cassandra.consistency from the client's ycsb.yaml
RECORD_CNT=""  # --record-cnt N: dataset size (default: local.load.record_cnt)
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH        ycsb.yaml (default: $CONFIG)
  --writers N          parallel loader processes on the load host (default: $WRITERS)
  --load-host HOST     client that runs the loader (default: first cloudlab.hosts entry)
  --consistency MODE   one | quorum | serial for the load itself
                       (default: the client's cassandra.consistency; quorum is
                       the sensible choice -- the dataset is the same either way)
  --record-cnt N       dataset size in records, forwarded as --record_cnt
  --dry-run            print the plan; no ssh
  -h | --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --config) CONFIG="$2"; shift 2 ;;
  --writers) WRITERS="$2"; shift 2 ;;
  --load-host) LOAD_HOST="$2"; shift 2 ;;
  --consistency) CONSISTENCY="$2"; shift 2 ;;
  --record-cnt) RECORD_CNT="$2"; shift 2 ;;
  --dry-run) DRY_RUN=1; shift ;;
  -h | --help) usage; exit 0 ;;
  *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done
[[ "$WRITERS" =~ ^[1-9][0-9]*$ ]] || { echo "--writers must be a positive integer" >&2; exit 1; }
if [[ -n "$RECORD_CNT" ]] && ! [[ "$RECORD_CNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "--record-cnt must be a positive integer (got: $RECORD_CNT)" >&2
  exit 1
fi
if [[ -n "$CONSISTENCY" ]] && ! [[ "$CONSISTENCY" =~ ^(one|quorum|serial)$ ]]; then
  echo "--consistency must be one|quorum|serial (got: $CONSISTENCY)" >&2
  exit 1
fi

cfg() { python3 "$OZONEDB_HOME/bench/scripts/ycsb_config.py" --config "$CONFIG" "$@"; }
SSH_USER="$(cfg --get nodes.ssh_user)"
SSH_KEY="$(cfg --get nodes.ssh_private_key_path)"
SSH_KEY="${SSH_KEY/#\~/$HOME}"
INSTALL_DIR="$(cfg --get cassandra.install_dir)"
KEYSPACE="$(cfg --get cassandra.keyspace)"
RF="$(cfg --get cassandra.replication_factor)"
FIELDS="$(cfg --get cassandra.fields)"
read -r -a SERVERS <<<"$(cfg --list cassandra --field ssh | tr '\n' ' ')"
[[ -n "$LOAD_HOST" ]] || LOAD_HOST="$(cfg --get cloudlab.hosts | python3 -c 'import json,sys; print(json.load(sys.stdin)[0])')"
[[ ${#SERVERS[@]} -gt 0 ]] || { echo "no cassandra servers resolved from $CONFIG" >&2; exit 1; }

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

LOAD_CMD="python3 \$OZONEDB_HOME/bench/scripts/local/load_local_ycsb_multiproc.py --db_name cassandra --num_writers $WRITERS"
[[ -n "$CONSISTENCY" ]] && LOAD_CMD="$LOAD_CMD --cassandra_consistency $CONSISTENCY"
[[ -n "$RECORD_CNT" ]] && LOAD_CMD="$LOAD_CMD --record_cnt $RECORD_CNT"

# Server sample around the load on the first server (bench/PLAN-cost.md
# phase 1: server CPU and bytes on disk during the load). Same sampler and
# cell naming as run_multinode_ycsb.py; the files land in
# bench/results/local/_server/ next to the per-writer insert files, where
# extract_cost_coefficients.py joins them as the "load" workload. Best effort.
SAMPLER_SRC="$OZONEDB_HOME/bench/scripts/server_sampler.sh"
SAMPLER_REMOTE_DIR="ozonedb_server_samples/load"
LOAD_MODE="${CONSISTENCY:-$(cfg --get cassandra.consistency 2>/dev/null || echo quorum)}"
# Record count in the cell name, so a second dataset size never overwrites the
# first load's server sample (see load_corfu_dataset.sh).
SAMPLE_RC="${RECORD_CNT:-$(cd "$OZONEDB_HOME" && python3 bench/scripts/ycsb_config.py --get local.load.record_cnt | tr -d "[] " | cut -d, -f1)}"
SAMPLE_CELL="cassandra-${LOAD_MODE}_workloadload_w${WRITERS}_rc${SAMPLE_RC}_trial0"
SAMPLE_HOST="${SERVERS[0]}"
SAMPLE_LOCAL="$OZONEDB_HOME/bench/results/local/_server/$(echo "$SAMPLE_HOST" | sed 's/[^A-Za-z0-9._-]/_/g')"
sampler_start() {
  scp "${SSH_OPTS[@]}" -q "$SAMPLER_SRC" "$SSH_USER@$SAMPLE_HOST:ozonedb_server_sampler.sh" &&
    rsh "$SAMPLE_HOST" "mkdir -p $SAMPLER_REMOTE_DIR && bash ozonedb_server_sampler.sh start $SAMPLER_REMOTE_DIR $SAMPLE_CELL" ||
    echo "[sampler] WARNING: server sample not started"
}
sampler_stop() {
  rsh "$SAMPLE_HOST" "bash ozonedb_server_sampler.sh stop $SAMPLER_REMOTE_DIR $SAMPLE_CELL" || true
  mkdir -p "$SAMPLE_LOCAL"
  scp "${SSH_OPTS[@]}" -q "$SSH_USER@$SAMPLE_HOST:$SAMPLER_REMOTE_DIR/$SAMPLE_CELL.*" "$SAMPLE_LOCAL/" &&
    echo "[sampler] server sample pulled into $SAMPLE_LOCAL" ||
    echo "[sampler] WARNING: server sample not pulled"
}

echo "=== cassandra load: servers=(${SERVERS[*]}) load_host=$LOAD_HOST writers=$WRITERS keyspace=$KEYSPACE rf=$RF fields=$FIELDS record_cnt=${RECORD_CNT:-yaml} ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] scp $CTL_SRC -> each server:$CTL"
  echo "[dry-run] each server: $CTL wipe && $CTL start && $CTL wait"
  echo "[dry-run] ${SERVERS[0]}: $CTL schema --rf $RF --fields $FIELDS --keyspace $KEYSPACE"
  echo "[dry-run] server sample start: cell=$SAMPLE_CELL on $SAMPLE_HOST -> $SAMPLE_LOCAL"
  echo "[dry-run] $LOAD_HOST: bash -lc '$LOAD_CMD'"
  echo "[dry-run] server sample stop"
  echo "[dry-run] each server: $CTL save-load && $CTL start && $CTL wait"
  exit 0
fi

for s in "${SERVERS[@]}"; do push_ctl "$s"; done
ctl_all wipe
ctl_all start
ctl_all wait
echo "[cassandra:${SERVERS[0]}] $CTL schema --rf $RF --fields $FIELDS --keyspace $KEYSPACE"
rsh "${SERVERS[0]}" "$CTL schema --rf $RF --fields $FIELDS --keyspace $KEYSPACE"

sampler_start
echo "[load:$LOAD_HOST] $LOAD_CMD"
# bash -lc: ~/.ozonedb.env is wired into .profile; a plain ssh command shell
# would not see OZONEDB_HOME / JAVA_HOME (see run_multinode_ycsb.py).
rsh "$LOAD_HOST" "bash -lc '$LOAD_CMD'"
sampler_stop

ctl_all save-load
ctl_all start
ctl_all wait
echo "=== cassandra load done; snapshot at $INSTALL_DIR/data.load on each server ==="
