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
LOG_TRIM=0      # --log-trim: writer 0 checkpoints + trims during the load
CACHE_WARM=0    # --cache-warm: forward --cache-warm to the loader (label -warm)
RECORD_CNT=""   # --record-cnt N: dataset size (default: local.load.record_cnt)
CORFU_CLIENT="" # --corfu-client jni|native: the Corfu client of the loader
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH     ycsb.yaml (default: $CONFIG)
  --writers N       loader processes on the load host (default: $WRITERS)
  --load-host HOST  client that runs the loader (default: first cloudlab.hosts)
  --log-trim        forward --log-trim to the loader: writer 0 checkpoints
                    the stream to the bucket and trims behind it, so the
                    snapshot holds one trim interval, not the whole load
                    (bench/PLAN-cost.md phase 1). The checkpoint lives in the
                    bucket: snapshot the bucket together with /mnt/corfu/load.
  --cache-warm      forward --cache-warm to the loader: every writer warms
                    the outputs of an applied COMPACT into its block cache
                    (bench/PLAN-compaction-cache.md). Result label gets -warm.
  --record-cnt N    dataset size in records, forwarded as --record_cnt
  --corfu-client C  forward --corfu-client jni|native to the loader (the
                    C++ client "native" is the default, PLAN-native-corfu.md)
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
  --log-trim) LOG_TRIM=1; shift ;;
  --cache-warm) CACHE_WARM=1; shift ;;
  --record-cnt) RECORD_CNT="$2"; shift 2 ;;
  --corfu-client) CORFU_CLIENT="$2"; shift 2 ;;
  --corfu-dir) CORFU_DIR="$2"; shift 2 ;;
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
  # [C]orfuServer: the pattern must not match the remote shell that runs this
  # pkill, or the shell is SIGKILLed with the server and ssh exits 255.
  corfu_sh "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' 2>/dev/null || true; command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null || true"
  local i
  for i in $(seq 1 30); do port_open || { echo "[corfu] down"; return 0; }; sleep 1; done
  echo "[corfu] ERROR: still reachable after SIGKILL" >&2; return 1
}

# The SSTable bucket and the log dir are one unit (PLAN-trimming.md §5): the
# log references SSTables by name, and with --log-trim the checkpoint in the
# bucket must match the trim mark in the log. So the load starts from an EMPTY
# bucket, and the snapshot copies the bucket to /mnt/corfu/load-bucket next
# to /mnt/corfu/load. run_multinode_ycsb_with_corfu.sh restores both before
# every cell. Ported from the cas branch (commit 5cacc48b).
S3_BUCKET="$(cfg --get s3.bucket 2>/dev/null || echo ozonedb-ycsb)"
MC_ALIAS="${MC_ALIAS:-ozonedb-local}"
BUCKET_SNAPSHOT="$CORFU_DATA/load-bucket"

start_fresh_corfu() {
  echo "[corfu] start FRESH (empty run_batch, empty bucket $S3_BUCKET) on $CORFU_BIND:$CORFU_PORT"
  corfu_sh "mc rm -r --force --quiet $MC_ALIAS/$S3_BUCKET >/dev/null 2>&1 || true; mc ls $MC_ALIAS/$S3_BUCKET >/dev/null 2>&1 || mc mb $MC_ALIAS/$S3_BUCKET; echo \"[bucket] \$(mc du $MC_ALIAS/$S3_BUCKET | tail -1)\""
  corfu_sh "cd $CORFU_DIR && rm -rf $CORFU_DATA/run_batch && mkdir -p $CORFU_DATA/run_batch && ( setsid nohup env CORFUDB_HEAP=$CORFU_HEAP JAVA_ARGS=-Xmx120g ./bin/corfu_server -l $CORFU_DATA/run_batch -s -a $CORFU_BIND $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
  local i
  for i in $(seq 1 120); do port_open && { echo "[corfu] up"; sleep 10; return 0; }; sleep 1; done
  echo "[corfu] ERROR: never came up (tail $CORFU_LOG on the log node)" >&2; return 1
}

snapshot() {
  echo "[corfu] snapshot run_batch -> load, bucket -> $BUCKET_SNAPSHOT"
  corfu_sh "rm -rf $CORFU_DATA/load && cp -r $CORFU_DATA/run_batch $CORFU_DATA/load && du -sh $CORFU_DATA/load"
  corfu_sh "rm -rf $BUCKET_SNAPSHOT && mkdir -p $BUCKET_SNAPSHOT && mc mirror --overwrite --quiet $MC_ALIAS/$S3_BUCKET $BUCKET_SNAPSHOT >/dev/null && echo \"[bucket] snapshot: \$(du -sh $BUCKET_SNAPSHOT | cut -f1) (\$(find $BUCKET_SNAPSHOT -type f | wc -l) objects); checkpoint/: \$(du -sh $BUCKET_SNAPSHOT/*/checkpoint 2>/dev/null | cut -f1 || echo none)\""
}

# Server sample around the load (bench/PLAN-cost.md phase 1: write
# amplification, checkpoint bytes, log bytes). Same sampler and cell naming
# as run_multinode_ycsb.py; the files land in bench/results/local/_server/,
# next to the per-writer insert files, where extract_cost_coefficients.py
# joins them as the "load" workload. Best effort: a failure here never
# stops the load.
SAMPLER_SRC="$OZONEDB_HOME/bench/scripts/server_sampler.sh"
SAMPLER_REMOTE_DIR="ozonedb_server_samples/load"
S3_PORT="$(cfg --get s3.port 2>/dev/null || echo 9000)"
S3_BUCKET="$(cfg --get s3.bucket 2>/dev/null || echo "")"
# The record count is part of the cell name: both dataset sizes of a campaign
# load into the same results root, and an untagged name lets the second load
# overwrite the first load's server sample (extract_cost_coefficients.py SAMPLE_RE).
SAMPLE_RC="${RECORD_CNT:-$(cd "$OZONEDB_HOME" && python3 bench/scripts/ycsb_config.py --get local.load.record_cnt | tr -d "[] " | cut -d, -f1)}"
# The label must match result_label() in load_local_ycsb_multiproc.py for the
# flags this wrapper forwards, or the extractor pairs the sample with the
# wrong load rows (the first --cache-warm load overwrote the plain load's
# sample under the plain name).
SAMPLE_LABEL="ozonedb-corfu"
[[ "$CACHE_WARM" -eq 1 ]] && SAMPLE_LABEL="${SAMPLE_LABEL}-warm"
SAMPLE_CELL="${SAMPLE_LABEL}_workloadload_w${WRITERS}_rc${SAMPLE_RC}_trial0"
SAMPLE_LOCAL="$OZONEDB_HOME/bench/results/local/_server/$(echo "$CORFU_SSH" | sed 's/^.*@//; s/[^A-Za-z0-9._-]/_/g')"
sampler_start() {
  scp "${SSH_OPTS[@]}" -q "$SAMPLER_SRC" "$CORFU_SSH:ozonedb_server_sampler.sh" &&
    corfu_sh "mkdir -p $SAMPLER_REMOTE_DIR && env SAMPLER_MINIO_URL=http://127.0.0.1:$S3_PORT SAMPLER_BUCKET=$S3_BUCKET bash ozonedb_server_sampler.sh start $SAMPLER_REMOTE_DIR $SAMPLE_CELL" ||
    echo "[sampler] WARNING: server sample not started"
}
sampler_stop() {
  corfu_sh "env SAMPLER_MINIO_URL=http://127.0.0.1:$S3_PORT SAMPLER_BUCKET=$S3_BUCKET bash ozonedb_server_sampler.sh stop $SAMPLER_REMOTE_DIR $SAMPLE_CELL" || true
  mkdir -p "$SAMPLE_LOCAL"
  scp "${SSH_OPTS[@]}" -q "$CORFU_SSH:$SAMPLER_REMOTE_DIR/$SAMPLE_CELL.*" "$SAMPLE_LOCAL/" &&
    echo "[sampler] server sample pulled into $SAMPLE_LOCAL" ||
    echo "[sampler] WARNING: server sample not pulled"
}

LOAD_CMD="cd \$OZONEDB_HOME && python3 bench/scripts/local/load_local_ycsb_multiproc.py --db_name ozonedb-corfu --num_writers $WRITERS"
[[ "$LOG_TRIM" -eq 1 ]] && LOAD_CMD="$LOAD_CMD --log-trim"
[[ "$CACHE_WARM" -eq 1 ]] && LOAD_CMD="$LOAD_CMD --cache-warm"
[[ -n "$RECORD_CNT" ]] && LOAD_CMD="$LOAD_CMD --record_cnt $RECORD_CNT"
[[ -n "$CORFU_CLIENT" ]] && LOAD_CMD="$LOAD_CMD --corfu-client $CORFU_CLIENT"

echo "=== corfu load: server=$CORFU_SSH ($CORFU_BIND:$CORFU_PORT) load_host=$LOAD_HOST writers=$WRITERS log_trim=$LOG_TRIM cache_warm=$CACHE_WARM record_cnt=${RECORD_CNT:-yaml} corfu_client=${CORFU_CLIENT:-yaml} ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] stop corfu; wipe run_batch; empty bucket $MC_ALIAS/$S3_BUCKET; start corfu -l $CORFU_DATA/run_batch"
  echo "[dry-run] server sample start: cell=$SAMPLE_CELL on $CORFU_SSH -> $SAMPLE_LOCAL"
  echo "[dry-run] $LOAD_HOST: bash -lc '$LOAD_CMD'"
  echo "[dry-run] server sample stop"
  echo "[dry-run] stop corfu; cp -r $CORFU_DATA/run_batch -> $CORFU_DATA/load; mc mirror bucket -> $BUCKET_SNAPSHOT"
  exit 0
fi

stop_corfu || true
start_fresh_corfu
sampler_start
echo "[load:$LOAD_HOST] $LOAD_CMD"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$LOAD_HOST" "bash -lc '$LOAD_CMD'"
sampler_stop
stop_corfu
snapshot
echo "=== corfu load done; snapshot at $CORFU_DATA/load on $CORFU_SSH ==="
