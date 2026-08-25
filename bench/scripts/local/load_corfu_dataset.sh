#!/usr/bin/env bash
set -euo pipefail
# Load the YCSB dataset once into the shared log so every later cell of a
# sweep can start from it. Produces, on the corfu node:
#   /mnt/corfu/load          the Corfu log after the load (run_multinode_ycsb_with_corfu.sh
#                            copies it to /mnt/corfu/run_batch before every cell)
#   /mnt/corfu/load-bucket   a snapshot of the SSTable bucket at the same moment
#                            (the wrapper mirrors it back before every cell; a run's
#                            compaction REMOVEs input SSTables the restored log
#                            still references, so log and bucket must be restored
#                            as a pair)
#
# Steps, all driven from this machine over ssh:
#   1. stop any corfu_server on the corfu node, wipe /mnt/corfu/load, empty the bucket
#   2. start corfu_server -l /mnt/corfu/load
#   3. run load_local_ycsb_multiproc.py --num_writers N on ONE client host
#      (record_cnt / db_name / key_size come from that host's ycsb.yaml)
#   4. stop corfu_server, snapshot the bucket to /mnt/corfu/load-bucket
#
# The load writes in batches: each writer caches puts and appends ONE Corfu
# entry per --batch-bytes (default 256 KiB, ~256 records) or per
# --commit-interval-ms, whichever comes first, with acks before sequencing
# (fine for a load, never for a run). Every replica replays the loaded log
# on openDB at ~28 us per entry, so entry count is what the run phase pays:
# 1 M one-record entries replay in 30 s, ~4 k batched ones in about a second.
# --batch-bytes 0 restores one entry per put.
#
# Usage:
#   bash bench/scripts/local/load_corfu_dataset.sh [--num-writers N] [--client-host HOST]
#                                                   [--batch-bytes B] [--commit-interval-ms MS]
#                                                   [--config ycsb.yaml] [--corfu-dir ~/CorfuDB]
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME
CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"
NUM_WRITERS=8
CLIENT_HOST=""
BATCH_BYTES=262144
COMMIT_MS=1000
CORFU_DIR="~/CorfuDB"
CORFU_LOG="/tmp/corfu_server.log"

while [[ $# -gt 0 ]]; do
  case "$1" in
  --num-writers) NUM_WRITERS="$2"; shift 2 ;;
  --client-host) CLIENT_HOST="$2"; shift 2 ;;
  --batch-bytes) BATCH_BYTES="$2"; shift 2 ;;
  --commit-interval-ms) COMMIT_MS="$2"; shift 2 ;;
  --config)      CONFIG="$2"; shift 2 ;;
  --corfu-dir)   CORFU_DIR="$2"; shift 2 ;;
  -h | --help)   sed -n 2,31p "$0"; exit 0 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

cfg() { python3 "$OZONEDB_HOME/bench/scripts/ycsb_config.py" --config "$CONFIG" "$@"; }
CORFU_BIND_HOST="$(cfg --node log --field lan)"
CORFU_SSH_HOST="$(cfg --node log --field ssh)"
SSH_USER="$(cfg --get nodes.ssh_user)"
SSH_KEY="$(cfg --get nodes.ssh_private_key_path)"; SSH_KEY="${SSH_KEY/#\~/$HOME}"
CORFU_PORT="$(cfg --get corfu.port)"
S3_BUCKET="$(cfg --get s3.bucket 2>/dev/null || echo ozonedb-ycsb)"
[[ -n "$CLIENT_HOST" ]] || CLIENT_HOST="$(cfg --get cloudlab.hosts | python3 -c 'import json,sys; print(json.load(sys.stdin)[0])')"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$SSH_KEY")
corfu_sh()  { ssh "${SSH_OPTS[@]}" "$SSH_USER@$CORFU_SSH_HOST" bash -lc "$(printf '%q' "$1")"; }
client_sh() { ssh "${SSH_OPTS[@]}" "$SSH_USER@$CLIENT_HOST" bash -lc "$(printf '%q' "$1")"; }

LOADER_FLAGS="--num_writers $NUM_WRITERS"
if [[ "$BATCH_BYTES" -gt 0 ]]; then
  LOADER_FLAGS="$LOADER_FLAGS --corfu-batch-bytes $BATCH_BYTES --corfu-commit-interval-ms $COMMIT_MS"
fi
echo "[load] corfu=$CORFU_SSH_HOST (bind $CORFU_BIND_HOST:$CORFU_PORT) client=$CLIENT_HOST writers=$NUM_WRITERS bucket=$S3_BUCKET batch=${BATCH_BYTES}B/${COMMIT_MS}ms"

echo "[load] 1/4 stop corfu, wipe /mnt/corfu/load, empty the bucket"
corfu_sh "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' 2>/dev/null || true; (command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null) || true; sleep 2; rm -rf /mnt/corfu/load /mnt/corfu/load-bucket && mkdir -p /mnt/corfu/load; mc rm -r --force --quiet ozonedb-local/$S3_BUCKET >/dev/null 2>&1 || true; mc ls ozonedb-local/$S3_BUCKET >/dev/null 2>&1 || mc mb ozonedb-local/$S3_BUCKET"

echo "[load] 2/4 start corfu -l /mnt/corfu/load"
corfu_sh "cd $CORFU_DIR && ( setsid nohup env CORFUDB_HEAP=122880 ./bin/corfu_server -l /mnt/corfu/load -s -a $CORFU_BIND_HOST $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
for i in $(seq 1 90); do
  if corfu_sh "(echo > /dev/tcp/$CORFU_BIND_HOST/$CORFU_PORT) 2>/dev/null"; then echo "[load] corfu up after ${i}s"; break; fi
  sleep 1
  [[ $i -lt 90 ]] || { echo "[load] ERROR: corfu never came up (see $CORFU_LOG on $CORFU_SSH_HOST)" >&2; exit 1; }
done
sleep 3

echo "[load] 3/4 loader on $CLIENT_HOST"
t0=$(date +%s)
client_sh "cd \$OZONEDB_HOME/bench/scripts/local && python3 load_local_ycsb_multiproc.py $LOADER_FLAGS 2>&1 | tail -25"
echo "[load] loader finished in $(( $(date +%s) - t0 )) s"

echo "[load] 4/4 stop corfu, snapshot the bucket"
corfu_sh "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' 2>/dev/null || true; (command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null) || true; sleep 2; mkdir -p /mnt/corfu/load-bucket && mc mirror --overwrite --quiet ozonedb-local/$S3_BUCKET /mnt/corfu/load-bucket >/dev/null; echo \"log: \$(du -sh /mnt/corfu/load | cut -f1)  bucket snapshot: \$(du -sh /mnt/corfu/load-bucket | cut -f1) (\$(find /mnt/corfu/load-bucket -type f | wc -l) objects)\""
echo "[load] done"
