#!/usr/bin/env bash
#
# Cross-node visibility experiment (bench/PLAN-visibility.md phase 2):
# writer on clients[0], reader on clients[1], ack notifications over TCP
# (reader listens on its LAN address; the writer's connect retries until the
# reader is up, so the handshake is the start barrier). The miss count is
# clock-free -- every first get provably starts after the write's ack via
# the socket -- and latency is referenced to the reader-side notify receipt,
# since the two hosts' CLOCK_MONOTONIC domains are incomparable.
#
#   bash run_visibility_cross_node.sh [--linearizable] [--rate 20]
#        [--duration 15] [--value-size 1000] [--poll-ms 1] [--key-timeout-s 30]
#
# Laptop-driven like run_consistency_with_corfu.sh (client nodes cannot ssh
# each other or the log node): restart corfu, start the reader role on
# clients[1], run the writer role on clients[0], pull BOTH nodes' output
# dirs (home dirs are NOT shared) and analyze locally.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cfg() { python3 "$CONFIG_DIR/ycsb_config.py" "$@"; }

LINEARIZABLE=""
RATE=20
DURATION=15
VALUE_SIZE=1000
POLL_MS=1
KEY_TIMEOUT_S=30
PORT="${VIS_PORT:-7911}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --linearizable) LINEARIZABLE="--linearizable"; shift ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --value-size) VALUE_SIZE="$2"; shift 2 ;;
    --poll-ms) POLL_MS="$2"; shift 2 ;;
    --key-timeout-s) KEY_TIMEOUT_S="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

BIND_HOST="$(cfg --node log --field lan)"
LOG_SSH="$(cfg --node log --field ssh)"
SSH_USER="$(cfg --get nodes.ssh_user)"
SSH_KEY="$(cfg --get nodes.ssh_private_key_path)"
SSH_KEY="${SSH_KEY/#\~/$HOME}"
CORFU_PORT="${CORFU_PORT:-9090}"
CORFU_HEAP="${CORFU_HEAP:-100000}"
W_SSH="$(cfg --get cloudlab.hosts | python3 -c 'import ast,sys; print(ast.literal_eval(sys.stdin.read())[0])')"
R_SSH="$(cfg --get cloudlab.hosts | python3 -c 'import ast,sys; print(ast.literal_eval(sys.stdin.read())[1])')"
R_LAN="$(python3 - "$CONFIG_DIR" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ycsb_config import load
c = (load().get("nodes") or {}).get("clients")[1]
print(c.get("lan") or c["ssh"] if isinstance(c, dict) else c)
PY
)"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$SSH_KEY")

STREAM_BASE="$(cfg --get corfu.stream_name)"
TAG="$(date +%Y%m%d-%H%M%S)"
OUT="bench/results/consistency/visibility-xnode-$TAG"
STREAM="$STREAM_BASE-visx-$TAG"
LOCAL_OUT="$CONFIG_DIR/../results/consistency/visibility-xnode-$TAG"
mkdir -p "$LOCAL_OUT"

echo "[visx] restarting corfu on $LOG_SSH (bind $BIND_HOST:$CORFU_PORT)"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$LOG_SSH" "
  fuser -k -9 $CORFU_PORT/tcp 2>/dev/null; sleep 2
  # Empty log dir, NOT a copy of /mnt/corfu/load: visibility clients open a
  # fresh stream, and a fresh-stream openDB fences on the GLOBAL sequencer
  # tail -- foreign-stream (YCSB load) data makes that fence unsatisfiable
  # and openDB hangs. See run_consistency_with_corfu.sh.
  rm -rf /mnt/corfu/run_consistency && mkdir -p /mnt/corfu/run_consistency
  cd \$HOME/CorfuDB && ( setsid nohup env CORFUDB_HEAP=$CORFU_HEAP \
    ./bin/corfu_server -l /mnt/corfu/run_consistency -s -a $BIND_HOST $CORFU_PORT \
    </dev/null >/tmp/corfu_server.log 2>&1 & )
  for i in \$(seq 1 15); do sleep 4; nc -z -w 2 $BIND_HOST $CORFU_PORT && exit 0; done
  echo 'corfu did not come up' >&2; tail -5 /tmp/corfu_server.log >&2; exit 1
"

READER_TIMEOUT=$((DURATION + KEY_TIMEOUT_S + 420))
echo "[visx] starting reader on $R_SSH (listen $R_LAN:$PORT)"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$R_SSH" \
  "bash -lc 'cd \$OZONEDB_HOME && timeout $READER_TIMEOUT python3 bench/scripts/consistency.py $LINEARIZABLE visibility-reader --out-dir $OUT --stream $STREAM --port $PORT --poll-ms $POLL_MS --key-timeout-s $KEY_TIMEOUT_S --run-timeout-s $((DURATION + KEY_TIMEOUT_S + 120))'" \
  > "$LOCAL_OUT/reader-ssh.log" 2>&1 &
READER_PID=$!

echo "[visx] running writer on $W_SSH (connect $R_LAN:$PORT)"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$W_SSH" \
  "bash -lc 'cd \$OZONEDB_HOME && timeout $((DURATION + 420)) python3 bench/scripts/consistency.py $LINEARIZABLE visibility-writer --out-dir $OUT --stream $STREAM --connect $R_LAN:$PORT --rate $RATE --duration $DURATION --value-size $VALUE_SIZE'" \
  | tee "$LOCAL_OUT/writer-ssh.log"

echo "[visx] waiting for the reader to drain"
if ! wait "$READER_PID"; then
  echo "[visx] reader exited nonzero -- see $LOCAL_OUT/reader-ssh.log" >&2
  exit 1
fi

echo "[visx] pulling both nodes' outputs"
rsync -a -e "ssh ${SSH_OPTS[*]}" "$SSH_USER@$W_SSH:ozonedb/$OUT/" "$LOCAL_OUT/"
rsync -a -e "ssh ${SSH_OPTS[*]}" "$SSH_USER@$R_SSH:ozonedb/$OUT/" "$LOCAL_OUT/"

python3 "$CONFIG_DIR/visibility_analysis.py" --dir "$LOCAL_OUT" --cross-node \
  $LINEARIZABLE --engine ozonedb-corfu --rate "$RATE" --duration "$DURATION" \
  --value-size "$VALUE_SIZE" --poll-ms "$POLL_MS" --stream "$STREAM"
echo "[visx] results in $LOCAL_OUT"
