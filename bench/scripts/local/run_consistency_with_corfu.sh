#!/usr/bin/env bash
#
# Laptop-side wrapper for the consistency experiments: restart corfu_server
# on an EMPTY log dir (REQUIRED -- consistency clients open fresh streams,
# and a fresh-stream openDB fences on the GLOBAL sequencer tail
# (CorfuBridge.tailAddress is runtime-wide, not per-stream) while the tailer
# only advances past entries of its OWN stream; any foreign-stream data on
# the server therefore makes that fence unsatisfiable and openDB hangs.
# Copying /mnt/corfu/load into the served dir, as run_ycsb_with_corfu.sh
# does for the benchmark stream, broke every consistency run the moment a
# real YCSB load populated /mnt/corfu/load -- hence the dedicated
# /mnt/corfu/run_consistency dir, recreated empty per run), then run one
# experiment on the first client node and pull its results back here.
#
#   bash bench/scripts/local/run_consistency_with_corfu.sh probe-staleness --write-rate 20 --duration 30
#   bash bench/scripts/local/run_consistency_with_corfu.sh check-lost-updates --workers 2 --increments 2000
#
# Node addresses come from bench/scripts/config/ycsb.yaml via ycsb_config.py,
# exactly like run_ycsb_with_corfu.sh. The corfu restart is driven from THIS
# machine because the client nodes cannot ssh to the log node.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cfg() { python3 "$CONFIG_DIR/ycsb_config.py" "$@"; }

BIND_HOST="$(cfg --node log --field lan)"
LOG_SSH="$(cfg --node log --field ssh)"
CLIENT_SSH="$(cfg --get cloudlab.hosts | python3 -c 'import ast,sys; print(ast.literal_eval(sys.stdin.read())[0])')"
SSH_USER="$(cfg --get nodes.ssh_user)"
SSH_KEY="$(cfg --get nodes.ssh_private_key_path)"
SSH_KEY="${SSH_KEY/#\~/$HOME}"
CORFU_PORT="${CORFU_PORT:-9090}"
CORFU_HEAP="${CORFU_HEAP:-100000}"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$SSH_KEY")

echo "[consistency] restarting corfu on $LOG_SSH (bind $BIND_HOST:$CORFU_PORT)"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$LOG_SSH" "
  fuser -k -9 $CORFU_PORT/tcp 2>/dev/null; sleep 2
  rm -rf /mnt/corfu/run_consistency && mkdir -p /mnt/corfu/run_consistency
  cd \$HOME/CorfuDB && ( setsid nohup env CORFUDB_HEAP=$CORFU_HEAP \
    ./bin/corfu_server -l /mnt/corfu/run_consistency -s -a $BIND_HOST $CORFU_PORT \
    </dev/null >/tmp/corfu_server.log 2>&1 & )
  for i in \$(seq 1 15); do sleep 4; nc -z -w 2 $BIND_HOST $CORFU_PORT && exit 0; done
  echo 'corfu did not come up' >&2; tail -5 /tmp/corfu_server.log >&2; exit 1
"

echo "[consistency] running on $CLIENT_SSH: consistency.py $*"
ssh "${SSH_OPTS[@]}" "$SSH_USER@$CLIENT_SSH" \
  "bash -lc 'cd \$OZONEDB_HOME && python3 bench/scripts/consistency.py $*'"

echo "[consistency] pulling results"
mkdir -p "$CONFIG_DIR/../results/consistency"
rsync -a -e "ssh ${SSH_OPTS[*]}" \
  "$SSH_USER@$CLIENT_SSH:ozonedb/bench/results/consistency/" \
  "$CONFIG_DIR/../results/consistency/"
echo "[consistency] results in bench/results/consistency/"
