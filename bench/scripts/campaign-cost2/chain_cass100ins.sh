#!/bin/bash
# Cassandra at 100 GB on the 2026-08-29 cluster: load, then the workload-a and workload-ai
# cells, so that the OzoneDB insert-against-update pair at 100 GB has a same-size, same-box
# twin. The old cluster's 100 GB Cassandra cells (2026-08-28) stay in the corpus.
#
# No snapshot: load_multinode_cassandra.sh ends with save-load, which copies data ->
# data.load (another ~107 GB). /tank/ssd has 169 GB free, so the pair (214 GB) does not
# fit. This chain therefore drives cassandra_ctl.sh by hand -- wipe, start, schema, load --
# and runs both cells with --no-restore. Cell order is a first (clean dataset), then ai,
# whose inserts add about 300k keys to the 100 M.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd197.utah.cloudlab.us
C0=oliverr3@amd189.utah.cloudlab.us
CTL=/tank/cassandra/cassandra_ctl.sh
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
step() { echo "[chain $(date '+%F %T')] $*"; }
RUN=bench/scripts/local/run_multinode_ycsb_with_cassandra.sh

step "guard: corfu down; free the 10 GB cassandra trees"
ssh -o BatchMode=yes "$SRV" 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 1; if pgrep -af "[C]orfuServer"; then echo "corfu still up"; exit 1; fi; echo "server clear"'
ssh -o BatchMode=yes "$SRV" "$CTL stop >/dev/null 2>&1 || true; rm -rf /tank/cassandra/data.load /tank/cassandra/data.load-10g; df -h /tank/ssd | tail -1"

step "wipe, start, schema (rf 1, 10 fields, keyspace ycsb)"
ssh -o BatchMode=yes "$SRV" "$CTL wipe && $CTL start && $CTL wait && $CTL schema --rf 1 --fields 10 --keyspace ycsb"

step "load 100 M records, 8 writer processes on the load host (about 50 min)"
ssh -o BatchMode=yes "$C0" "bash -lc 'python3 \$OZONEDB_HOME/bench/scripts/local/load_local_ycsb_multiproc.py --db_name cassandra --num_writers 8 --cassandra_consistency quorum --record_cnt 100000000' 2>&1 | tail -5"
ssh -o BatchMode=yes "$SRV" "$CTL space; df -h /tank/ssd | tail -1"

step "cell 1: quorum workload a, 120 s, no restore (tag $TAG-cass-100g-new)"
bash $RUN --consistency quorum --record-cnt 100000000 --no-restore --workloads a --writers-list 1 \
  --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-100g-new
step "cell 2: quorum workload ai, 120 s, no restore (same tag)"
bash $RUN --consistency quorum --record-cnt 100000000 --no-restore --workloads ai --writers-list 1 \
  --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-100g-new

step "stop cassandra, report space"
ssh -o BatchMode=yes "$SRV" "$CTL stop || true; df -h /tank/ssd | tail -1"
step "CHAIN-DONE"
