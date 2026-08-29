#!/bin/bash
# PLAN-cost-2 Task 2: Cassandra cells at 10 GB and 100 GB. Runs detached on the laptop.
set -euo pipefail
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd127.utah.cloudlab.us
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -s -d, -; }
step() { echo "[chain $(date '+%F %T')] $*"; }
RUN=bench/scripts/local/run_multinode_ycsb_with_cassandra.sh

step "corfu down on the server"
ssh -o BatchMode=yes "$SRV" "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' || true"

step "10 GB: quorum + serial, workloads a c, 8 hosts"
for mode in quorum serial; do
  bash $RUN --consistency $mode --record-cnt 10000000 --workloads "a c" --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-10g
done
step "10 GB: quorum workload a on 2 and 4 hosts (scaling)"
for n in 2 4; do
  bash $RUN --consistency quorum --record-cnt 10000000 --workloads a --writers-list 1 \
    --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG-cass-10g
done

step "keep the 10 GB snapshot as data.load-10g"
ssh -o BatchMode=yes "$SRV" 'rm -rf /tank/cassandra/data.load-10g && cp -a /tank/cassandra/data.load /tank/cassandra/data.load-10g && du -sh /tank/cassandra/data.load-10g'

step "100 GB load (8 writers on the load host)"
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8 --record-cnt 100000000
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh space; du -sk /tank/cassandra/data.load; df -h /tank/ssd | tail -1'

step "100 GB: workload c, quorum + serial, no restore"
for mode in quorum serial; do
  bash $RUN --consistency $mode --record-cnt 100000000 --no-restore --workloads c --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-100g
done
step "100 GB: workload a, quorum + serial, restore before each"
for mode in quorum serial; do
  bash $RUN --consistency $mode --record-cnt 100000000 --workloads a --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-100g
done

step "drop the 100 GB trees, put the 10 GB snapshot back as data.load"
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop || true; rm -rf /tank/cassandra/data /tank/cassandra/data.load && cp -a /tank/cassandra/data.load-10g /tank/cassandra/data.load && df -h /tank/ssd | tail -1'
step "CHAIN-DONE"
