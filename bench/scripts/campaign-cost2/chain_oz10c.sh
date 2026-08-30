#!/bin/bash
# PLAN-cost-2 Task 6 follow-up: the 20 GiB tier at 10 GB needs ~11 min to fill per
# writer (miss rate x 64 KiB ~ 30 MB/s), so the 600 s cells of chain_oz10b are not
# converged. Rerun a and c at 1800 s. Runs detached on the laptop.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd127.utah.cloudlab.us
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
step() { echo "[chain $(date '+%F %T')] $*"; }
RUN=bench/scripts/local/run_multinode_ycsb_with_corfu.sh
RC=10000000

step "guard: nothing else on the server"
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop >/dev/null 2>&1 || true; pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 1; if pgrep -af "[C]assandraDaemon"; then echo "server busy"; exit 1; fi; echo "server clear"'

step "Task 6 rerun: 20 GiB tier behind 80 MiB RAM, a c, 1800 s each"
bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes 83886080 --disk-cache-bytes 21474836480 --workloads "a c" \
  --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 1800 --run-tag $TAG-10g-tier
step "Task 5 follow-up: linearizable workload c at 5 GiB, 600 s (the 120 s cell closed 5 % full, its default twin 26 %)"
bash $RUN --log-trim --linearizable --record-cnt $RC --lru-cache-bytes 5368709120 --workloads c --writers-list 1 \
  --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
step "CHAIN-DONE"
