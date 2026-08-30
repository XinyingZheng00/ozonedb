#!/bin/bash
# PLAN-cost-2 Tasks 5, 6 (resume; Task 4 and the 0b check are done) and the 10 GB half of 7b: one 16-writer load, the h sweep,
# workload a, linearizable, scaling, the JNI control, the key-space zipfian twins, the
# tier cells. Runs detached on the laptop. set -e: a failed cell stops the chain.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd127.utah.cloudlab.us
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
hosts_n() { echo "$HOSTS" | tr ',' '\n' | head -n "$1" | paste -s -d, -; }
step() { echo "[chain $(date '+%F %T')] $*"; }
RUN=bench/scripts/local/run_multinode_ycsb_with_corfu.sh
RC=10000000
CHAINS=bench/results/local/cost2-chains

step "guard: nothing else on the server (resumed after the zipf-check column bug; load and check cell done)"
ssh -o BatchMode=yes "$SRV" '/tank/cassandra/cassandra_ctl.sh stop >/dev/null 2>&1 || true; pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 1; if pgrep -af "[C]assandraDaemon"; then echo "server busy"; exit 1; fi; echo "server clear"; ls -d /mnt/corfu/load /mnt/corfu/load-bucket; df -h /tank/ssd | tail -1'

step "Task 5: h sweep, workload c, 5 GiB and 640 MiB at 600 s"
for c in 5368709120 671088640; do
  bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes $c --workloads c --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
done
step "Task 5: h sweep, workload c, 80 MiB / 10 MiB / 1.25 MiB at 300 s"
for c in 83886080 10485760 1310720; do
  bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes $c --workloads c --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-10g
done
step "Task 5: workload a at 5 GiB and 80 MiB, 600 s"
for c in 5368709120 83886080; do
  bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes $c --workloads a --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-10g
done
step "Task 5: linearizable a c at 5 GiB, 120 s"
bash $RUN --log-trim --linearizable --record-cnt $RC --lru-cache-bytes 5368709120 --workloads "a c" --writers-list 1 \
  --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-10g
step "Task 5: server scaling, workload a on 2 and 4 hosts, 120 s"
for n in 2 4; do
  bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes 5368709120 --workloads a --writers-list 1 \
    --client-hosts "$(hosts_n $n)" --trial 1 --duration 120 --run-tag $TAG-scale
done
step "Task 5: JNI control, workload a at 5 GiB, 600 s"
bash $RUN --log-trim --corfu-client jni --record-cnt $RC --lru-cache-bytes 5368709120 --workloads a --writers-list 1 \
  --client-hosts "$HOSTS" --trial 1 --duration 600 --run-tag $TAG-jni

step "Task 7b: 10 GB key-space zipfian twins at 80 MiB and 1.25 MiB, 300 s"
for c in 83886080 1310720; do
  bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes $c --workloads cz --writers-list 1 \
    --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-10g-zipf
done

step "Task 6: tier cells behind 80 MiB RAM: 2.5 GiB c 300 s, 5 GiB a c 600 s, 20 GiB a c 600 s"
run_tier() {
  bash $RUN --log-trim --record-cnt $1 --lru-cache-bytes 83886080 --disk-cache-bytes $2 --workloads "$3" \
    --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration $5 --run-tag $4
}
run_tier $RC 2684354560  c     $TAG-10g-tier 300
run_tier $RC 5368709120  "a c" $TAG-10g-tier 600
run_tier $RC 21474836480 "a c" $TAG-10g-tier 600

step "CHAIN-DONE"
