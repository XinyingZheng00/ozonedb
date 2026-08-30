#!/bin/bash
# Diagnostic for the wasted SSTable fetches on workload a (1.35 fetches per read at
# 100 GB against 1.02 on c): one workload-a and one workload-c cell at 100 GB on the
# build with the probe counters (1662446b), then the per-level probes/pruned/absent
# lines of writer 0. Swaps the 100 GB snapshot back into /mnt/corfu/load first (the
# 10 GB one goes to load-10g). Runs detached after chain_oz10fix.sh and a
# `sync.yml -e build=true`.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd197.utah.cloudlab.us
C0=oliverr3@amd189.utah.cloudlab.us
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
step() { echo "[chain $(date '+%F %T')] $*"; }
RUN=bench/scripts/local/run_multinode_ycsb_with_corfu.sh
RC=100000000
R=bench/results/local

step "guard: server clear, counters build on the clients"
ssh -o BatchMode=yes "$SRV" 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 1; if pgrep -af "[C]assandraDaemon|[C]orfuServer"; then echo "server busy"; exit 1; fi; echo "server clear"; ls /mnt/corfu/'
ssh -o BatchMode=yes "$C0" 'grep -c noteProbe ~/ozonedb/src/db/sstable/table_reader.cpp; ls -la --time-style=+%T ~/ozonedb/build/libOzoneDB.so'

step "swap snapshots: 10 GB -> load-10g, 100 GB -> load"
ssh -o BatchMode=yes "$SRV" 'set -e; cd /mnt/corfu; rm -rf load-10g load-bucket-10g; mv load load-10g; mv load-bucket load-bucket-10g; mv load-100g load; mv load-bucket-100g load-bucket; du -sh load load-bucket'

step "diag cells at 100 GB, 100 MiB RAM: workload a 300 s, workload c 300 s (tag $TAG-100g-diag)"
bash $RUN --log-trim --record-cnt $RC --lru-cache-bytes 104857600 --workloads "a c" --writers-list 1 \
  --client-hosts "$HOSTS" --trial 1 --duration 300 --run-tag $TAG-100g-diag

step "probe counters, writer 0 of each cell"
for wl in a c; do
  f=$(ls $R/$TAG-100g-diag/1KB-999999999-${RC}-workload${wl}-*lru100m_w0of8_t1_trial1.result | head -1)
  echo "== $wl: $f"
  awk '/\[lru_cache\] (sstable|levels)/' "$f" | cut -c1-400
  awk '/\[READ\], Operations|\[READ\], Return=/' "$f"
done
ssh -o BatchMode=yes "$SRV" 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true'
step "CHAIN-DONE"
