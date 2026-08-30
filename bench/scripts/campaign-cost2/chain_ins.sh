#!/bin/bash
# One cell: workload ai (50 % read, 50 % blind insert) at 100 GB, 100 MiB RAM, 300 s,
# on the probe-counter build. Twin of the workload-a diag cell of the same tag, which
# pays a read-modify-write per update. Pushes ycsb/workloads/workloadai to the clients
# first (no rebuild: the file is data, not code). Runs detached on the laptop.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
export ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd197.utah.cloudlab.us
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
step() { echo "[chain $(date '+%F %T')] $*"; }
R=bench/results/local

step "guard: server clear"
ssh -o BatchMode=yes "$SRV" 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 1; if pgrep -af "[C]assandraDaemon|[C]orfuServer"; then echo "server busy"; exit 1; fi; echo "server clear"; du -sh /mnt/corfu/load'

step "push workloadai to the clients"
(cd bench/ansible && ansible-playbook sync.yml 2>&1 | tail -3)
(cd bench/ansible && ansible -m shell -a 'ls ~/ozonedb/ycsb/workloads/workloadai' clients 2>&1 | grep -c workloadai)

step "cell: workload ai at 100 GB, 100 MiB RAM, 300 s (tag $TAG-100g-diag)"
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --log-trim --record-cnt 100000000 \
  --lru-cache-bytes 104857600 --workloads ai --writers-list 1 --client-hosts "$HOSTS" \
  --trial 1 --duration 300 --run-tag $TAG-100g-diag

step "probe counters, writer 0"
f=$(ls $R/$TAG-100g-diag/1KB-999999999-100000000-workloadai-*lru100m_w0of8_t1_trial1.result | head -1)
awk '/\[lru_cache\] (sstable|levels)/' "$f" | cut -c1-400
awk '/\[READ\], Operations|\[INSERT\], Operations|Return=OK|Return=ERROR/' "$f" | head -6
ssh -o BatchMode=yes "$SRV" 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true'
step "CHAIN-DONE"
