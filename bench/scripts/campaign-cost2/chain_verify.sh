#!/bin/bash
# New experiment 2026-08-29: PLAN-cost-2 Task 3 step 3 (storage tests on native against a
# fresh Corfu) and the Cassandra 10 GB load + two quorum control cells on the new box.
# Runs detached on the laptop after chain_boot.sh ends with CHAIN-DONE.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
SRV=oliverr3@amd197.utah.cloudlab.us
C0=oliverr3@amd189.utah.cloudlab.us
HOSTS=$(python3 bench/scripts/ycsb_config.py --list clients --field ssh | paste -s -d, -)
step() { echo "[chain $(date '+%F %T')] $*"; }
ssh_srv() { ssh -o BatchMode=yes "$SRV" "$@"; }
ssh_c0() { ssh -o BatchMode=yes "$C0" "$@"; }

step "guard: nothing on the server"
ssh_srv 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 1; if pgrep -af "[C]assandraDaemon|[C]orfuServer"; then echo "server busy"; exit 1; fi; echo "server clear"'

step "Task 3: build runUnitTests on amd189"
ssh_c0 "bash -lc 'cd ~/ozonedb && cmake --build build --target runUnitTests corfu_native_probe -j\$(nproc) 2>&1 | tail -3'"

step "Task 3: fresh empty Corfu on amd197 (/mnt/corfu/test)"
ssh_srv 'cd ~/CorfuDB && rm -rf /mnt/corfu/test && mkdir -p /mnt/corfu/test && ( setsid nohup env CORFUDB_HEAP=122880 ./bin/corfu_server -l /mnt/corfu/test -s -a 10.10.1.1 9090 </dev/null >/tmp/corfu_test.log 2>&1 & ); for i in $(seq 1 60); do ss -ltn | grep -q ":9090 " && break; sleep 1; done; ss -ltn | grep ":9090 " || { tail -20 /tmp/corfu_test.log; exit 1; }'

step "Task 3: CorfuStorageTest + DiskCacheStorageTest + FrequencySketch on native"
ssh_c0 "bash -lc 'cd ~/ozonedb/build && CORFU_TEST_CLIENT=native CORFU_TEST_ENDPOINT=10.10.1.1:9090 CORFU_BRIDGE_JAR=\$HOME/ozonedb/ozonedb-jni-maven/corfu-bridge/target/corfu-bridge-1.0-all.jar ./runUnitTests --gtest_filter=\"CorfuStorageTest.*:DiskCacheStorageTest.*:FrequencySketch*\" 2>&1 | grep -E \"^\[  (PASSED|FAILED)|tests? from|SKIPPED|FAILED TEST\" | tail -8'"

step "Task 3: stop the test Corfu, drop its log dir"
ssh_srv 'pkill -KILL -f "org.corfudb.infrastructure.[C]orfuServer" || true; sleep 2; rm -rf /mnt/corfu/test; ls -la /mnt/corfu/'

step "Cassandra: 10 GB load, 8 writers on the load host"
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8 --record-cnt 10000000
ssh_srv '/tank/cassandra/cassandra_ctl.sh space; rm -rf /tank/cassandra/data.load-10g; cp -a /tank/cassandra/data.load /tank/cassandra/data.load-10g; du -sh /tank/cassandra/data.load-10g; df -h /tank/ssd | tail -1'

step "Cassandra: control cells quorum a c, 120 s, tag $TAG-cass-10g-rerun (new box vs amd127)"
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency quorum --record-cnt 10000000 \
  --workloads "a c" --writers-list 1 --client-hosts "$HOSTS" --trial 1 --duration 120 --run-tag $TAG-cass-10g-rerun
ssh_srv '/tank/cassandra/cassandra_ctl.sh stop || true; df -h /tank/ssd | tail -1'
step "CHAIN-DONE"
