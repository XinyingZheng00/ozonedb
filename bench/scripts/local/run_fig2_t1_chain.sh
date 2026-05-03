#!/usr/bin/env bash
# Wait for the in-flight Fig 2 multi-writer sweep (PID passed as $1) to exit,
# then launch a follow-up sweep with --threads 1 for every (system, workload)
# pair so the scaling lines have a single-thread baseline.
#
# Designed to survive SSH disconnect: invoked under setsid + nohup, ignores
# stdin, redirects all output to a dedicated log.

set -uo pipefail

WAIT_PID="${1:?usage: $0 <pid_of_running_sweep>}"
# T=1 baseline data goes into the fig1 dir (paper Fig 1 — single-writer).
LOG_DIR="/users/Xinying/ozonedb/bench/results/local/fig1-single-writer-local"
mkdir -p "$LOG_DIR"
CHAIN_LOG="$LOG_DIR/sweep_t1.log"

echo "[chain] $(date -u +%FT%TZ) waiting for pid $WAIT_PID ..." >> "$CHAIN_LOG"
while kill -0 "$WAIT_PID" 2>/dev/null; do
    sleep 30
done
echo "[chain] $(date -u +%FT%TZ) pid $WAIT_PID exited; launching T=1 sweep" >> "$CHAIN_LOG"

# Brief pause to let the OS settle (drop_caches, etc.).
sleep 10

OZONEDB_HOME=/users/Xinying/ozonedb \
exec python3 /users/Xinying/ozonedb/bench/scripts/local/run_fig2_sweep.py \
    --systems ozonedb,trunkcpp,bcw2,hctree,rocksdb \
    --workloads a,b,c,d,f \
    --threads 1 \
    --repeats 1 \
    --duration 120 \
    --results-dir "$LOG_DIR" \
    >> "$CHAIN_LOG" 2>&1
