#!/usr/bin/env bash
#
# Sample the server side of one benchmark cell, ON the server node.
#
#   server_sampler.sh start DIR CELL   snapshot -> DIR/CELL.before.txt, then
#                                      pidstat into DIR/CELL.pidstat.txt
#   server_sampler.sh stop  DIR CELL   snapshot -> DIR/CELL.after.txt, kill pidstat
#   server_sampler.sh snapshot         print one snapshot to stdout
#
# Why: the cost model of bench/PLAN-cost.md needs, per cell, the object-store
# request counts (GET, PUT, DELETE), the bytes in and out, the bytes on disk of
# every stateful directory, and the server CPU per process. MinIO's counters
# are cumulative since the process started, so a cell has a request count
# only when a "before" snapshot exists -- that is what `start` records.
#
# One snapshot is line oriented and parsed by extract_cost_coefficients.py:
#   # server_sampler host=H time=EPOCH iso=... nproc=N
#   metric <prometheus line>          MinIO /minio/v2/metrics/{cluster,bucket}
#   du <path> <kbytes>                du -sk of every SAMPLER_DIRS entry
#   mcdu <json>                       mc du --json --depth 2 of the bucket
#   pids <class> <pid[,pid]>          corfu | minio | cassandra
#   loadavg <1> <5> <15>
#
# Needs: curl, sysstat (pidstat), optionally mc with the ozonedb-local alias
# (setup.sh --role minio creates it) -- all in the server roles of setup.sh.
# MinIO answers the metrics endpoints without a token only when the unit
# carries MINIO_PROMETHEUS_AUTH_TYPE=public (setup.sh --role minio writes it).
#
# run_multinode_ycsb.py scps this file to every server it samples and calls
# start/stop around the cell, then pulls DIR back into <results>/_server/.

set -uo pipefail

MINIO_URL="${SAMPLER_MINIO_URL:-http://127.0.0.1:9000}"
DIRS="${SAMPLER_DIRS:-/mnt/corfu/run_batch /mnt/corfu/load /tank/minio /tank/cassandra/data}"
INTERVAL="${SAMPLER_INTERVAL:-10}"
MC_ALIAS="${SAMPLER_MC_ALIAS:-ozonedb-local}"
BUCKET="${SAMPLER_BUCKET:-}"

# Only the counters the model reads; the full scrape is thousands of lines.
METRIC_RE='^minio_(s3_requests_total|s3_requests_errors_total|s3_traffic_sent_bytes|s3_traffic_received_bytes|bucket_usage_total_bytes|bucket_usage_object_total|bucket_traffic_sent_bytes|bucket_traffic_received_bytes|cluster_usage_total_bytes|cluster_usage_object_total)'

log() { printf '[server_sampler] %s\n' "$*" >&2; }

pids_of() {
  case "$1" in
  corfu) pgrep -f 'org.corfudb.infrastructure.CorfuServer' 2>/dev/null ;;
  minio) pgrep -x minio 2>/dev/null ;;
  cassandra) pgrep -f 'org.apache.cassandra.service.CassandraDaemon' 2>/dev/null ;;
  esac | paste -sd, -
}

snapshot() {
  local now
  now="$(date +%s)"
  echo "# server_sampler host=$(hostname) time=$now iso=$(date -u -d "@$now" +%FT%TZ 2>/dev/null || date -u +%FT%TZ) nproc=$(nproc 2>/dev/null || echo '?')"
  local ep
  for ep in cluster bucket; do
    if out="$(curl -sf --max-time 5 "$MINIO_URL/minio/v2/metrics/$ep" 2>/dev/null)"; then
      printf '%s\n' "$out" | grep -E "$METRIC_RE" | sed 's/^/metric /'
    else
      echo "metric_error $ep $MINIO_URL"
    fi
  done
  local d
  for d in $DIRS; do
    if [[ -d "$d" ]]; then
      du -sk "$d" 2>/dev/null | awk '{printf "du %s %s\n", $2, $1}'
    fi
  done
  if command -v mc >/dev/null 2>&1 && [[ -n "$BUCKET" ]]; then
    mc du --json --depth 2 "$MC_ALIAS/$BUCKET" 2>/dev/null | sed 's/^/mcdu /'
  fi
  local c
  for c in corfu minio cassandra; do
    echo "pids $c $(pids_of "$c")"
  done
  echo "loadavg $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || echo '? ? ?')"
}

start() {
  local dir="$1" cell="$2"
  mkdir -p "$dir"
  snapshot >"$dir/$cell.before.txt"
  local all=""
  local c p
  for c in corfu minio cassandra; do
    p="$(pids_of "$c")"
    [[ -n "$p" ]] && all="${all:+$all,}$p"
  done
  if [[ -z "$all" ]]; then
    log "no server process found: pidstat not started"
    return 0
  fi
  if ! command -v pidstat >/dev/null 2>&1; then
    log "pidstat missing (apt install sysstat): server CPU not recorded"
    return 0
  fi
  # -h: one line per process per interval, time as seconds since the epoch.
  (setsid nohup pidstat -h -u -r -p "$all" "$INTERVAL" >"$dir/$cell.pidstat.txt" 2>&1 </dev/null &
   echo $! >"$dir/$cell.pidstat.pid")
  log "start cell=$cell pids=$all pidstat every ${INTERVAL}s"
}

stop() {
  local dir="$1" cell="$2"
  if [[ -f "$dir/$cell.pidstat.pid" ]]; then
    pkill -TERM -P "$(cat "$dir/$cell.pidstat.pid")" 2>/dev/null || true
    kill -TERM "$(cat "$dir/$cell.pidstat.pid")" 2>/dev/null || true
    rm -f "$dir/$cell.pidstat.pid"
  fi
  snapshot >"$dir/$cell.after.txt"
  log "stop cell=$cell"
}

verb="${1:-}"
case "$verb" in
start) [[ $# -eq 3 ]] || { echo "usage: $0 start DIR CELL" >&2; exit 2; }; start "$2" "$3" ;;
stop) [[ $# -eq 3 ]] || { echo "usage: $0 stop DIR CELL" >&2; exit 2; }; stop "$2" "$3" ;;
snapshot) snapshot ;;
*) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
