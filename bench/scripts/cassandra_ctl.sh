#!/usr/bin/env bash
#
# Control the Cassandra baseline server ON the server node.
#
#   cassandra_ctl.sh start          start in the background (log: $INSTALL_DIR/cassandra_server.log)
#   cassandra_ctl.sh stop           nodetool drain (best effort), then kill; wait for the port to close
#   cassandra_ctl.sh status         pid + port state (exit 0 when up)
#   cassandra_ctl.sh wait           block until CQL answers (up to 180 s)
#   cassandra_ctl.sh wipe           stop + delete the data tree: a fresh, empty node
#   cassandra_ctl.sh schema [--rf N] [--fields N] [--keyspace K]
#                                   DROP + CREATE the YCSB keyspace/table (server must be up)
#   cassandra_ctl.sh save-load      stop + copy data -> data.load   (after the load phase)
#   cassandra_ctl.sh restore-load   stop + copy data.load -> data   (before every run trial)
#   cassandra_ctl.sh space [KS]     bytes on disk: nodetool tablestats (server up) + du -sk
#   cassandra_ctl.sh compact [KS]   nodetool compact, then wait until no compaction is pending
#   cassandra_ctl.sh cqlsh ...      cqlsh against this node
#
# space / compact are the phase 1 measurements of bench/PLAN-cost.md: `space`
# before `compact` is the transient peak, after it is the steady footprint
# (sC, the Cassandra bytes on disk per logical byte).
#
# save-load / restore-load are the Corfu /mnt/corfu/{load,run_batch} pattern:
# every run trial starts from the identical, drained on-disk state the load
# phase produced, so trials are comparable and never see each other's writes.
#
# Installed by `setup.sh --role cassandra` as $INSTALL_DIR/cassandra_ctl.sh;
# the orchestrator wrappers scp the tree copy over it before each use, so an
# edit here propagates without re-running setup. Reads $INSTALL_DIR/env.sh
# (written by setup.sh): JAVA_HOME, CASSANDRA_HOME, CASSANDRA_BIND,
# CASSANDRA_CQL_PORT, MAX_HEAP_SIZE, HEAP_NEWSIZE.

set -euo pipefail

INSTALL_DIR="${CASSANDRA_INSTALL_DIR:-/tank/cassandra}"
ENV_FILE="$INSTALL_DIR/env.sh"
DATA_DIR="$INSTALL_DIR/data"
LOAD_DIR="$INSTALL_DIR/data.load"
PID_FILE="$INSTALL_DIR/cassandra.pid"
SERVER_LOG="$INSTALL_DIR/cassandra_server.log"

log() { printf '[cassandra_ctl] %s\n' "$*"; }
die() {
  printf '[cassandra_ctl:error] %s\n' "$*" >&2
  exit 1
}

[[ -f "$ENV_FILE" ]] || die "$ENV_FILE missing -- run: bash bench/scripts/setup.sh --role cassandra"
# shellcheck disable=SC1090
. "$ENV_FILE"
: "${CASSANDRA_HOME:?}" "${CASSANDRA_BIND:?}" "${CASSANDRA_CQL_PORT:?}" "${JAVA_HOME:?}"

# CQL client. cqlsh refuses Python >= 3.12 (its driver imports asyncore, gone
# in 3.12), and the CloudLab image ships only 3.12, so setup.sh builds a venv
# with cassandra-driver and drops cassandra_cql.py beside this script. Use it
# when present; fall back to cqlsh otherwise (older nodes with Python <= 3.11).
CQL_VENV_PY="${CASSANDRA_CQL_PYTHON:-$INSTALL_DIR/pyenv/bin/python}"
CQL_HELPER="${CASSANDRA_CQL_HELPER:-$INSTALL_DIR/cassandra_cql.py}"

port_open() { (exec 3<>"/dev/tcp/$CASSANDRA_BIND/$CASSANDRA_CQL_PORT") >/dev/null 2>&1; }

server_pids() {
  # bin/cassandra execs java; match the daemon class, not the wrapper name.
  pgrep -f 'org.apache.cassandra.service.CassandraDaemon' 2>/dev/null || true
}

# Run one or more CQL statements. Prefers the venv helper; if it is absent,
# falls back to the bundled cqlsh (which needs Python <= 3.11).
cql() {
  if [[ -x "$CQL_VENV_PY" && -f "$CQL_HELPER" ]]; then
    "$CQL_VENV_PY" "$CQL_HELPER" --host "$CASSANDRA_BIND" --port "$CASSANDRA_CQL_PORT" "$@"
  else
    "$CASSANDRA_HOME/bin/cqlsh" "$CASSANDRA_BIND" "$CASSANDRA_CQL_PORT" --request-timeout=120 "$@"
  fi
}
cql_ping() {
  if [[ -x "$CQL_VENV_PY" && -f "$CQL_HELPER" ]]; then
    "$CQL_VENV_PY" "$CQL_HELPER" --host "$CASSANDRA_BIND" --port "$CASSANDRA_CQL_PORT" --ping >/dev/null 2>&1
  else
    "$CASSANDRA_HOME/bin/cqlsh" "$CASSANDRA_BIND" "$CASSANDRA_CQL_PORT" -e "DESCRIBE CLUSTER" >/dev/null 2>&1
  fi
}

do_status() {
  local pids
  pids="$(server_pids | tr '\n' ' ')"
  if port_open; then
    log "up: pids=${pids:-?} $CASSANDRA_BIND:$CASSANDRA_CQL_PORT open"
    return 0
  fi
  log "down: pids=${pids:-none} $CASSANDRA_BIND:$CASSANDRA_CQL_PORT closed"
  return 1
}

do_start() {
  if port_open; then
    log "already up on $CASSANDRA_BIND:$CASSANDRA_CQL_PORT"
    return 0
  fi
  mkdir -p "$DATA_DIR" "$CASSANDRA_LOG_DIR"
  ulimit -n 100000 2>/dev/null || true
  log "starting $CASSANDRA_HOME (bind=$CASSANDRA_BIND heap=$MAX_HEAP_SIZE) -> $SERVER_LOG"
  # bin/cassandra daemonizes itself without -f; -p records the pid.
  "$CASSANDRA_HOME/bin/cassandra" -p "$PID_FILE" >"$SERVER_LOG" 2>&1 </dev/null
}

do_wait() {
  local i
  log "waiting for CQL on $CASSANDRA_BIND:$CASSANDRA_CQL_PORT"
  for i in $(seq 1 180); do
    if port_open && cql_ping; then
      log "up after ${i}s"
      return 0
    fi
    if [[ $((i % 30)) -eq 0 ]] && [[ -z "$(server_pids)" ]]; then
      die "no CassandraDaemon process after ${i}s -- tail $SERVER_LOG and $CASSANDRA_LOG_DIR/system.log"
    fi
    sleep 1
  done
  die "CQL never answered on $CASSANDRA_BIND:$CASSANDRA_CQL_PORT (tail $CASSANDRA_LOG_DIR/system.log)"
}

do_stop() {
  local pids i
  pids="$(server_pids | tr '\n' ' ')"
  if [[ -z "$pids" ]] && ! port_open; then
    log "already down"
    rm -f "$PID_FILE"
    return 0
  fi
  # drain flushes memtables and stops accepting writes, so the on-disk tree
  # is complete and the commit log is empty: what save-load must snapshot.
  if port_open; then
    log "nodetool drain"
    # No -h: JMX is local-only by default (LOCAL_JMX=yes in cassandra-env.sh),
    # so nodetool must dial 127.0.0.1:7199, not the LAN bind address.
    "$CASSANDRA_HOME/bin/nodetool" drain >/dev/null 2>&1 || log "warning: nodetool drain failed; killing anyway"
  fi
  log "killing ${pids:-(by pattern)}"
  pkill -TERM -f 'org.apache.cassandra.service.CassandraDaemon' 2>/dev/null || true
  for i in $(seq 1 30); do
    [[ -z "$(server_pids)" ]] && ! port_open && break
    sleep 1
  done
  if [[ -n "$(server_pids)" ]]; then
    log "still alive after 30s; SIGKILL"
    pkill -KILL -f 'org.apache.cassandra.service.CassandraDaemon' 2>/dev/null || true
    sleep 2
  fi
  rm -f "$PID_FILE"
  [[ -z "$(server_pids)" ]] || die "CassandraDaemon survived SIGKILL"
  log "down"
}

do_wipe() {
  do_stop
  log "removing $DATA_DIR"
  rm -rf "$DATA_DIR"
  mkdir -p "$DATA_DIR"
}

do_schema() {
  local rf=1 fields=10 keyspace=ycsb
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --rf) rf="$2"; shift 2 ;;
    --fields) fields="$2"; shift 2 ;;
    --keyspace) keyspace="$2"; shift 2 ;;
    *) die "schema: unknown option $1" ;;
    esac
  done
  port_open || die "schema: server is not up (run: cassandra_ctl.sh start && cassandra_ctl.sh wait)"
  local cols="" i
  for i in $(seq 0 $((fields - 1))); do
    cols="$cols, field$i varchar"
  done
  log "schema: keyspace=$keyspace rf=$rf fields=$fields (DROP + CREATE)"
  cql -e "DROP KEYSPACE IF EXISTS $keyspace;"
  cql -e "CREATE KEYSPACE $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $rf};"
  cql -e "CREATE TABLE $keyspace.usertable (y_id varchar PRIMARY KEY$cols);"
  # Confirm with a plain SELECT rather than DESCRIBE: the venv helper returns
  # driver rows, and an empty table answering at all proves the schema exists.
  local n
  n="$(cql -e "SELECT count(*) FROM $keyspace.usertable;" | tail -n1)"
  log "schema ready: $keyspace.usertable count=$n"
}

do_save_load() {
  do_stop
  [[ -d "$DATA_DIR" ]] || die "save-load: $DATA_DIR does not exist -- nothing was loaded"
  log "snapshot $DATA_DIR -> $LOAD_DIR"
  rm -rf "$LOAD_DIR"
  cp -a "$DATA_DIR" "$LOAD_DIR"
  du -sh "$LOAD_DIR" | sed 's/^/[cassandra_ctl]   /'
}

do_restore_load() {
  do_stop
  [[ -d "$LOAD_DIR" ]] || die "restore-load: $LOAD_DIR missing -- run load_multinode_cassandra.sh first (or pass --no-restore to the sweep)"
  log "restore $LOAD_DIR -> $DATA_DIR"
  rm -rf "$DATA_DIR"
  cp -a "$LOAD_DIR" "$DATA_DIR"
}

# Bytes on disk. Two views: what Cassandra counts as live SSTable bytes
# (tablestats; needs the server), and what the filesystem holds (du; the
# commit log and obsolete SSTables included -- what a node actually needs).
do_space() {
  local keyspace="${1:-ycsb}"
  if port_open; then
    log "nodetool tablestats $keyspace"
    "$CASSANDRA_HOME/bin/nodetool" tablestats "$keyspace" |
      grep -E 'Table:|Space used|SSTable count|Compression ratio|Number of partitions|Bytes repaired|Bytes unrepaired' |
      sed 's/^[[:space:]]*/[cassandra_ctl]   /'
  else
    log "server is down: nodetool tablestats skipped, du only"
  fi
  local d
  for d in "$DATA_DIR" "$DATA_DIR/data" "$DATA_DIR/commitlog" "$LOAD_DIR"; do
    [[ -d "$d" ]] && du -sk "$d" 2>/dev/null | awk '{printf "[cassandra_ctl] du_kb %s %s\n", $2, $1}'
  done
  return 0
}

# Major compaction, and wait until compactionstats reports nothing pending,
# so the `space` that follows is the settled footprint.
do_compact() {
  local keyspace="${1:-ycsb}"
  port_open || die "compact: server is not up (run: cassandra_ctl.sh start && cassandra_ctl.sh wait)"
  log "nodetool compact $keyspace (blocks until the major compaction returns)"
  "$CASSANDRA_HOME/bin/nodetool" compact "$keyspace"
  local i
  for i in $(seq 1 1800); do
    if "$CASSANDRA_HOME/bin/nodetool" compactionstats 2>/dev/null | grep -q 'pending tasks: 0'; then
      log "no compaction pending after ${i}s"
      return 0
    fi
    sleep 1
  done
  die "compactionstats still reports pending tasks after 1800s"
}

verb="${1:-}"
[[ -n "$verb" ]] || {
  sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}
shift
case "$verb" in
start) do_start ;;
stop) do_stop ;;
status) do_status ;;
wait) do_wait ;;
wipe) do_wipe ;;
schema) do_schema "$@" ;;
save-load) do_save_load ;;
restore-load) do_restore_load ;;
space) do_space "$@" ;;
compact) do_compact "$@" ;;
cqlsh) cql "$@" ;;
*) die "unknown verb: $verb" ;;
esac
