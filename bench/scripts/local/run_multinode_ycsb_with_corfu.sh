#!/usr/bin/env bash
set -euo pipefail

# Orchestrate a single Corfu server on one remote node while sweeping YCSB
# workloads and writers-per-host on N other client nodes via the Python
# multi-node orchestrator (run_multinode_ycsb.py).
#
# Same shape as run_ycsb_with_corfu.sh, except:
#   - YCSB does not run on this machine. It runs on $CLIENT_HOSTS (each host
#     spawns writers_per_host processes via run_local_ycsb_multiproc.py).
#   - Corfu still runs on a single dedicated $CORFU_BIND_HOST. All YCSB writers
#     across all client hosts share that one Corfu instance and stream.
#
# Loop order: trials -> workloads -> writers_per_host. Corfu is restarted on
# every (trial, workload, writers_per_host) iteration. All iterations share
# one OZONEDB_RUN_TAG (--run-tag, default: timestamp) so result files land in
# one tagged dir.
#
# A campaign driver (crsqlite/bench/scripts/campaign/throughput.py) calls this
# once per cell: --trial N --duration S --run-tag TAG [--linearizable]
# --workloads <wl> --writers-list <wph> --client-hosts <prefix>. The read mode
# is a flag, never a ycsb.yaml edit: --linearizable is forwarded to every
# client and shows up in the result filenames as ozonedb-corfu-linearizable,
# so a default and a strict sweep cannot be mistaken for one another.
#
# Per iteration:
#   1. stop corfu on $CORFU_BIND_HOST (best-effort)
#   2. start corfu on $CORFU_BIND_HOST (clean /mnt/corfu/run_batch from /mnt/corfu/load)
#   3. wait until $CORFU_BIND_HOST:$CORFU_PORT is reachable
#   4. run run_multinode_ycsb.py with
#        --workloads=<wl> --writers_per_host=<n> --trial=<k>
#        [--max_exec_time=<s>] [--linearizable]
#      (this SSHes to every client host and launches its writer slice)
#   5. stop corfu
# After the full sweep, corfu is started one more time so the cluster is
# left ready for a subsequent manual run.
#
# PRECONDITIONS
#   - Each client host's ycsb.yaml has corfu.endpoint set to
#     $CORFU_BIND_HOST:$CORFU_PORT (so its writer processes connect to the same
#     instance this script restarts).
#   - cloudlab.hosts in ycsb.yaml lists the client hosts (or pass
#     --client-hosts).
#   - cloudlab.ssh_user / cloudlab.ssh_private_key_path resolve for the
#     client hosts (or pass --client-user / --client-ssh-key).
#   - For non-corfu db_names (rocksdb, ozonedb local, sqlite) the loader
#     load_local_ycsb_multiproc.py must already have produced the per-writer
#     cached_data dirs on each client host. ozonedb-corfu does not need this.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${OZONEDB_HOME:=$(cd "$SCRIPT_DIR/../../.." && pwd)}"
export OZONEDB_HOME

CONFIG="${OZONEDB_HOME}/bench/scripts/config/ycsb.yaml"

# All empty: resolved from ycsb.yaml after argument parsing, so there is no
# second copy of the cluster's addressing left to drift.
CORFU_BIND_HOST=""
CORFU_SSH_HOST=""
CORFU_USER="${SSH_USER:-}"
CORFU_SSH_KEY=""
CORFU_DIR="~/CorfuDB"
CORFU_PORT=""
CORFU_LOG="/tmp/corfu_server.log"

CLIENT_HOSTS=""
CLIENT_USER=""
CLIENT_SSH_KEY=""

WORKLOADS="a b c d f"
WRITERS_LIST="1 2 4 8"
TRIALS=1
TRIALS_SET=0
TRIAL=""          # --trial N: exactly this trial (what a campaign driver passes)
DURATION=""       # --duration S: forwarded to every client as --max_exec_time
LINEARIZABLE=0    # --linearizable: strict reads, label ozonedb-corfu-linearizable
LOG_TRIM=0        # --log-trim: writer 0 checkpoints + trims the Corfu stream
LRU_CACHE_BYTES=""  # --lru-cache-bytes N: per-process block cache, label -lru64m
CACHE_WARM=0      # --cache-warm: warm compaction outputs into the block cache, label -warm
RECORD_CNT=""     # --record-cnt N: dataset size, forwarded as --record_cnt
RUN_TAG=""        # --run-tag: bench/results/local/<tag> on every host and here
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

  --config PATH             ycsb.yaml (default: $CONFIG)

  Corfu server (one node):
  --corfu-host HOST         sets both addresses below at once
  --corfu-bind-host ADDR    corfu_server -a, and what clients dial
                            (default: nodes.log.lan from ycsb.yaml)
  --corfu-ssh-host HOST     how this script reaches the node
                            (default: nodes.log.ssh, else nodes.log.lan)
  --corfu-user USER         (default: nodes.ssh_user from ycsb.yaml)
  --corfu-ssh-key PATH      (default: nodes.ssh_private_key_path)
  --corfu-dir PATH          corfu repo on remote (default: $CORFU_DIR)
  --corfu-port PORT         (default: corfu.port from ycsb.yaml)

  YCSB client nodes (orchestrated via run_multinode_ycsb.py):
  --client-hosts "h1,h2"    comma-separated. Default: read cloudlab.hosts
                            from ycsb.yaml.
  --client-user USER        default: read cloudlab.ssh_user from ycsb.yaml.
  --client-ssh-key PATH     default: read cloudlab.ssh_private_key_path
                            from ycsb.yaml.

  Sweep:
  --workloads "a b c"       space-separated workload letters
                            (default: "$WORKLOADS")
  --writers-list "1 2"      space-separated writers_per_host values
                            (default: "$WRITERS_LIST")
  --trials N                run trials 1..N per (workload, writers_per_host)
                            (default: $TRIALS)
  --trial N                 run exactly trial N -- one cell per invocation,
                            as a campaign driver does. Excludes --trials.
  --duration SECONDS        cap every YCSB run at this many seconds
                            (forwarded to each client as --max_exec_time;
                            default: each client's local.run.max_exec_time)
  --linearizable            strict reads: every writer's generated
                            shared_config gets linearizable_reads=true +
                            trust_background_tail=false, and result files
                            are labelled ozonedb-corfu-linearizable instead
                            of ozonedb-corfu. Use this, never a ycsb.yaml edit.
  --log-trim                log trimming: global writer 0 checkpoints the
                            Corfu stream to the bucket and trims behind the
                            checkpoint; every writer opens from the newest
                            checkpoint (PLAN-trimming.md). Forwarded to each
                            client as --log-trim.
  --lru-cache-bytes N       per-process SSTable block cache in bytes, written
                            into every writer's generated shared_config and
                            appended to the result label (-lru64m). The
                            cache sweep of bench/PLAN-cost.md phase 2.
  --cache-warm              compaction-aware block cache: every writer warms
                            the outputs of an applied COMPACT into its cache
                            (cache_warm_enabled=true in each generated
                            shared_config; bench/PLAN-compaction-cache.md).
                            Result label gets -warm. Forwarded to each
                            client as --cache-warm.
  --record-cnt N            dataset size in records, forwarded to every
                            client as --record_cnt (must match the load).
  --run-tag TAG             results dir bench/results/local/TAG on every host
                            and locally (default: timestamp). Reusing a tag
                            across invocations is safe -- trial, workload
                            and writer count are all in the filenames.
  --dry-run                 print the iteration plan and the exact
                            run_multinode_ycsb.py command per iteration;
                            no ssh, no corfu restart.

  -h | --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --config)         CONFIG="$2"; shift 2 ;;
  --corfu-host)      CORFU_BIND_HOST="$2"; CORFU_SSH_HOST="$2"; shift 2 ;;
  --corfu-bind-host) CORFU_BIND_HOST="$2"; shift 2 ;;
  --corfu-ssh-host)  CORFU_SSH_HOST="$2"; shift 2 ;;
  --corfu-user)     CORFU_USER="$2"; shift 2 ;;
  --corfu-ssh-key)  CORFU_SSH_KEY="$2"; shift 2 ;;
  --corfu-dir)      CORFU_DIR="$2"; shift 2 ;;
  --corfu-port)     CORFU_PORT="$2"; shift 2 ;;
  --client-hosts)   CLIENT_HOSTS="$2"; shift 2 ;;
  --client-user)    CLIENT_USER="$2"; shift 2 ;;
  --client-ssh-key) CLIENT_SSH_KEY="$2"; shift 2 ;;
  --workloads)      WORKLOADS="$2"; shift 2 ;;
  --writers-list)   WRITERS_LIST="$2"; shift 2 ;;
  --trials)         TRIALS="$2"; TRIALS_SET=1; shift 2 ;;
  --trial)          TRIAL="$2"; shift 2 ;;
  --duration)       DURATION="$2"; shift 2 ;;
  --linearizable)   LINEARIZABLE=1; shift ;;
  --log-trim)       LOG_TRIM=1; shift ;;
  --lru-cache-bytes) LRU_CACHE_BYTES="$2"; shift 2 ;;
  --cache-warm)     CACHE_WARM=1; shift ;;
  --record-cnt)     RECORD_CNT="$2"; shift 2 ;;
  --run-tag)        RUN_TAG="$2"; shift 2 ;;
  --dry-run)        DRY_RUN=1; shift ;;
  -h | --help)      usage; exit 0 ;;
  *)
    echo "Unknown option: $1" >&2
    usage
    exit 1
    ;;
  esac
done

read -r -a WORKLOADS_ARR <<<"$WORKLOADS"
read -r -a WRITERS_ARR <<<"$WRITERS_LIST"
if [[ ${#WORKLOADS_ARR[@]} -eq 0 || ${#WRITERS_ARR[@]} -eq 0 ]]; then
  echo "workloads and writers-list must each contain at least one value" >&2
  exit 1
fi
if ! [[ "$TRIALS" =~ ^[1-9][0-9]*$ ]]; then
  echo "--trials must be a positive integer (got: $TRIALS)" >&2
  exit 1
fi
if [[ -n "$TRIAL" ]]; then
  if [[ "$TRIALS_SET" -eq 1 ]]; then
    echo "--trial and --trials are mutually exclusive" >&2
    exit 1
  fi
  if ! [[ "$TRIAL" =~ ^[1-9][0-9]*$ ]]; then
    echo "--trial must be a positive integer (got: $TRIAL)" >&2
    exit 1
  fi
  TRIAL_SEQ=("$TRIAL")
else
  # read -r -a rather than mapfile: this also runs on a macOS bash 3.2.
  read -r -a TRIAL_SEQ <<<"$(seq 1 "$TRIALS" | tr '\n' ' ')"
fi
if [[ -n "$DURATION" ]] && ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]]; then
  echo "--duration must be a positive integer (got: $DURATION)" >&2
  exit 1
fi
if [[ -n "$RUN_TAG" ]] && ! [[ "$RUN_TAG" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "--run-tag is a directory name and must match [A-Za-z0-9._-]+ (got: $RUN_TAG)" >&2
  exit 1
fi
if [[ -n "$LRU_CACHE_BYTES" ]] && ! [[ "$LRU_CACHE_BYTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "--lru-cache-bytes must be a positive integer (got: $LRU_CACHE_BYTES)" >&2
  exit 1
fi
if [[ -n "$RECORD_CNT" ]] && ! [[ "$RECORD_CNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "--record-cnt must be a positive integer (got: $RECORD_CNT)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Resolve the corfu node from ycsb.yaml's `nodes:` block instead of a hardcoded
# constant. This default used to say 10.10.1.2 while corfu.endpoint said
# 10.10.1.3 -- which would start the server on the MinIO node, bound to an
# address no client was configured to dial.
#
# The two addresses are deliberately separate:
#   BIND  nodes.log.lan -- what corfu_server -a binds, and what clients dial
#   SSH   nodes.log.ssh -- how THIS script reaches the box (falls back to lan)
# Overrides still win; --corfu-host sets both, as it always did.
# ---------------------------------------------------------------------------
cfg() { python3 "$OZONEDB_HOME/bench/scripts/ycsb_config.py" --config "$CONFIG" "$@"; }
[[ -n "$CORFU_BIND_HOST" ]] || CORFU_BIND_HOST="$(cfg --node log --field lan)" || exit 1
[[ -n "$CORFU_SSH_HOST"  ]] || CORFU_SSH_HOST="$(cfg --node log --field ssh)"  || exit 1
[[ -n "$CORFU_USER"      ]] || CORFU_USER="$(cfg --get nodes.ssh_user)"        || exit 1
[[ -n "$CORFU_SSH_KEY"   ]] || CORFU_SSH_KEY="$(cfg --get nodes.ssh_private_key_path)" || exit 1
[[ -n "$CORFU_PORT"      ]] || CORFU_PORT="$(cfg --get corfu.port)"            || exit 1
echo "[corfu] node: ssh=$CORFU_SSH_HOST bind=$CORFU_BIND_HOST port=$CORFU_PORT (from $CONFIG)"

CORFU_SSH_OPTS=(-o BatchMode=yes -o ServerAliveInterval=30 -o StrictHostKeyChecking=accept-new)
[[ -n "$CORFU_SSH_KEY" ]] && CORFU_SSH_OPTS+=(-i "$CORFU_SSH_KEY")
CORFU_TARGET="${CORFU_USER}@${CORFU_SSH_HOST}"

corfu_sh() {
  ssh "${CORFU_SSH_OPTS[@]}" "$CORFU_TARGET" "$1"
}

# Probed ON the corfu node against its own bind address. A local /dev/tcp check
# cannot succeed when the bind address is a CloudLab-internal 10.10.1.x and the
# sweep is driven from off-cluster.
port_open() {
  corfu_sh "bash -c '(exec 3<>/dev/tcp/$1/$2) >/dev/null 2>&1'" >/dev/null 2>&1
}

# The SSTable bucket must be restored together with the log (PLAN-trimming.md
# §5): the log references SSTables by name, and with --log-trim the checkpoint
# in the bucket must match the trim mark in the log dir. A cell's compactions
# and checkpoints change the bucket, so the next cell would otherwise open a
# restored log against a bucket that is ahead of it (bootstrap throws) or that
# lost files the log still names. load_corfu_dataset.sh writes the snapshot to
# /mnt/corfu/load-bucket; when it is absent (a log-only snapshot from before
# this) the bucket is left as is, which is only safe without --log-trim.
S3_BUCKET="$(cfg --get s3.bucket 2>/dev/null || echo ozonedb-ycsb)"
MC_ALIAS="${MC_ALIAS:-ozonedb-local}"

start_corfu() {
  echo "[corfu] starting on $CORFU_TARGET ($CORFU_BIND_HOST:$CORFU_PORT)"
  corfu_sh "if [ -d /mnt/corfu/load-bucket ]; then mc mirror --overwrite --remove --quiet /mnt/corfu/load-bucket $MC_ALIAS/$S3_BUCKET >/dev/null && echo '[corfu] bucket restored from /mnt/corfu/load-bucket'; else echo '[corfu] WARNING: no /mnt/corfu/load-bucket snapshot; bucket left as is'; fi"
  # Trailing & must apply only to nohup, not to the whole chain — otherwise
  # ssh holds stdout open on a foregrounded subshell and hangs.
  corfu_sh "cd $CORFU_DIR && rm -rf /mnt/corfu/run_batch/ && cp -r /mnt/corfu/load/ /mnt/corfu/run_batch/ && ( setsid nohup env CORFUDB_HEAP=122880 ./bin/corfu_server -l /mnt/corfu/run_batch -s -a $CORFU_BIND_HOST $CORFU_PORT </dev/null >$CORFU_LOG 2>&1 & )"
  sleep 10
  corfu_sh "pgrep -af 'org.corfudb.infrastructure.CorfuServer' | head -n1 || echo '[corfu] WARNING: no CorfuServer process found after start'"
}

stop_corfu() {
  echo "[corfu] stopping on $CORFU_TARGET"
  # bin/corfu_server execs java, so the live cmdline matches the main class,
  # not the wrapper name. SIGKILL directly — we wipe /mnt/corfu/run_batch
  # next start, so graceful shutdown buys nothing.
  # [C]orfuServer / [-]a: the patterns must not match the remote shell that
  # runs these pkills, or the shell is SIGKILLed with the server and ssh
  # exits 255 before the rest of the line runs.
  corfu_sh "pkill -KILL -f 'org.corfudb.infrastructure.[C]orfuServer' 2>/dev/null || true; pkill -KILL -f '[-]a $CORFU_BIND_HOST $CORFU_PORT' 2>/dev/null || true; command -v fuser >/dev/null && fuser -k -9 ${CORFU_PORT}/tcp 2>/dev/null || true"
  for _ in $(seq 1 30); do
    if ! port_open "$CORFU_BIND_HOST" "$CORFU_PORT"; then
      echo "[corfu] down"
      return 0
    fi
    sleep 1
  done
  echo "[corfu] ERROR: $CORFU_BIND_HOST:$CORFU_PORT still reachable after SIGKILL" >&2
  corfu_sh "pgrep -af CorfuServer 2>/dev/null || true; ss -ltnp 2>/dev/null | grep :${CORFU_PORT} || netstat -ltnp 2>/dev/null | grep :${CORFU_PORT} || true"
  return 1
}

wait_for_corfu() {
  echo "[corfu] waiting for $CORFU_BIND_HOST:$CORFU_PORT"
  for _ in $(seq 1 120); do
    if port_open "$CORFU_BIND_HOST" "$CORFU_PORT"; then
      echo "[corfu] up"
      sleep 10
      return 0
    fi
    sleep 1
  done
  echo "[corfu] ERROR: never became reachable (tail $CORFU_LOG on $CORFU_BIND_HOST)" >&2
  return 1
}

FINAL_STATE_LEFT_RUNNING=0
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ "$FINAL_STATE_LEFT_RUNNING" -eq 0 ]]; then
    echo "[orchestrator] cleanup (rc=$rc) -- stopping remote corfu"
    stop_corfu || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

BENCH_SCRIPT="$OZONEDB_HOME/bench/scripts/local/run_multinode_ycsb.py"
RESULTS=()

# All trials/workloads/writers iterations land under this single tag.
# run_multinode_ycsb.py also passes it through to each client host so
# every node writes into the same tag dir. --run-tag lets a campaign driver
# accumulate many invocations (one per cell) in one dir.
export OZONEDB_RUN_TAG="${RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
READ_MODE=default
[[ "$LINEARIZABLE" -eq 1 ]] && READ_MODE=linearizable

# Common args to every run_multinode_ycsb.py invocation.
COMMON_ARGS=(--config "$CONFIG" --run_tag "$OZONEDB_RUN_TAG")
[[ -n "$CLIENT_HOSTS"   ]] && COMMON_ARGS+=(--hosts "$CLIENT_HOSTS")
[[ -n "$CLIENT_USER"    ]] && COMMON_ARGS+=(--ssh_user "$CLIENT_USER")
[[ -n "$CLIENT_SSH_KEY" ]] && COMMON_ARGS+=(--ssh_key "$CLIENT_SSH_KEY")
[[ -n "$DURATION"       ]] && COMMON_ARGS+=(--max_exec_time "$DURATION")
[[ "$LINEARIZABLE" -eq 1 ]] && COMMON_ARGS+=(--linearizable)
[[ "$LOG_TRIM" -eq 1 ]] && COMMON_ARGS+=(--log-trim)
[[ -n "$LRU_CACHE_BYTES" ]] && COMMON_ARGS+=(--lru-cache-bytes "$LRU_CACHE_BYTES")
[[ "$CACHE_WARM" -eq 1 ]] && COMMON_ARGS+=(--cache-warm)
[[ -n "$RECORD_CNT"      ]] && COMMON_ARGS+=(--record_cnt "$RECORD_CNT")

echo "=== sweep: workloads=(${WORKLOADS_ARR[*]}) writers_per_host=(${WRITERS_ARR[*]}) trials=(${TRIAL_SEQ[*]}) read_mode=$READ_MODE log_trim=$LOG_TRIM lru_cache_bytes=${LRU_CACHE_BYTES:-base} cache_warm=$CACHE_WARM record_cnt=${RECORD_CNT:-yaml} duration=${DURATION:-yaml} run_tag=$OZONEDB_RUN_TAG ==="
echo "=== corfu server: $CORFU_TARGET:$CORFU_PORT ==="
if [[ -n "$CLIENT_HOSTS" ]]; then
  echo "=== client hosts (override): $CLIENT_HOSTS ==="
else
  echo "=== client hosts: from cloudlab.hosts in $CONFIG ==="
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  for TRIAL in "${TRIAL_SEQ[@]}"; do
    for WL in "${WORKLOADS_ARR[@]}"; do
      for NW in "${WRITERS_ARR[@]}"; do
        echo "[dry-run] trial=$TRIAL workload=$WL writers_per_host=$NW: restart corfu on $CORFU_TARGET, then"
        echo "[dry-run]   python3 $BENCH_SCRIPT ${COMMON_ARGS[*]} --workloads $WL --writers_per_host $NW --trial $TRIAL"
      done
    done
  done
  FINAL_STATE_LEFT_RUNNING=1 # nothing was touched; cleanup must not stop corfu
  exit 0
fi

for TRIAL in "${TRIAL_SEQ[@]}"; do
  for WL in "${WORKLOADS_ARR[@]}"; do
    for NW in "${WRITERS_ARR[@]}"; do
      echo ""
      echo "### iteration: trial=$TRIAL (of: ${TRIAL_SEQ[*]}) workload=$WL writers_per_host=$NW ###"

      stop_corfu || true
      start_corfu
      wait_for_corfu

      echo "[bench] python3 $BENCH_SCRIPT ${COMMON_ARGS[*]} --workloads $WL --writers_per_host $NW --trial $TRIAL"
      set +e
      python3 "$BENCH_SCRIPT" "${COMMON_ARGS[@]}" \
        --workloads "$WL" \
        --writers_per_host "$NW" \
        --trial "$TRIAL"
      rc=$?
      set -e
      RESULTS+=("trial=$TRIAL workload=$WL writers_per_host=$NW rc=$rc")
      echo "[bench] trial=$TRIAL workload=$WL writers_per_host=$NW rc=$rc"

      stop_corfu || echo "[orchestrator] warning: stop_corfu failed; continuing sweep"
    done
  done
done

echo ""
echo "=== final restart: corfu on $CORFU_BIND_HOST:$CORFU_PORT ==="
start_corfu
wait_for_corfu
FINAL_STATE_LEFT_RUNNING=1

echo ""
echo "=== sweep summary ==="
for line in "${RESULTS[@]}"; do
  echo "  $line"
done

for line in "${RESULTS[@]}"; do
  if [[ "$line" != *"rc=0"* ]]; then
    exit 1
  fi
done
exit 0
