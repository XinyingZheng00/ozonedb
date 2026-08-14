#!/usr/bin/env bash
#
# One entrypoint for provisioning a node. Idempotent: re-running is cheap and
# never duplicates shell config or reinstalls what is already present.
#
#   bash bench/scripts/setup.sh --role client        # bench/YCSB machine
#   bash bench/scripts/setup.sh --role corfu-server  # the shared-log node
#   bash bench/scripts/setup.sh --role minio         # SSTable object store
#   bash bench/scripts/setup.sh --role all
#
# You do NOT need to source this. It writes ~/.ozonedb.env and wires a guarded
# block into ~/.profile and ~/.bashrc, then sources that file itself for the
# rest of the run. Open a new shell (or `. ~/.ozonedb.env`) afterwards.
#
# JDK policy -- see docs/ or CLAUDE.md. Three constraints pull in different
# directions, which is why there are two knobs:
#   * YCSB compiles with <source>1.8</source> (ycsb/pom.xml), and support for
#     source 8 is deprecated and being removed in newer JDKs.
#   * corfu-bridge targets Java 11, so anything running it needs JDK >= 11.
#   * CorfuDB itself is built with a much newer JDK (setup_corfu.sh used 25).
# So --jdk (default 17) is the node's persistent JAVA_HOME, and --corfu-jdk
# (default 25) is used only for the CorfuDB build. Both fall back through a
# candidate list if the exact apt package is unavailable.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ROLES=()
JDK_VERSION=""
CORFU_JDK_VERSION=""
JDK_FALLBACKS=(17 21 11 25)
CORFU_JDK_FALLBACKS=(25 21 17)
DO_BUILD=1
DO_CORFU_RUNTIME=1
CORFU_REPO="https://github.com/CorfuDB/CorfuDB.git"
CORFU_CLONE_DIR="${CLONE_DIR:-$HOME/CorfuDB}"
CORFU_DATA_DIR="/mnt/corfu"
TANK_DIR="/tank"
MINIO_DATA_DIR="/tank/minio"
MINIO_BUCKET=""
MINIO_USER="minioadmin"
MINIO_PASSWORD="minioadmin"
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001

ENV_FILE="$HOME/.ozonedb.env"
MARK_BEGIN="# >>> ozonedb env >>>"
MARK_END="# <<< ozonedb env <<<"

usage() {
  cat <<EOF
Provision a node for OzoneDB. Idempotent -- re-running is cheap and never
duplicates shell config or reinstalls what is already present.

  bash bench/scripts/setup.sh --role client        # bench/YCSB machine
  bash bench/scripts/setup.sh --role corfu-server  # the shared-log node
  bash bench/scripts/setup.sh --role minio         # SSTable object store
  bash bench/scripts/setup.sh --role all

You do NOT need to source this. It writes ~/.ozonedb.env and wires a guarded
block into ~/.profile and ~/.bashrc. Open a new shell afterwards, or run
\`. ~/.ozonedb.env\`.

JDK policy: three constraints pull in different directions, hence two knobs.
YCSB compiles with <source>1.8</source>; corfu-bridge targets Java 11 so
anything running it needs JDK >= 11; CorfuDB itself wants a much newer JDK.
--jdk sets the node's persistent JAVA_HOME, --corfu-jdk is used only to build
CorfuDB. Both fall back through a candidate list if the exact apt package is
unavailable.

Options:
  --role ROLE          client | corfu-server | minio | all   (repeatable, default: client)
  --jdk N              persistent JAVA_HOME JDK major version (default: ${JDK_FALLBACKS[0]})
  --corfu-jdk N        JDK used only to build CorfuDB (default: ${CORFU_JDK_FALLBACKS[0]})
  --no-build           client: skip bench/scripts/build.sh at the end
  --no-corfu-runtime   client: skip installing the CorfuDB runtime into ~/.m2.
                       corfu-bridge cannot build without it -- only pass this
                       if ~/.m2 already has org.corfudb:runtime, or you are
                       building without -DOZONEDB_ENABLE_CORFU=ON.
  --corfu-dir PATH     CorfuDB checkout (default: $CORFU_CLONE_DIR)
  --corfu-data PATH    corfu-server log directories (default: $CORFU_DATA_DIR)
  --tank PATH          data directory the configs point db_path at (default: $TANK_DIR)
  --minio-data PATH    minio data directory (default: $MINIO_DATA_DIR)
  --bucket NAME        minio bucket (default: s3.bucket from ycsb.yaml)
  -h | --help
EOF
}

log() { printf '\033[1;36m[setup]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[setup:warn]\033[0m %s\n' "$*" >&2; }
die() {
  printf '\033[1;31m[setup:error]\033[0m %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --role)
    ROLES+=("$2")
    shift 2
    ;;
  --jdk)
    JDK_VERSION="$2"
    shift 2
    ;;
  --corfu-jdk)
    CORFU_JDK_VERSION="$2"
    shift 2
    ;;
  --no-build)
    DO_BUILD=0
    shift
    ;;
  --no-corfu-runtime)
    DO_CORFU_RUNTIME=0
    shift
    ;;
  --corfu-dir)
    CORFU_CLONE_DIR="$2"
    shift 2
    ;;
  --corfu-data)
    CORFU_DATA_DIR="$2"
    shift 2
    ;;
  --tank)
    TANK_DIR="$2"
    shift 2
    ;;
  --minio-data)
    MINIO_DATA_DIR="$2"
    shift 2
    ;;
  --bucket)
    MINIO_BUCKET="$2"
    shift 2
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *) die "unknown option: $1 (try --help)" ;;
  esac
done

[[ ${#ROLES[@]} -eq 0 ]] && ROLES=(client)
if [[ " ${ROLES[*]} " == *" all "* ]]; then
  ROLES=(client corfu-server minio)
fi
for r in "${ROLES[@]}"; do
  case "$r" in
  client | corfu-server | minio) ;;
  *) die "unknown role: $r" ;;
  esac
done

# ---------------------------------------------------------------- primitives

APT_UPDATED=0
apt_update_once() {
  [[ $APT_UPDATED -eq 1 ]] && return 0
  log "apt-get update"
  sudo apt-get update -qq
  APT_UPDATED=1
}

# Install only what is missing, so a re-run costs nothing.
apt_install() {
  local missing=()
  for pkg in "$@"; do
    dpkg -s "$pkg" >/dev/null 2>&1 || missing+=("$pkg")
  done
  if [[ ${#missing[@]} -eq 0 ]]; then
    log "already installed: $*"
    return 0
  fi
  apt_update_once
  log "installing: ${missing[*]}"
  sudo apt-get install -y "${missing[@]}"
}

apt_available() { apt-cache show "$1" >/dev/null 2>&1; }

# Resolve JAVA_HOME for an installed JDK major version.
java_home_for() {
  local v="$1" arch candidate
  arch="$(dpkg --print-architecture)"
  for candidate in \
    "/usr/lib/jvm/java-${v}-openjdk-${arch}" \
    "/usr/lib/jvm/java-1.${v}.0-openjdk-${arch}"; do
    [[ -x "$candidate/bin/javac" ]] && {
      echo "$candidate"
      return 0
    }
  done
  # Glob fallback: packaging names vary across Ubuntu releases.
  for candidate in /usr/lib/jvm/java-${v}-openjdk*; do
    [[ -x "$candidate/bin/javac" ]] && {
      echo "$candidate"
      return 0
    }
  done
  return 1
}

# Install the first available JDK from a candidate list; echo its major version.
# $1 = explicitly requested version (may be empty), remaining args = fallbacks.
ensure_jdk() {
  local requested="$1"
  shift
  local candidates=("$@")
  [[ -n "$requested" ]] && candidates=("$requested")

  local v
  for v in "${candidates[@]}"; do
    if java_home_for "$v" >/dev/null; then
      echo "$v"
      return 0
    fi
  done

  apt_update_once
  for v in "${candidates[@]}"; do
    if apt_available "openjdk-${v}-jdk"; then
      log "installing openjdk-${v}-jdk"
      sudo apt-get install -y "openjdk-${v}-jdk"
      java_home_for "$v" >/dev/null && {
        echo "$v"
        return 0
      }
    fi
  done

  if [[ -n "$requested" ]]; then
    die "openjdk-${requested}-jdk is not installable on this release. Pass a different --jdk."
  fi
  die "none of these JDKs are installable: ${candidates[*]}"
}

# ------------------------------------------------------------------ env file

strip_env_block() {
  local rc="$1"
  [[ -f "$rc" ]] || return 0
  awk -v b="$MARK_BEGIN" -v e="$MARK_END" '
    $0 == b { skip = 1 }
    !skip   { print }
    $0 == e { skip = 0 }
  ' "$rc" >"${rc}.ozonedb.tmp" && mv "${rc}.ozonedb.tmp" "$rc"
}

# Rewrites, never appends -- the old setup_node.sh grew a duplicate set of
# exports on every run.
wire_env() {
  local java_home="$1"

  # $JAVA_HOME and $PATH are escaped so they expand when the file is sourced.
  # The old script expanded $JAVA_HOME at write time, while it was still
  # unset, and wrote a literal `export PATH=/bin:$PATH` into both rc files.
  # The PATH guard keeps repeated sourcing idempotent: a bash login shell runs
  # .profile, which on Ubuntu sources .bashrc, so this file gets sourced twice.
  cat >"$ENV_FILE" <<EOF
# Generated by bench/scripts/setup.sh -- do not edit, changes are overwritten.
export OZONEDB_HOME="$REPO_ROOT"
export JAVA_HOME="$java_home"
case ":\$PATH:" in
  *":\$JAVA_HOME/bin:"*) ;;
  *) export PATH="\$JAVA_HOME/bin:\$PATH" ;;
esac
EOF
  log "wrote $ENV_FILE"

  local rc
  for rc in "$HOME/.profile" "$HOME/.bashrc"; do
    [[ -f "$rc" ]] || touch "$rc"
    strip_env_block "$rc"
    {
      echo "$MARK_BEGIN"
      echo "[ -f \"$ENV_FILE\" ] && . \"$ENV_FILE\""
      echo "$MARK_END"
    } >>"$rc"
    log "wired $ENV_FILE into $rc"
  done

  # `source ~/.bashrc` from a script is a no-op: Ubuntu's default .bashrc
  # returns immediately for non-interactive shells. Source the env file itself.
  # shellcheck disable=SC1090
  . "$ENV_FILE"
}

# --------------------------------------------------------------- corfu runtime

# corfu-bridge/pom.xml depends on org.corfudb:runtime:0.9.1.0-SNAPSHOT and
# declares no <repositories>, so it resolves ONLY from the local ~/.m2. Any
# machine that builds the bridge -- which includes every client node, because
# CMake builds it under -DOZONEDB_ENABLE_CORFU=ON -- needs CorfuDB installed
# locally first.
corfu_runtime_present() {
  compgen -G "$HOME/.m2/repository/org/corfudb/runtime/*/runtime-*.jar" >/dev/null 2>&1
}

install_corfu_runtime() {
  if corfu_runtime_present; then
    log "CorfuDB runtime already in ~/.m2 -- skipping (delete it to force a rebuild)"
    return 0
  fi

  local corfu_jdk corfu_java_home
  corfu_jdk="$(ensure_jdk "$CORFU_JDK_VERSION" "${CORFU_JDK_FALLBACKS[@]}")"
  corfu_java_home="$(java_home_for "$corfu_jdk")"
  log "building CorfuDB with JDK $corfu_jdk ($corfu_java_home)"

  if [[ ! -d "$CORFU_CLONE_DIR/.git" ]]; then
    log "cloning CorfuDB into $CORFU_CLONE_DIR"
    git clone "$CORFU_REPO" "$CORFU_CLONE_DIR"
  else
    log "reusing existing CorfuDB checkout at $CORFU_CLONE_DIR"
  fi

  # Phase 4 pins this to a commit SHA; today it tracks whatever HEAD was
  # cloned, which is the main remaining reproducibility gap in this script.
  (cd "$CORFU_CLONE_DIR" && JAVA_HOME="$corfu_java_home" mvn -q clean install -DskipTests)
  log "CorfuDB runtime installed into ~/.m2"
}

# ------------------------------------------------------------------- roles

role_client() {
  log "=== role: client ==="

  apt_install cmake maven python3-pip zip pkg-config build-essential git ninja-build

  local jdk java_home
  jdk="$(ensure_jdk "$JDK_VERSION" "${JDK_FALLBACKS[@]}")"
  java_home="$(java_home_for "$jdk")"
  log "node JAVA_HOME -> $java_home (JDK $jdk)"
  if [[ "$jdk" -lt 11 ]]; then
    warn "JDK $jdk cannot load corfu-bridge (Java 11 bytecode). Use --jdk 17 or newer."
  fi

  wire_env "$java_home"

  log "installing python requirements"
  python3 -m pip install --quiet --user -r "$SCRIPT_DIR/requirements.txt"

  log "ensuring $TANK_DIR exists and is writable"
  sudo mkdir -p "$TANK_DIR"
  sudo chmod 777 "$TANK_DIR"

  # vcpkg is vendored, not a submodule -- `git ls-tree HEAD vcpkg` is a plain
  # tree of ~11.7k committed files and `git submodule status` is empty -- so
  # this fetches nothing today. It is kept for the day something is actually
  # added as a submodule, and skipped when the tree arrived by rsync
  # (bench/ansible/bootstrap.yml -e include_git=false) rather than by clone,
  # where it would otherwise abort the run under `set -e`.
  if [[ -e "$REPO_ROOT/.git" ]]; then
    log "updating git submodules"
    (cd "$REPO_ROOT" && git submodule update --init --recursive)
  else
    log "no .git in $REPO_ROOT (rsync'd tree) -- skipping submodule update"
  fi

  if [[ ! -d "$REPO_ROOT/vcpkg/ports" ]]; then
    die "$REPO_ROOT/vcpkg/ports is missing -- the build cannot resolve dependencies.
    vcpkg is vendored in this repo, so a clone or a full rsync should have brought it.
    If you pushed with bench/ansible/sync.yml, that excludes vcpkg/ by design: use
    bootstrap.yml for the first push to a node."
  fi

  if [[ $DO_CORFU_RUNTIME -eq 1 ]]; then
    install_corfu_runtime
  else
    corfu_runtime_present ||
      warn "--no-corfu-runtime and no org.corfudb:runtime in ~/.m2: a build with -DOZONEDB_ENABLE_CORFU=ON will fail at the corfu-bridge step."
  fi

  if [[ $DO_BUILD -eq 1 ]]; then
    log "building ozonedb"
    bash "$SCRIPT_DIR/build.sh"
  else
    log "--no-build: skipping build.sh"
  fi

  java -version 2>&1 | sed 's/^/[setup]   /'
}

role_corfu_server() {
  log "=== role: corfu-server ==="

  apt_install maven git

  local corfu_jdk corfu_java_home
  corfu_jdk="$(ensure_jdk "$CORFU_JDK_VERSION" "${CORFU_JDK_FALLBACKS[@]}")"
  corfu_java_home="$(java_home_for "$corfu_jdk")"
  log "CorfuDB JDK $corfu_jdk ($corfu_java_home)"

  if [[ ! -d "$CORFU_CLONE_DIR/.git" ]]; then
    log "cloning CorfuDB into $CORFU_CLONE_DIR"
    git clone "$CORFU_REPO" "$CORFU_CLONE_DIR"
  fi
  (cd "$CORFU_CLONE_DIR" && JAVA_HOME="$corfu_java_home" mvn -q clean install -DskipTests)

  # run_ycsb_with_corfu.sh resets run_batch from load on every server start:
  #   rm -rf $CORFU_DATA_DIR/run_batch && cp -r $CORFU_DATA_DIR/load $CORFU_DATA_DIR/run_batch
  # Both must exist; nothing created them before this script.
  log "creating $CORFU_DATA_DIR/{load,run_batch}"
  sudo mkdir -p "$CORFU_DATA_DIR/load" "$CORFU_DATA_DIR/run_batch"
  sudo chmod -R 777 "$CORFU_DATA_DIR"

  log "corfu-server ready. Start it with:"
  log "  cd $CORFU_CLONE_DIR && ./bin/corfu_server -l $CORFU_DATA_DIR/run_batch -s -a <host> 9090"
}

role_minio() {
  log "=== role: minio ==="

  # The corfu base config defaults sstable_backend to s3, so the default
  # benchmark path needs an object store. This was previously documented only
  # as a comment in ycsb.yaml.
  local bucket="$MINIO_BUCKET"
  if [[ -z "$bucket" ]]; then
    bucket="$(python3 - "$REPO_ROOT/bench/scripts/config/ycsb.yaml" <<'PY' 2>/dev/null || true
import sys
try:
    import yaml
    with open(sys.argv[1]) as f:
        print((yaml.safe_load(f).get("s3") or {}).get("bucket", ""))
except Exception:
    print("")
PY
    )"
  fi
  [[ -z "$bucket" ]] && bucket="ozonedb-ycsb"

  if ! command -v minio >/dev/null 2>&1; then
    log "installing minio server"
    sudo curl -fsSL -o /usr/local/bin/minio https://dl.min.io/server/minio/release/linux-amd64/minio
    sudo chmod +x /usr/local/bin/minio
  else
    log "minio already installed"
  fi
  if ! command -v mc >/dev/null 2>&1; then
    log "installing minio client"
    sudo curl -fsSL -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc
    sudo chmod +x /usr/local/bin/mc
  else
    log "mc already installed"
  fi

  sudo mkdir -p "$MINIO_DATA_DIR"
  sudo chmod 777 "$MINIO_DATA_DIR"

  log "writing systemd unit /etc/systemd/system/minio.service"
  sudo tee /etc/systemd/system/minio.service >/dev/null <<EOF
[Unit]
Description=MinIO (ozonedb sstable store)
After=network-online.target
Wants=network-online.target

[Service]
User=$USER
Environment=MINIO_ROOT_USER=$MINIO_USER
Environment=MINIO_ROOT_PASSWORD=$MINIO_PASSWORD
ExecStart=/usr/local/bin/minio server $MINIO_DATA_DIR --address :$MINIO_PORT --console-address :$MINIO_CONSOLE_PORT
Restart=always
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

  sudo systemctl daemon-reload
  sudo systemctl enable --now minio

  log "waiting for minio on 127.0.0.1:$MINIO_PORT"
  local i
  for i in $(seq 1 30); do
    (exec 3<>/dev/tcp/127.0.0.1/"$MINIO_PORT") >/dev/null 2>&1 && break
    sleep 1
  done

  mc alias set ozonedb-local "http://127.0.0.1:$MINIO_PORT" "$MINIO_USER" "$MINIO_PASSWORD" >/dev/null
  if mc ls "ozonedb-local/$bucket" >/dev/null 2>&1; then
    log "bucket '$bucket' already exists"
  else
    log "creating bucket '$bucket'"
    mc mb "ozonedb-local/$bucket"
  fi

  log "minio ready. Point s3.endpoint in ycsb.yaml at http://<this-host>:$MINIO_PORT"
}

# -------------------------------------------------------------------- main

log "repo: $REPO_ROOT"
log "roles: ${ROLES[*]}"

for role in "${ROLES[@]}"; do
  case "$role" in
  client) role_client ;;
  corfu-server) role_corfu_server ;;
  minio) role_minio ;;
  esac
done

log "done. Open a new shell, or run: . $ENV_FILE"
