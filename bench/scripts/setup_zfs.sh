#!/usr/bin/env bash
#
# Create the ZFS pool that backs /tank for local experiments.
#
#   bash bench/scripts/setup_zfs.sh --device /dev/sda6
#   bash bench/scripts/setup_zfs.sh --device /dev/sda6 --pool tank --force
#
# The previous version ran a bare `sudo fdisk /dev/sda`, which is interactive
# and so could never run unattended, and hardcoded /dev/sda6. Nothing invoked
# it. This version takes the device as an argument, refuses to guess, and
# no-ops when the pool already exists.
#
# It does NOT partition anything. Partitioning is destructive and
# machine-specific -- create the partition yourself (fdisk/parted/sgdisk), then
# point this at it. `--list` shows candidate block devices.

set -euo pipefail

POOL="tank"
DEVICE=""
MOUNT=""
FORCE=0

usage() {
  cat <<EOF
Usage: $(basename "$0") --device /dev/sdXN [options]

  --device PATH   block device or partition to hand to zpool (required)
  --pool NAME     pool name (default: $POOL); mountpoint is /<name>
  --mount PATH    override the mountpoint (default: /<pool>)
  --force         pass -f to zpool create (device has a stale label/fs)
  --list          list block devices and exit
  -h | --help
EOF
}

log() { printf '\033[1;36m[zfs]\033[0m %s\n' "$*"; }
die() {
  printf '\033[1;31m[zfs:error]\033[0m %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --device)
    DEVICE="$2"
    shift 2
    ;;
  --pool)
    POOL="$2"
    shift 2
    ;;
  --mount)
    MOUNT="$2"
    shift 2
    ;;
  --force)
    FORCE=1
    shift
    ;;
  --list)
    lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT
    exit 0
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *) die "unknown option: $1 (try --help)" ;;
  esac
done

MOUNT="${MOUNT:-/$POOL}"

log "installing zfsutils-linux"
if ! dpkg -s zfsutils-linux >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y zfsutils-linux
fi

if sudo zpool list -H -o name 2>/dev/null | grep -qx "$POOL"; then
  log "pool '$POOL' already exists -- nothing to do"
  sudo zfs list "$POOL" || true
  sudo chmod 777 "$MOUNT" || true
  exit 0
fi

[[ -n "$DEVICE" ]] || {
  echo "No --device given, and pool '$POOL' does not exist." >&2
  echo "Pick a partition from below, then re-run with --device." >&2
  echo >&2
  lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT >&2
  exit 1
}
[[ -b "$DEVICE" ]] || die "$DEVICE is not a block device"

# Refuse to eat a mounted or in-use device.
if lsblk -no MOUNTPOINT "$DEVICE" 2>/dev/null | grep -q .; then
  die "$DEVICE (or a child) is mounted. Unmount it first."
fi

create_args=(create)
[[ $FORCE -eq 1 ]] && create_args+=(-f)
create_args+=(-m "$MOUNT" "$POOL" "$DEVICE")

log "zpool ${create_args[*]}"
sudo zpool "${create_args[@]}"

sudo zfs list "$POOL"
df -h "$MOUNT"
sudo chmod 777 "$MOUNT"
log "pool '$POOL' created and mounted at $MOUNT"
