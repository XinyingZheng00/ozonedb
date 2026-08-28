#!/usr/bin/env bash
# setup_disk_cache.sh -- format one unused local SSD as the OzoneDB disk-cache
# tier and mount it (bench/PLAN-disk-cache.md).
#
# Idempotent: a device that already carries the label is only mounted; a
# mount that is already up is left alone. The script formats WHOLE DISKS
# only and refuses a disk with partitions, a foreign filesystem signature
# (unless --force), a mount, or the root filesystem.
#
#   bash bench/scripts/setup_disk_cache.sh --list
#   bash bench/scripts/setup_disk_cache.sh --device /dev/sdb
#   bash bench/scripts/setup_disk_cache.sh            # auto: the single unused SSD
set -euo pipefail

DEVICE=""
MOUNT="/tank/cache"
LABEL="ozcache"
FSTYPE="ext4"
FORCE=0

usage() {
  cat <<EOF
Usage: $0 [--device /dev/sdX] [--mount DIR] [--label NAME] [--force] [--list]

  --device DEV   whole disk to format (default: the single non-rotational disk
                 that has no partitions, no filesystem signature and no mount,
                 and is not the root disk; the script refuses to guess between two)
  --mount DIR    mount point (default $MOUNT)
  --label NAME   filesystem label (default $LABEL); the fstab entry uses LABEL=
  --force        wipe a device that carries another filesystem signature
  --list         print lsblk and exit
EOF
}

log() { echo "[disk-cache] $*"; }
die() { echo "[disk-cache] error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --device) DEVICE="$2"; shift 2 ;;
    --mount) MOUNT="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --list) lsblk -o NAME,SIZE,TYPE,ROTA,FSTYPE,LABEL,MOUNTPOINT; exit 0 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (try --help)" ;;
  esac
done

# 1. Already mounted from our label: nothing to do.
if findmnt -n "$MOUNT" >/dev/null 2>&1; then
  src=$(findmnt -n -o SOURCE "$MOUNT")
  if [[ "$(sudo blkid -s LABEL -o value "$src" 2>/dev/null || true)" == "$LABEL" ]]; then
    log "$MOUNT is already mounted from $src (label $LABEL) -- nothing to do"
    sudo chmod 777 "$MOUNT"
    df -h "$MOUNT"
    exit 0
  fi
  die "$MOUNT is mounted from $src, which does not carry the label $LABEL"
fi

# 2. Pick the device.
root_disk=$(lsblk -no PKNAME "$(findmnt -n -o SOURCE /)")
if [[ -z "$DEVICE" ]]; then
  candidates=()
  while read -r name type rota; do
    [[ "$type" == "disk" && "$rota" == "0" && "$name" != "$root_disk" ]] || continue
    [[ "$(lsblk -n "/dev/$name" | wc -l)" -eq 1 ]] || continue
    [[ -z "$(sudo blkid -o value -s TYPE "/dev/$name" 2>/dev/null || true)" ]] || continue
    candidates+=("/dev/$name")
  done < <(lsblk -dn -o NAME,TYPE,ROTA)
  if [[ ${#candidates[@]} -ne 1 ]]; then
    lsblk -o NAME,SIZE,TYPE,ROTA,FSTYPE,LABEL,MOUNTPOINT
    die "expected exactly one unused SSD, found ${#candidates[@]}: ${candidates[*]:-none}. Pass --device."
  fi
  DEVICE="${candidates[0]}"
fi
[[ -b "$DEVICE" ]] || die "$DEVICE is not a block device"
[[ "$(basename "$DEVICE")" != "$root_disk" ]] || die "$DEVICE holds the root filesystem"
if lsblk -no MOUNTPOINT "$DEVICE" 2>/dev/null | grep -q .; then
  die "$DEVICE (or a child) is mounted. Unmount it first."
fi
[[ "$(lsblk -n "$DEVICE" | wc -l)" -eq 1 ]] || die "$DEVICE has partitions; this script formats whole disks only"

# 3. Format unless our label is already there.
existing_label=$(sudo blkid -s LABEL -o value "$DEVICE" 2>/dev/null || true)
existing_type=$(sudo blkid -s TYPE -o value "$DEVICE" 2>/dev/null || true)
if [[ "$existing_label" == "$LABEL" && "$existing_type" == "$FSTYPE" ]]; then
  log "$DEVICE already carries $FSTYPE label $LABEL -- keeping the filesystem"
elif [[ -n "$existing_type" && $FORCE -ne 1 ]]; then
  die "$DEVICE has a $existing_type filesystem (label '${existing_label}'). Pass --force to wipe it."
else
  log "formatting $DEVICE as $FSTYPE (label $LABEL)"
  sudo wipefs -a "$DEVICE"
  sudo mkfs.ext4 -q -F -L "$LABEL" -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$DEVICE"
fi

# 4. fstab by label, mount, permissions.
sudo mkdir -p "$MOUNT"
if ! grep -qE "^LABEL=${LABEL}[[:space:]]" /etc/fstab; then
  echo "LABEL=$LABEL $MOUNT $FSTYPE defaults,noatime,nofail 0 2" | sudo tee -a /etc/fstab >/dev/null
  log "fstab entry added"
fi
sudo mount "$MOUNT"
sudo chmod 777 "$MOUNT"
log "mounted $DEVICE at $MOUNT"
df -h "$MOUNT"
