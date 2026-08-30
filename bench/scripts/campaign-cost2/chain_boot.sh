#!/bin/bash
# New experiment 2026-08-29 (ozonedb-cass, amd197 + amd189..amd200): bootstrap every node
# from the worktree, then the cassandra role on the server. The SSD layout was done by hand
# before this (layout.py in the job tmp dir): /tank/ssd on the server with the MinIO bind
# mount and the /mnt/corfu + /tank/cassandra links, /tank/cache on every client.
set -euo pipefail
trap 'echo "[chain $(date "+%F %T")] CHAIN-FAILED rc=$? line $LINENO"' ERR
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
export ANSIBLE_CONFIG=$OZONEDB_HOME/bench/ansible/ansible.cfg
cd "$OZONEDB_HOME/bench/ansible"
SRV=oliverr3@amd197.utah.cloudlab.us
step() { echo "[chain $(date '+%F %T')] $*"; }

step "bootstrap: push tree + setup.sh on all 9 nodes (clients: client; amd197: corfu-server + minio)"
ansible-playbook bootstrap.yml -e target=nodes -e setup_timeout=10800
step "cassandra role on the server"
ssh -o BatchMode=yes "$SRV" "bash -lc 'cd ~/ozonedb && bash bench/scripts/setup.sh --role cassandra' 2>&1 | tail -20"
step "post-check: build artefacts, tier mounts, server services"
ansible -m shell -a "bash -lc 'ls -la ~/ozonedb/build/libOzoneDB.so ~/ozonedb/build/libozonedb.so ~/ozonedb/build/corfu_native_probe 2>&1 | tail -3; mountpoint /tank/cache; git -C ~/ozonedb log --oneline -1 2>/dev/null || echo no-git'" clients
ssh -o BatchMode=yes "$SRV" "bash -lc 'systemctl is-active minio; mc ls ozonedb-local/ | head; ls -ld /mnt/corfu/load /mnt/corfu/run_batch /tank/cassandra/current /tank/cassandra/cassandra_ctl.sh; df -h /tank/ssd | tail -1'"
step "CHAIN-DONE"
