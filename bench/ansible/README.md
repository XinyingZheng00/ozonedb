# Pushing code to the CloudLab nodes

`sync.yml` replaces running `sync-ozonedb.sh` once per host. Every host syncs
concurrently, each over a single reused SSH connection, and can rebuild in the
same pass.

## One-time

```bash
pip install ansible                                    # bundles ansible.posix
# or: pip install ansible-core && ansible-galaxy collection install -r requirements.yml
```

## Use

Run from this directory — Ansible only reads `ansible.cfg` from the cwd.

```bash
cd $OZONEDB_HOME/bench/ansible

ansible-playbook sync.yml                  # push the working tree to every host
ansible-playbook sync.yml -e build=true    # push, then run build.sh on each host
ansible-playbook sync.yml --check --diff   # dry run (rsync -n); see what would move
ansible-playbook sync.yml --limit amd172.utah.cloudlab.us
ansible-playbook sync.yml -e delete=true   # mirror: prune remote-only files too
ansible -m ping cloudlab                   # is everything reachable?
```

Sibling repos ride along:

```bash
ansible-playbook sync.yml \
  -e '{"extra_repos":[{"src":"../../../slatedb/","dest":"slatedb"}]}'
```

`src` is relative to this directory (or absolute); `dest` is relative to the
remote user's home (or absolute).

## Where the hosts come from

`inventory.py` reads `bench/scripts/config/ycsb.yaml` —
`cloudlab.{hosts,ssh_user,ssh_private_key_path}` — so Ansible and
`run_multinode_ycsb.py` can never disagree about which nodes are in the
experiment. After a CloudLab re-swap, edit `ycsb.yaml` and nothing else.

```bash
./inventory.py --list       # what Ansible sees
```

Groups: `cloudlab`, and `clients` as an alias for it.

## What gets excluded

The exclude list in `sync.yml` mirrors `sync-ozonedb.sh`: `.git/`, `vcpkg/`,
`build/`, `target/`, `bench/results/`, editor and agent scratch, and build
artifacts.

**This syncs a working tree; it does not bootstrap a node.** Because `.git/`
and `vcpkg/` stay local, the destination must already be a clone with its
submodules initialised — run `setup.sh --role client` there first. After that,
`sync.yml` is the fast inner loop.

## Destination

Each host is probed for `OZONEDB_HOME` via a login shell (the same thing
`run_multinode_ycsb.py` does), and the tree lands there. Hosts that don't have
it set yet fall back to `~/ozonedb`. Override with
`-e remote_repo_path=/abs/path`, which also skips the probe.

## Notes

- Result files still come back through `run_multinode_ycsb.py`'s own `scp`
  pull. This playbook only handles the outbound direction.
- With many hosts on a slow uplink, the WAN transfer is the floor: the tree is
  uploaded once per host. Syncing to one node and having it fan out over
  CloudLab's internal network would beat this, but it needs the nodes to hold
  each other's keys.
