# Pushing code to the CloudLab nodes

Two playbooks. `bootstrap.yml` turns a freshly provisioned node into a working
one, straight from your local tree — no `git clone`, no GitHub credentials on the
node. `sync.yml` is the fast inner loop after that.

## One-time

```bash
pip install ansible                                    # bundles ansible.posix
# or: pip install ansible-core && ansible-galaxy collection install -r requirements.yml
```

## After a CloudLab swap

Update `cloudlab.hosts` in `bench/scripts/config/ycsb.yaml`, then:

```bash
cd $OZONEDB_HOME/bench/ansible
ansible -m ping cloudlab             # reachable?

# client nodes: push the tree, then provision
ansible-playbook bootstrap.yml

# the log/store node
ansible-playbook bootstrap.yml --limit amd171.utah.cloudlab.us \
  -e '{"setup_roles":["corfu-server","minio"]}'
```

`bootstrap.yml` pushes the working tree **including `vcpkg/`** and then runs
`bench/scripts/setup.sh --role client` on each host (detached via `async`, since
the first run builds CorfuDB into `~/.m2`). Useful flags:

```bash
-e run_setup=false                   # push only, don't provision
-e include_git=false                 # skip .git/ (40 of the 59 MB)
-e '{"setup_roles":["client"]}'      # roles to pass to setup.sh
-e setup_extra_args="--jdk 17"       # anything else setup.sh takes
```

## Every push after that

```bash
ansible-playbook sync.yml                  # push changed files to every host
ansible-playbook sync.yml -e build=true    # push, then run build.sh on each host
ansible-playbook sync.yml --check --diff   # dry run (rsync -n); see what would move
ansible-playbook sync.yml --limit amd172.utah.cloudlab.us
ansible-playbook sync.yml -e delete=true   # mirror: prune remote-only files too
```

Run everything from this directory — Ansible only reads `ansible.cfg` from the
cwd.

## Payload

Measured with `rsync -avn --stats` against this tree:

| | files | bytes |
|---|---|---|
| `bootstrap.yml`, no `.git` | 14867 | 19 MB |
| `bootstrap.yml`, with `.git` | 15616 | 59 MB |
| `sync.yml` (incremental) | 442 | 2.2 MB |

`sync.yml` excludes `vcpkg/` and `.git/`, which is the whole difference — there
is no point re-sending 11.3k unchanged vendored port files on every edit.

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

Both playbooks share one list, in `group_vars/all.yml`. `rsync_excludes_base`
drops what is never useful on a node — `build/`, `target/`, `bench/results/`,
`__pycache__`, compiled objects, editor and agent scratch. On top of that:

- `sync.yml` adds `.git/` and `vcpkg/` (`rsync_excludes_incremental`)
- `bootstrap.yml` keeps `vcpkg/` but drops the parts of it that are local to
  your machine (`rsync_excludes_vcpkg_local`): `vcpkg/vcpkg` — which is a
  **Mach-O binary** and would be actively broken on Linux — plus
  `vcpkg/downloads/` (63 MB of re-fetchable tarballs) and the per-platform
  `installed/`, `packages/`, `buildtrees/`.

### Why bootstrap can skip the clone

`vcpkg` is **vendored, not a submodule**. `git ls-tree HEAD vcpkg` is a plain
`040000 tree` of ~11.7k committed files and `git submodule status` is empty —
`.gitmodules` is vestigial. So the ports tree arrives with an rsync just as it
would with a clone, nothing on the node has to reach GitHub, and your PAT never
leaves your laptop.

`.git/` is optional (`-e include_git=false`) for the same reason: no build step
needs it. `setup.sh` skips its `git submodule update` when `.git` is absent
rather than aborting under `set -e`, and hard-fails early if `vcpkg/ports` is
missing — which is what you would hit by trying to bootstrap a node with
`sync.yml`.

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
