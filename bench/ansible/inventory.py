#!/usr/bin/env python3
"""Dynamic Ansible inventory backed by bench/scripts/config/ycsb.yaml.

Hosts come from the `nodes:` block, so Ansible, run_multinode_ycsb.py and the
sweep wrappers all read one list. A CloudLab re-swap is a single edit there.

    ./inventory.py --list
    ./inventory.py --host ms0917.utah.cloudlab.us

Groups:
    clients    the YCSB writer nodes (nodes.clients)
    log        the shared-log / CorfuDB node (nodes.log)
    store      the MinIO / SSTable node (nodes.store)
    cloudlab   alias for clients, so `hosts: cloudlab` playbooks keep working
    nodes      every machine above

Each host also carries `lan_address` -- the internal 10.10.1.x address other
nodes use, which is NOT what Ansible connects over. Playbooks that need to tell
a service where to bind or dial should use that, never ansible_host.

Override the config path with OZONEDB_YCSB_CONFIG.
"""

import argparse
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "scripts"))

from ycsb_config import ConfigError, load, ssh_addr  # noqa: E402

# Matches ssh_base() in bench/scripts/local/run_multinode_ycsb.py. CloudLab
# re-images nodes constantly, so a pinned host key is a guaranteed false alarm.
SSH_COMMON_ARGS = (
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    "-o LogLevel=ERROR"
)


def build_inventory():
    config = load()
    nodes = config.get("nodes") or {}

    default_user = nodes.get("ssh_user")
    key = os.path.expanduser(str(nodes.get("ssh_private_key_path") or "~/.ssh/id_rsa"))

    hostvars = {}
    groups = {"clients": [], "log": [], "store": []}

    def add(entry, group, where):
        if isinstance(entry, str):
            entry = {"ssh": entry}
        addr = ssh_addr(entry, where)
        # ycsb.yaml's own example shows "user@host" entries, so honour a
        # per-host user rather than producing "user@user@host".
        user, _, host = addr.rpartition("@")
        user = user or default_user
        if not user:
            raise ConfigError(f"{where}: no ssh user (set nodes.ssh_user)")
        groups[group].append(host)
        hostvars[host] = {
            "ansible_host": host,
            "ansible_user": user,
            "ansible_ssh_private_key_file": key,
            "ansible_ssh_common_args": SSH_COMMON_ARGS,
            # For playbooks that configure a bind/dial address. Not a connection
            # setting -- Ansible reaches the node via ansible_host.
            "lan_address": entry.get("lan") or host,
            "ozonedb_role": {"clients": "client", "log": "corfu-server",
                             "store": "minio"}[group],
        }

    for i, entry in enumerate(nodes.get("clients") or []):
        add(entry, "clients", f"nodes.clients[{i}]")
    for role in ("log", "store"):
        if nodes.get(role):
            add(nodes[role], role, f"nodes.{role}")

    if not groups["clients"]:
        sys.stderr.write("inventory.py: nodes.clients is empty\n")

    inv = {g: {"hosts": h} for g, h in groups.items() if h}
    inv["cloudlab"] = {"children": ["clients"]}
    inv["nodes"] = {"children": [g for g in groups if groups[g]]}
    inv["_meta"] = {"hostvars": hostvars}
    return inv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--host")
    args = parser.parse_args()

    try:
        inventory = build_inventory()
    except (ConfigError, OSError) as e:
        sys.stderr.write(f"inventory.py: {e}\n")
        sys.exit(1)

    if args.host:
        print(json.dumps(inventory["_meta"]["hostvars"].get(args.host, {}), indent=2))
    else:
        print(json.dumps(inventory, indent=2))


if __name__ == "__main__":
    main()
