#!/usr/bin/env python3
"""Dynamic Ansible inventory backed by bench/scripts/config/ycsb.yaml.

Ansible reads the same host list the benchmark orchestrators already read, so
re-swapping a CloudLab experiment means editing exactly one place --
cloudlab.{hosts,ssh_user,ssh_private_key_path} -- and both run_multinode_ycsb.py
and every playbook here follow along.

    ./inventory.py --list
    ./inventory.py --host amd172.utah.cloudlab.us

Groups:
    cloudlab   every entry in cloudlab.hosts
    clients    alias for cloudlab (the YCSB writer nodes)

Override the config path with OZONEDB_YCSB_CONFIG.
"""

import argparse
import json
import os
import sys

import yaml

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.normpath(
    os.path.join(_HERE, "..", "scripts", "config", "ycsb.yaml")
)

# Matches ssh_base() in bench/scripts/local/run_multinode_ycsb.py. CloudLab
# re-images nodes constantly, so a pinned host key is a guaranteed false alarm.
SSH_COMMON_ARGS = (
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    "-o LogLevel=ERROR"
)


def load_config():
    path = os.environ.get("OZONEDB_YCSB_CONFIG", DEFAULT_CONFIG)
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def build_inventory():
    config = load_config()
    cloudlab = config.get("cloudlab") or {}

    default_user = cloudlab.get("ssh_user")
    key = (
        cloudlab.get("ssh_private_key_path")
        or cloudlab.get("ssh_private_key")
        or "~/.ssh/id_rsa"
    )
    key = os.path.expanduser(str(key))

    hostvars = {}
    names = []
    for raw in cloudlab.get("hosts") or []:
        entry = str(raw).strip()
        if not entry:
            continue
        # The ycsb.yaml example comment shows "user@host" entries, so honour a
        # per-host user override rather than producing "user@user@host".
        user, _, host = entry.rpartition("@")
        user = user or default_user
        if not user:
            sys.stderr.write(
                "inventory.py: no ssh user for %s -- set cloudlab.ssh_user\n" % host
            )
            continue
        names.append(host)
        hostvars[host] = {
            "ansible_host": host,
            "ansible_user": user,
            "ansible_ssh_private_key_file": key,
            "ansible_ssh_common_args": SSH_COMMON_ARGS,
        }

    if not names:
        sys.stderr.write(
            "inventory.py: cloudlab.hosts is empty in %s\n"
            % os.environ.get("OZONEDB_YCSB_CONFIG", DEFAULT_CONFIG)
        )

    return {
        "cloudlab": {"hosts": names},
        "clients": {"children": ["cloudlab"]},
        "_meta": {"hostvars": hostvars},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--host")
    args = parser.parse_args()

    inventory = build_inventory()
    if args.host:
        print(json.dumps(inventory["_meta"]["hostvars"].get(args.host, {}), indent=2))
    else:
        print(json.dumps(inventory, indent=2))


if __name__ == "__main__":
    main()
