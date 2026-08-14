#!/usr/bin/env python3
"""The one loader for bench/scripts/config/ycsb.yaml.

`nodes:` is the single source of truth for every address in the cluster. Each
node is named once, with the two addresses that are NOT interchangeable:

    ssh   how the orchestrator reaches it -- a public CloudLab name
    lan   how other nodes reach it -- the internal 10.10.1.x address

Conflating those is what broke: `corfu_server -a <addr>` binds the address it is
given, so it must be the *lan* one for clients to connect, while the sweep
script has to *ssh* to the same box, which from a laptop needs the public one.
The two sweep wrappers each carried their own hardcoded copy of the corfu host,
and had already drifted from corfu.endpoint (10.10.1.2 vs 10.10.1.3) -- which
would start Corfu on the MinIO node while every client dialled somewhere else.

So the derived values are computed here, exactly once, and materialised back
into the config dict under the keys the existing consumers already read:

    cloudlab.hosts                 <- [c.ssh for c in nodes.clients]
    cloudlab.ssh_user              <- nodes.ssh_user
    cloudlab.ssh_private_key_path  <- nodes.ssh_private_key_path
    corfu.endpoint                 <- nodes.log.lan   : corfu.port
    s3.endpoint                    <- nodes.store.lan : s3.port  (with scheme)

That is why callers only had to swap `yaml.safe_load(f)` for `load(path)`;
everything downstream still reads corfu_settings["endpoint"] as before.

Also usable from shell:

    python3 ycsb_config.py --get corfu.endpoint
    python3 ycsb_config.py --ssh-target log        # user@public-name
    python3 ycsb_config.py --node log --field lan  # bind address
    python3 ycsb_config.py --check                 # validate and print the plan
"""

import argparse
import json
import os
import sys

import yaml

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(_HERE, "config", "ycsb.yaml")

DEFAULT_CORFU_PORT = 9090
DEFAULT_S3_PORT = 9000
DEFAULT_S3_SCHEME = "http"

# Roles that name exactly one machine. `clients` is the list role.
SINGLETON_ROLES = ("log", "store")


class ConfigError(ValueError):
    """Raised for a config that would produce a silently wrong benchmark."""


def _as_node(entry, where):
    """Accept either `- host.example` or `- {ssh: host.example, lan: 10.0.0.1}`."""
    if isinstance(entry, str):
        return {"ssh": entry.strip()}
    if isinstance(entry, dict):
        return entry
    raise ConfigError(f"{where}: expected a hostname or a mapping, got {entry!r}")


def ssh_addr(node, where):
    """Address to SSH to. Falls back to lan, which is right when you orchestrate
    from inside the CloudLab LAN and is what the scripts did before `nodes:`."""
    addr = (node.get("ssh") or node.get("lan") or "").strip()
    if not addr:
        raise ConfigError(f"{where}: needs an `ssh:` or `lan:` address")
    return addr


def lan_addr(node, where):
    """Address other nodes use. Falls back to ssh for a single-machine setup."""
    addr = (node.get("lan") or node.get("ssh") or "").strip()
    if not addr:
        raise ConfigError(f"{where}: needs a `lan:` or `ssh:` address")
    return addr


def _reject_derived(cfg, dotted):
    """A hand-written value that `nodes:` also computes is exactly the drift
    this module exists to prevent, so refuse rather than silently pick one."""
    section, _, key = dotted.partition(".")
    if isinstance(cfg.get(section), dict) and key in cfg[section]:
        raise ConfigError(
            f"{dotted} is derived from nodes: and must not be set by hand. "
            f"Delete it; the value comes from nodes.{'log' if section == 'corfu' else 'store'}."
            if section in ("corfu", "s3")
            else f"{dotted} is derived from nodes.clients and must not be set by hand. Delete it."
        )


def derive(cfg):
    """Materialise the derived addresses. No-op for a config without `nodes:`."""
    nodes = cfg.get("nodes")
    if not nodes:
        return cfg

    for dotted in ("cloudlab.hosts", "corfu.endpoint", "s3.endpoint"):
        _reject_derived(cfg, dotted)

    clients = [
        _as_node(e, f"nodes.clients[{i}]")
        for i, e in enumerate(nodes.get("clients") or [])
    ]
    if not clients:
        raise ConfigError("nodes.clients is empty -- there is nothing to run YCSB on.")

    ssh_user = nodes.get("ssh_user")
    if not ssh_user:
        raise ConfigError("nodes.ssh_user is not set.")

    cfg["cloudlab"] = {
        "ssh_user": ssh_user,
        "ssh_private_key_path": nodes.get("ssh_private_key_path") or "~/.ssh/id_rsa",
        "hosts": [
            ssh_addr(c, f"nodes.clients[{i}]") for i, c in enumerate(clients)
        ],
    }

    if isinstance(cfg.get("corfu"), dict):
        log = _as_node(nodes.get("log") or {}, "nodes.log")
        port = cfg["corfu"].get("port", DEFAULT_CORFU_PORT)
        cfg["corfu"]["endpoint"] = f"{lan_addr(log, 'nodes.log')}:{port}"

    if isinstance(cfg.get("s3"), dict):
        store = _as_node(nodes.get("store") or {}, "nodes.store")
        port = cfg["s3"].get("port", DEFAULT_S3_PORT)
        scheme = cfg["s3"].get("scheme", DEFAULT_S3_SCHEME)
        cfg["s3"]["endpoint"] = f"{scheme}://{lan_addr(store, 'nodes.store')}:{port}"

    return cfg


def load(path=None):
    path = path or os.environ.get("OZONEDB_YCSB_CONFIG") or DEFAULT_CONFIG_PATH
    with open(path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    return derive(cfg)


def node(cfg, role):
    """The `nodes:` entry for a singleton role, as a mapping."""
    nodes = cfg.get("nodes") or {}
    if role not in nodes:
        raise ConfigError(
            f"nodes.{role} is not defined (have: {sorted(k for k in nodes if k in SINGLETON_ROLES)})"
        )
    return _as_node(nodes[role], f"nodes.{role}")


def ssh_target(cfg, role):
    """user@host for SSHing to a singleton role."""
    user = (cfg.get("nodes") or {}).get("ssh_user")
    if not user:
        raise ConfigError("nodes.ssh_user is not set.")
    return f"{user}@{ssh_addr(node(cfg, role), f'nodes.{role}')}"


def get(cfg, dotted):
    cur = cfg
    for part in dotted.split("."):
        if isinstance(cur, list):
            cur = cur[int(part)]
        elif isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            raise ConfigError(f"no such config key: {dotted}")
    return cur


def _plan(cfg):
    out = []
    nodes = cfg.get("nodes") or {}
    for role in SINGLETON_ROLES:
        if role in nodes:
            n = _as_node(nodes[role], f"nodes.{role}")
            out.append(
                f"{role:<8} ssh={ssh_addr(n, role):<28} lan={lan_addr(n, role)}"
            )
    for i, c in enumerate(nodes.get("clients") or []):
        n = _as_node(c, f"nodes.clients[{i}]")
        out.append(
            f"{'client'+str(i):<8} ssh={ssh_addr(n, 'c'):<28} lan={n.get('lan', '-')}"
        )
    out.append("")
    out.append(f"corfu.endpoint  = {get(cfg, 'corfu.endpoint')}   (clients dial this; also corfu_server -a)")
    out.append(f"s3.endpoint     = {get(cfg, 's3.endpoint')}")
    out.append(f"cloudlab.hosts  = {get(cfg, 'cloudlab.hosts')}")
    return "\n".join(out)


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--config", default=None)
    p.add_argument("--get", metavar="DOTTED", help="print one value, e.g. corfu.endpoint")
    p.add_argument("--node", metavar="ROLE", help="a role under nodes: (log, store)")
    p.add_argument("--field", default="lan", help="with --node: ssh or lan (default lan)")
    p.add_argument("--ssh-target", metavar="ROLE", help="print user@host for a role")
    p.add_argument("--check", action="store_true", help="validate and print the resolved plan")
    p.add_argument("--json", action="store_true", help="dump the fully resolved config")
    args = p.parse_args()

    try:
        cfg = load(args.config)
        if args.get:
            v = get(cfg, args.get)
            print(json.dumps(v) if isinstance(v, (list, dict)) else v)
        elif args.ssh_target:
            print(ssh_target(cfg, args.ssh_target))
        elif args.node:
            n = node(cfg, args.node)
            print(ssh_addr(n, args.node) if args.field == "ssh" else lan_addr(n, args.node))
        elif args.json:
            print(json.dumps(cfg, indent=2, default=str))
        else:
            print(_plan(cfg))
    except (ConfigError, OSError) as e:
        sys.stderr.write(f"ycsb_config: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
