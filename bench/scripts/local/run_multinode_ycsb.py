import sys
import argparse
import os
import re
import shlex
import subprocess
import threading
import time
from datetime import datetime

import yaml

from load_local_ycsb_multiproc import write_aggregate

# bench/scripts is one level up. ycsb_config.derive() resolves the `nodes:`
# block into the cloudlab.hosts / corfu.endpoint / s3.endpoint keys read below,
# so those are computed in exactly one place.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ycsb_config import derive as _derive_addresses

"""
Multi-node YCSB orchestrator. SSHes to each host and invokes
run_local_ycsb_multiproc.py with the slice of writers assigned to that
host. After all hosts finish, per-writer result files are pulled back
to the orchestrator and a cross-node aggregate is produced.

Writer indices are global across the whole cluster:
  host i -> writers [i * writers_per_host, (i+1) * writers_per_host)
  total_writers     = num_hosts * writers_per_host

For ozonedb-corfu all writers share one Corfu stream — this is the
intended shape for a real multi-node distributed shared-log run. For
other backends each writer reads its own cached dir on its own host;
the loader (load_local_ycsb_multiproc.py) must already have been run
on every host so the cached_data-* dirs corresponding to that host's
writer indices exist locally.

All hosts share an OZONEDB_RUN_TAG so each one writes its results to
$OZONEDB_HOME/bench/results/local/<tag>/. The orchestrator pulls
those into the same tag-named dir locally and aggregates.

Hosts/SSH come from cloudlab.{hosts,ssh_user,ssh_private_key_path} in
ycsb.yaml (overridable via --hosts / --ssh_user / --ssh_key). Per-node
slice size and pass-throughs come from local.run.writers_per_host (or
--writers_per_host) plus optional --workloads / --trial.
"""

ozonedb_home = os.environ.get("OZONEDB_HOME")


def _expand_path(p):
    return os.path.expanduser(p) if p else p


def resolve_hosts(config, hosts_arg):
    if hosts_arg:
        return [h.strip() for h in hosts_arg.split(",") if h.strip()]
    cloudlab = config.get("cloudlab") or {}
    hosts = [h.strip() for h in (cloudlab.get("hosts") or []) if h and str(h).strip()]
    if not hosts:
        raise ValueError(
            "No hosts configured. Set cloudlab.hosts in ycsb.yaml or pass --hosts."
        )
    return hosts


def resolve_ssh_user(config, ssh_user_arg):
    if ssh_user_arg:
        return ssh_user_arg
    cloudlab = config.get("cloudlab") or {}
    user = cloudlab.get("ssh_user")
    if not user:
        raise ValueError(
            "No ssh user. Set cloudlab.ssh_user in ycsb.yaml or pass --ssh_user."
        )
    return user


def resolve_ssh_key(config, ssh_key_arg):
    p = ssh_key_arg
    if not p:
        cloudlab = config.get("cloudlab") or {}
        p = cloudlab.get("ssh_private_key_path") or cloudlab.get("ssh_private_key")
    return _expand_path(p or "~/.ssh/id_rsa")


def ssh_base(target, ssh_key):
    return [
        "ssh",
        "-i", ssh_key,
        "-oStrictHostKeyChecking=no",
        "-oUserKnownHostsFile=/dev/null",
        "-oLogLevel=ERROR",
        target,
    ]


def remote_ozonedb_home(target, ssh_key):
    """Read OZONEDB_HOME on the remote (sourced from a login shell)."""
    cmd = ssh_base(target, ssh_key) + ["bash -lc 'echo $OZONEDB_HOME'"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True)
    line = (out.stdout or "").strip().splitlines()
    home = line[-1] if line else ""
    if not home:
        raise RuntimeError(
            f"OZONEDB_HOME not set on {target} (got: {out.stdout!r} / {out.stderr!r})"
        )
    return home


def stream_remote(target, ssh_key, remote_command, log_path):
    """Run a command on the remote, streaming combined output to log_path."""
    ssh_cmd = ssh_base(target, ssh_key) + [
        f"bash -lc {shlex.quote(remote_command)}"
    ]
    print(f"[{target}] launching: {remote_command}")
    with open(log_path, "a") as f:
        f.write(
            f"# {datetime.now().isoformat()} run_multinode_ycsb -> {target}\n"
            f"# command: {remote_command}\n"
        )
        f.flush()
        proc = subprocess.Popen(ssh_cmd, stdout=f, stderr=subprocess.STDOUT)
        rc = proc.wait()
    print(f"[{target}] exit={rc}")
    return rc


def fetch_results(target, ssh_key, remote_dir, local_dir):
    """scp every entry from remote_dir into local_dir.

    Captures stdout/stderr and the rc instead of suppressing them — a
    silent scp failure is how a partial pull (e.g. one host's
    per-machine aggregate missing) sneaks past cross_node_aggregate and
    produces wrong totals.
    """
    os.makedirs(local_dir, exist_ok=True)
    cmd = [
        "scp",
        "-i", ssh_key,
        "-oStrictHostKeyChecking=no",
        "-oUserKnownHostsFile=/dev/null",
        "-r",
        # `dir/*` (glob) instead of `dir/.` — SFTP-mode scp on OpenSSH 9.x
        # rejects the `.` shortcut with "error: unexpected filename: ." but
        # expands `*` via remote SFTP listing.
        f"{target}:{remote_dir}/*",
        local_dir,
    ]
    print(f"[{target}] {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout.strip():
        print(f"[{target}] scp stdout: {proc.stdout.strip()}")
    if proc.stderr.strip():
        print(f"[{target}] scp stderr: {proc.stderr.strip()}")
    if proc.returncode != 0:
        print(
            f"[{target}] WARNING: scp exited {proc.returncode}; "
            f"some result files from this host may be missing in {local_dir}"
        )
    return proc.returncode


def host_log_name(host, log_dir):
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", host)
    return os.path.join(log_dir, f"{safe}.log")


def build_remote_command(remote_home, run_tag, host_offset, host_writers,
                         total_writers, workloads, trial):
    parts = [
        f"export OZONEDB_RUN_TAG={shlex.quote(run_tag)}",
        f"cd {shlex.quote(remote_home)}",
        " ".join(
            [
                "python3",
                shlex.quote(
                    os.path.join(
                        remote_home, "bench/scripts/local/run_local_ycsb_multiproc.py"
                    )
                ),
                "--num_writers", str(host_writers),
                "--offset", str(host_offset),
                "--total_writers", str(total_writers),
            ]
            + (["--workloads", shlex.quote(workloads)] if workloads else [])
            + (["--trial", str(trial)] if trial is not None else [])
        ),
    ]
    return " && ".join(parts)


_PERWRITER_RE = re.compile(
    r"^(?P<ks>[^-]+)-(?P<opcnt>\d+)-(?P<rc>\d+)-workload(?P<wl>[^-]+)-"
    r"(?P<db>.+?)_w(?P<widx>\d+)of(?P<total>\d+)_t(?P<thread>\d+)"
    r"_trial(?P<trial>\d+)\.result$"
)


def cross_node_aggregate(results_root, total_writers, wall_ms, trial_filter=None):
    """Group per-writer files merged from every node and emit one aggregate
    per (workload, key_size, op_cnt, rec_cnt, db, thread, trial) tuple.

    Validates per-group file count matches total_writers and prints
    missing writer indices loudly — silent under-reporting (e.g. when scp
    drops a host's files) was previously how this produced wrong totals.
    """
    if not os.path.isdir(results_root):
        print(f"[aggregate] {results_root} missing; nothing to do")
        return

    all_entries = sorted(os.listdir(results_root))
    file_count = sum(
        1 for f in all_entries if os.path.isfile(os.path.join(results_root, f))
    )
    print(f"[aggregate] {file_count} files in {results_root}")

    groups = {}
    matched = 0
    for fn in all_entries:
        full = os.path.join(results_root, fn)
        if not os.path.isfile(full):
            continue
        m = _PERWRITER_RE.match(fn)
        if not m:
            continue
        if int(m.group("total")) != total_writers:
            continue
        if trial_filter is not None and int(m.group("trial")) != trial_filter:
            continue
        matched += 1
        key = (
            m.group("ks"), m.group("opcnt"), m.group("rc"),
            m.group("wl"), m.group("db"), m.group("thread"), m.group("trial"),
        )
        groups.setdefault(key, []).append(full)

    print(
        f"[aggregate] per-writer files matched (of{total_writers}, "
        f"trial={trial_filter}): {matched}"
    )
    if not groups:
        print("[aggregate] no per-writer files to aggregate")
        return

    any_incomplete = False
    for (ks, opcnt, rc, wl, db, thread, trial), files in sorted(groups.items()):
        if len(files) != total_writers:
            any_incomplete = True
            present = sorted(
                int(_PERWRITER_RE.match(os.path.basename(p)).group("widx"))
                for p in files
            )
            missing = [i for i in range(total_writers) if i not in present]
            print(
                f"[aggregate] WARNING: workload={wl} db={db} thread={thread} "
                f"trial={trial}: expected {total_writers} per-writer files, "
                f"got {len(files)}. Missing writer indices: {missing}. "
                f"Aggregate WILL UNDER-REPORT — re-pull these files before trusting it."
            )
        agg_path = os.path.join(
            results_root,
            f"{ks}-{opcnt}-{rc}-workload{wl}-{db}"
            f"_agg_multinode_w{total_writers}_t{thread}_trial{trial}.result",
        )
        write_aggregate(agg_path, sorted(files), wall_ms, total_writers)
    if any_incomplete:
        print(
            "[aggregate] one or more groups were incomplete; check the "
            "scp warnings above for which host's pull failed"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Multi-node YCSB orchestrator (drives run_local_ycsb_multiproc.py per host)."
    )
    parser.add_argument(
        "--config",
        default=os.path.join(ozonedb_home or "", "bench/scripts/config/ycsb.yaml"),
    )
    parser.add_argument(
        "--hosts",
        help="Comma-separated host list (overrides cloudlab.hosts).",
    )
    parser.add_argument("--ssh_user", help="Override cloudlab.ssh_user.")
    parser.add_argument("--ssh_key", help="Override cloudlab.ssh_private_key_path.")
    parser.add_argument(
        "--writers_per_host",
        type=int,
        default=None,
        help="Writer processes per host (overrides local.run.writers_per_host; "
             "falls back to local.run.num_writers, then 1).",
    )
    parser.add_argument(
        "--workloads",
        help="Pass-through to per-host runner (comma-separated workload letters).",
    )
    parser.add_argument(
        "--trial",
        type=int,
        default=None,
        help="Pass-through trial index. Caller drives the trial loop.",
    )
    parser.add_argument(
        "--run_tag",
        help="Tag for results dir; defaults to YYYYmmdd-HHMMSS. All hosts share this tag.",
    )
    parser.add_argument(
        "--no_fetch",
        action="store_true",
        help="Skip pulling per-writer result files back from each host.",
    )
    parser.add_argument(
        "--no_aggregate",
        action="store_true",
        help="Skip cross-node aggregation step.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print the per-host plan and exit.",
    )
    args = parser.parse_args()

    if not ozonedb_home:
        raise EnvironmentError(
            "OZONEDB_HOME is not set on the orchestrator host."
        )
    with open(args.config, "r") as f:
        config = _derive_addresses(yaml.safe_load(f))

    hosts = resolve_hosts(config, args.hosts)
    ssh_user = resolve_ssh_user(config, args.ssh_user)
    ssh_key = resolve_ssh_key(config, args.ssh_key)

    run_config = config["local"]["run"]
    writers_per_host = (
        args.writers_per_host
        if args.writers_per_host is not None
        else run_config.get(
            "writers_per_host", run_config.get("num_writers", 1)
        )
    )
    if writers_per_host < 1:
        raise ValueError("writers_per_host must be >= 1")

    trial = (
        args.trial if args.trial is not None else run_config.get("trial", 1)
    )

    total_writers = writers_per_host * len(hosts)
    run_tag = args.run_tag or datetime.now().strftime("%Y%m%d-%H%M%S")
    local_results_root = os.path.join(
        ozonedb_home, "bench/results/local", run_tag
    )
    log_dir = os.path.join(local_results_root, "_orchestrator_logs")
    os.makedirs(log_dir, exist_ok=True)

    targets = [f"{ssh_user}@{h}" for h in hosts]
    plan = []
    for i, host in enumerate(hosts):
        plan.append(
            {
                "host": host,
                "target": targets[i],
                "offset": i * writers_per_host,
                "writers": writers_per_host,
            }
        )

    print(
        f"[orchestrator] tag={run_tag} hosts={len(hosts)} "
        f"writers_per_host={writers_per_host} total_writers={total_writers} "
        f"trial={trial}"
    )
    for p in plan:
        print(
            f"  - {p['target']}: writers [{p['offset']}, "
            f"{p['offset'] + p['writers']}) of {total_writers}"
        )

    if args.dry_run:
        print("[orchestrator] --dry_run: not launching.")
        return

    remote_homes = {}
    for p in plan:
        remote_homes[p["host"]] = remote_ozonedb_home(p["target"], ssh_key)

    rcs = {}
    threads = []

    def worker(p):
        remote_home = remote_homes[p["host"]]
        cmd = build_remote_command(
            remote_home, run_tag, p["offset"], p["writers"],
            total_writers, args.workloads, trial,
        )
        log_path = host_log_name(p["host"], log_dir)
        try:
            rcs[p["host"]] = stream_remote(p["target"], ssh_key, cmd, log_path)
        except Exception as e:
            print(f"[{p['target']}] error: {e}")
            rcs[p["host"]] = -1

    wall_start = time.time()
    for p in plan:
        t = threading.Thread(target=worker, args=(p,), daemon=False)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    wall_ms = (time.time() - wall_start) * 1000.0

    print(f"[orchestrator] wall={wall_ms / 1000.0:.1f}s return_codes={rcs}")
    if any(rc != 0 for rc in rcs.values()):
        print("[orchestrator] one or more hosts failed; aggregation may be incomplete")

    if args.no_fetch:
        print("[orchestrator] --no_fetch: skipping result pull")
        return

    for p in plan:
        remote_home = remote_homes[p["host"]]
        remote_dir = os.path.join(remote_home, "bench/results/local", run_tag)
        fetch_results(p["target"], ssh_key, remote_dir, local_results_root)

    if args.no_aggregate:
        print("[orchestrator] --no_aggregate: skipping cross-node aggregation")
        return

    cross_node_aggregate(
        local_results_root,
        total_writers=total_writers,
        wall_ms=wall_ms,
        trial_filter=trial,
    )
    print(f"[orchestrator] done; results at {local_results_root}")


if __name__ == "__main__":
    main()
