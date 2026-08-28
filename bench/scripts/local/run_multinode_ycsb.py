import sys
import argparse
import os
import re
import shlex
import signal
import subprocess
import threading
import time
from datetime import datetime

import yaml

from load_local_ycsb_multiproc import (
    cassandra_mode_settings,
    CORFU_CLIENTS,
    corfu_client_corfu_settings,
    linearizable_corfu_settings,
    lru_cache_corfu_settings,
    result_label,
    write_aggregate,
)

# bench/scripts is one level up. ycsb_config.derive() resolves the `nodes:`
# block into the cloudlab.hosts / corfu.endpoint / s3.endpoint keys read below,
# so those are computed in exactly one place.
_SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _SCRIPTS_DIR)
from ycsb_config import derive as _derive_addresses
from ycsb_config import ConfigError, node as _cfg_node, ssh_addr as _ssh_addr

# bench/scripts/server_sampler.sh, pushed to every sampled server per cell.
SERVER_SAMPLER_SRC = os.path.join(_SCRIPTS_DIR, "server_sampler.sh")
SERVER_SAMPLER_REMOTE = "ozonedb_server_sampler.sh"       # in $HOME on the server
SERVER_SAMPLES_REMOTE_DIR = "ozonedb_server_samples"      # $HOME/<this>/<run_tag>/

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
--writers_per_host) plus optional --workloads / --trial / --max_exec_time
/ --linearizable, all forwarded verbatim to every host's runner so one
orchestrator invocation fully determines what every client runs -- no
per-client ycsb.yaml edit is needed for a cell.
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
    # ServerAlive*: a cell's ssh session carries no data while YCSB runs
    # (its output goes to a file on the client), and a 600 s silent TCP
    # connection through the campus NAT was dropped -- the remote side
    # finished, the local ssh never noticed, and the cell hung until the
    # timeout with return_codes={}. 30 s keepalives hold the NAT entry, and
    # a dead link now fails the session within 3 min instead of never.
    return [
        "ssh",
        "-i", ssh_key,
        "-oStrictHostKeyChecking=no",
        "-oUserKnownHostsFile=/dev/null",
        "-oLogLevel=ERROR",
        "-oServerAliveInterval=30",
        "-oServerAliveCountMax=6",
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


def kill_remote_ycsb(target, ssh_key):
    """SIGKILL any YCSB writer processes left on a host. Used when a cell
    overruns its timeout: a writer stuck in the linearizable tailer catch-up
    does 0 ops, so YCSB's between-ops maxexecutiontime never fires and the
    process hangs forever, blocking the whole sweep. This unblocks it."""
    # The bracket trick keeps the pattern from matching the remote shell
    # that runs this very command (its argv holds the pattern text), which
    # otherwise dies at the first pkill and never reaches the second.
    cmd = ssh_base(target, ssh_key) + [
        "pkill -9 -f '[s]ite.ycsb.Client'; pkill -9 -f '[r]un_local_ycsb_multiproc'; true"
    ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        print(f"[{target}] killed remote YCSB writers (cell timeout)")
    except Exception as e:
        print(f"[{target}] remote kill failed: {e}")


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
                         total_writers, workloads, trial, max_exec_time=None,
                         linearizable=False, log_trim=False, db_name=None,
                         cassandra_consistency=None, lru_cache_bytes=None,
                         record_cnt=None, corfu_client=None):
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
            + (["--max_exec_time", str(int(max_exec_time))] if max_exec_time else [])
            + (["--linearizable"] if linearizable else [])
            + (["--log-trim"] if log_trim else [])
            + (["--db_name", shlex.quote(db_name)] if db_name else [])
            + (["--cassandra_consistency", cassandra_consistency] if cassandra_consistency else [])
            + (["--lru-cache-bytes", str(int(lru_cache_bytes))] if lru_cache_bytes is not None else [])
            + (["--record_cnt", str(int(record_cnt))] if record_cnt is not None else [])
            + (["--corfu-client", corfu_client] if corfu_client else [])
        ),
    ]
    return " && ".join(parts)


# --------------------------------------------------------------------------
# Server-side sampling (bench/PLAN-cost.md P0.2). The orchestrator is the
# one place that knows the cell's label, writer count and trial, and it
# already SSHes and scps, so the sampler is driven from here rather than
# from the two shell wrappers. Every failure here is a warning: a cell
# without a server sample is still a valid throughput cell.
# --------------------------------------------------------------------------

def cell_label(config, args):
    """The result label the clients will write for this invocation, computed
    with the same functions they use, so the server sample files carry the
    same cell name as the per-writer result files."""
    run_config = config["local"]["run"]
    if args.db_name:
        db_names = [d.strip() for d in args.db_name.split(",") if d.strip()]
    else:
        db_names = list(run_config.get("db_name") or [])
    corfu = config.get("corfu")
    if args.linearizable:
        corfu = linearizable_corfu_settings(corfu)
    if args.lru_cache_bytes is not None:
        corfu = lru_cache_corfu_settings(corfu, args.lru_cache_bytes)
    if args.corfu_client:
        corfu = corfu_client_corfu_settings(corfu, args.corfu_client)
    cass = config.get("cassandra")
    if args.cassandra_consistency:
        cass = cassandra_mode_settings(cass, args.cassandra_consistency)
    labels = []
    for d in db_names:
        try:
            labels.append(result_label(d, corfu, cass, args.lru_cache_bytes))
        except Exception:
            labels.append(d)
    return "+".join(labels), db_names


def server_sample_targets(config, db_names):
    """SSH addresses of the stateful servers behind these backends, in order,
    without duplicates: the Corfu node and the object-store node for
    ozonedb-corfu (one box on the current cluster), the Cassandra nodes for
    cassandra. Other backends have no server."""
    out = []

    def add(addr):
        if addr and addr not in out:
            out.append(addr)

    for d in db_names:
        if d == "ozonedb-corfu":
            for role in ("log", "store"):
                try:
                    add(_ssh_addr(_cfg_node(config, role), f"nodes.{role}"))
                except ConfigError:
                    pass
        elif d == "cassandra":
            for addr in (config.get("cassandra") or {}).get("ssh_hosts") or []:
                add(addr)
    return out


def sampler_env(config):
    """Environment for server_sampler.sh: the MinIO port and bucket from the
    s3 block, so `mc du` and the metrics scrape hit the right store."""
    s3 = config.get("s3") or {}
    parts = [f"SAMPLER_MINIO_URL=http://127.0.0.1:{s3.get('port', 9000)}"]
    if s3.get("bucket"):
        parts.append(f"SAMPLER_BUCKET={shlex.quote(str(s3['bucket']))}")
    return " ".join(parts)


def sampler_push(target, ssh_key):
    cmd = [
        "scp", "-i", ssh_key,
        "-oStrictHostKeyChecking=no", "-oUserKnownHostsFile=/dev/null",
        "-q", SERVER_SAMPLER_SRC, f"{target}:{SERVER_SAMPLER_REMOTE}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"scp of server_sampler.sh failed: {proc.stderr.strip()}")


def sampler_run(target, ssh_key, env, verb, remote_dir, cell):
    remote = (
        f"mkdir -p {shlex.quote(remote_dir)} && "
        f"env {env} bash {SERVER_SAMPLER_REMOTE} {verb} {shlex.quote(remote_dir)} {shlex.quote(cell)}"
    )
    cmd = ssh_base(target, ssh_key) + [remote]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    msg = (proc.stderr or "").strip()
    if msg:
        print(f"[{target}] sampler {verb}: {msg}")
    if proc.returncode != 0:
        raise RuntimeError(f"server_sampler.sh {verb} exited {proc.returncode}: {msg}")


def sampler_fetch(target, ssh_key, remote_dir, cell, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    cmd = [
        "scp", "-i", ssh_key,
        "-oStrictHostKeyChecking=no", "-oUserKnownHostsFile=/dev/null",
        "-q", f"{target}:{remote_dir}/{cell}.*", local_dir,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(f"scp of server samples failed: {proc.stderr.strip()}")


class ServerSampling:
    """start() before the writers launch, stop() after they exit. Samples
    land in <local_results_root>/_server/<host>/<cell>.{before,after,pidstat}.txt."""

    def __init__(self, config, ssh_user, ssh_key, run_tag, cell, local_results_root, hosts):
        self.ssh_key = ssh_key
        self.env = sampler_env(config)
        self.remote_dir = f"{SERVER_SAMPLES_REMOTE_DIR}/{run_tag}"
        self.cell = cell
        self.local_root = os.path.join(local_results_root, "_server")
        self.targets = [f"{ssh_user}@{h}" for h in hosts]
        self.hosts = hosts
        self.started = []

    def start(self):
        for host, target in zip(self.hosts, self.targets):
            try:
                sampler_push(target, self.ssh_key)
                sampler_run(target, self.ssh_key, self.env, "start", self.remote_dir, self.cell)
                self.started.append((host, target))
                print(f"[{target}] server sample started: cell={self.cell}")
            except Exception as e:
                print(f"[{target}] WARNING: server sampling not started: {e}")

    def stop(self):
        for host, target in self.started:
            try:
                sampler_run(target, self.ssh_key, self.env, "stop", self.remote_dir, self.cell)
                local_dir = os.path.join(self.local_root, re.sub(r"[^A-Za-z0-9._-]", "_", host))
                sampler_fetch(target, self.ssh_key, self.remote_dir, self.cell, local_dir)
                print(f"[{target}] server sample pulled into {local_dir}")
            except Exception as e:
                print(f"[{target}] WARNING: server sample incomplete: {e}")


# `db` is the result LABEL, which may carry a read-mode suffix
# (ozonedb-corfu-linearizable); the lazy `.+?` stops at the `_w{idx}of` tail.
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
        "--max_exec_time",
        type=int,
        default=None,
        help="Cap every writer's YCSB run at this many seconds (forwarded to each "
             "host's run_local_ycsb_multiproc.py, overriding its local.run.max_exec_time).",
    )
    parser.add_argument(
        "--linearizable",
        action="store_true",
        help="Strict reads on every writer (linearizable_reads=true + "
             "trust_background_tail=false in each generated shared_config); "
             "result files are labelled ozonedb-corfu-linearizable.",
    )
    parser.add_argument(
        "--log-trim",
        action="store_true",
        help="Log trimming: forwarded to every host's run_local_ycsb_multiproc.py; "
             "global writer 0 (first writer on the first host) runs the trimmer.",
    )
    parser.add_argument(
        "--db_name",
        help="Pass-through to per-host runner: comma-separated backends "
             "(overrides local.run.db_name on every client), e.g. cassandra.",
    )
    parser.add_argument(
        "--cassandra_consistency",
        choices=["one", "quorum", "serial"],
        default=None,
        help="Pass-through to per-host runner (cassandra only): force the consistency "
             "mode; result files are labelled cassandra-<mode>.",
    )
    parser.add_argument(
        "--lru-cache-bytes",
        type=int,
        default=None,
        help="Pass-through to per-host runner (ozonedb / ozonedb-corfu): per-process "
             "block cache in bytes; result files get a -lru64m style token.",
    )
    parser.add_argument(
        "--record_cnt",
        type=int,
        default=None,
        help="Pass-through to per-host runner: dataset size in records (overrides "
             "local.run.record_cnt on every client); must match the load.",
    )
    parser.add_argument(
        "--corfu-client",
        choices=CORFU_CLIENTS,
        default=None,
        help="Pass-through to per-host runner (ozonedb-corfu only): native (the C++ "
             "client, default) or jni (embedded JVM + CorfuBridge); native result files "
             "are labelled ozonedb-corfu-native.",
    )
    parser.add_argument(
        "--no_server_sample",
        action="store_true",
        help="Do not run bench/scripts/server_sampler.sh on the server node(s) "
             "around this cell. By default the Corfu/MinIO node (ozonedb-corfu) or "
             "the Cassandra nodes are sampled and the files land in "
             "<results>/_server/<host>/ (bench/PLAN-cost.md P0.2).",
    )
    parser.add_argument(
        "--cell_timeout",
        type=int,
        default=None,
        help="Hard wall-clock cap (seconds) for this cell. If a host has not "
             "finished by then, remote YCSB writers are killed on every host and "
             "the sweep continues (the cell under-reports). Default: "
             "2*max_exec_time+120 when --max_exec_time is set, else no cap.",
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

    # Server sampling: one cell name shared with the per-writer result files.
    label, db_names = cell_label(config, args)
    workloads_token = (args.workloads or "yaml").replace(",", "+")
    cell = f"{label}_workload{workloads_token}_w{total_writers}_trial{trial}"
    sample_hosts = [] if args.no_server_sample else server_sample_targets(config, db_names)

    print(
        f"[orchestrator] tag={run_tag} hosts={len(hosts)} "
        f"writers_per_host={writers_per_host} total_writers={total_writers} "
        f"trial={trial} workloads={args.workloads or 'yaml'} "
        f"max_exec_time={args.max_exec_time or 'yaml'} "
        f"read_mode={'linearizable' if args.linearizable else 'default'} "
        f"db_name={args.db_name or 'yaml'} "
        f"cassandra_consistency={args.cassandra_consistency or 'yaml'} "
        f"lru_cache_bytes={args.lru_cache_bytes or 'base'} "
        f"record_cnt={args.record_cnt or 'yaml'} "
        f"corfu_client={args.corfu_client or 'yaml'} "
        f"label={label} server_sample={sample_hosts or 'off'}"
    )
    for p in plan:
        print(
            f"  - {p['target']}: writers [{p['offset']}, "
            f"{p['offset'] + p['writers']}) of {total_writers}"
        )

    if args.dry_run:
        sample = build_remote_command(
            "$OZONEDB_HOME", run_tag, plan[0]["offset"], plan[0]["writers"],
            total_writers, args.workloads, trial,
            max_exec_time=args.max_exec_time, linearizable=args.linearizable,
            log_trim=args.log_trim,
            db_name=args.db_name, cassandra_consistency=args.cassandra_consistency,
            lru_cache_bytes=args.lru_cache_bytes, record_cnt=args.record_cnt,
            corfu_client=args.corfu_client,
        )
        print(f"[orchestrator] per-host command (first host): {sample}")
        if sample_hosts:
            print(f"[orchestrator] server sample: cell={cell} on {sample_hosts}")
        print("[orchestrator] --dry_run: not launching.")
        return

    sampling = ServerSampling(
        config, ssh_user, ssh_key, run_tag, cell, local_results_root, sample_hosts
    )

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
            max_exec_time=args.max_exec_time, linearizable=args.linearizable,
            log_trim=args.log_trim,
            db_name=args.db_name, cassandra_consistency=args.cassandra_consistency,
            lru_cache_bytes=args.lru_cache_bytes, record_cnt=args.record_cnt,
            corfu_client=args.corfu_client,
        )
        log_path = host_log_name(p["host"], log_dir)
        try:
            rcs[p["host"]] = stream_remote(p["target"], ssh_key, cmd, log_path)
        except Exception as e:
            print(f"[{p['target']}] error: {e}")
            rcs[p["host"]] = -1

    # Per-cell wall-clock timeout. A writer stuck in linearizable catch-up
    # hangs forever (0 ops -> maxexecutiontime never fires), so without this
    # one bad host blocks the entire sweep. Default: generous room over the
    # run itself for catch-up + scp, only when a run cap is set.
    cell_timeout = args.cell_timeout
    if cell_timeout is None and args.max_exec_time:
        cell_timeout = args.max_exec_time * 2 + 120

    # A SIGTERM (the shell wrapper's trap, a killed campaign chain) must not
    # leave the writers running on the clients: they would sit on a stopped
    # Corfu until the next cell's preflight finds them.
    def on_term(signum, frame):
        print(f"[orchestrator] signal {signum}: killing remote YCSB on all hosts")
        for p in plan:
            kill_remote_ycsb(p["target"], ssh_key)
        try:
            sampling.stop()
        except Exception:
            pass
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    sampling.start()
    wall_start = time.time()
    for p in plan:
        t = threading.Thread(target=worker, args=(p,), daemon=False)
        t.start()
        threads.append(t)

    deadline = (wall_start + cell_timeout) if cell_timeout else None
    for t in threads:
        if deadline is not None:
            t.join(timeout=max(0.0, deadline - time.time()))
        else:
            t.join()
    if deadline is not None and any(t.is_alive() for t in threads):
        print(
            f"[orchestrator] CELL TIMEOUT after {cell_timeout}s -- killing remote "
            f"YCSB on all hosts so the sweep can continue (this cell under-reports)"
        )
        for p in plan:
            kill_remote_ycsb(p["target"], ssh_key)
        for t in threads:
            t.join(60)
    wall_ms = (time.time() - wall_start) * 1000.0
    sampling.stop()

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
