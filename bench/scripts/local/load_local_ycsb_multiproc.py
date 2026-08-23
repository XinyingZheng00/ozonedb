import sys
import subprocess
import os
import argparse
import yaml
import json
import re
import sqlite3
import time
from datetime import datetime

from load_local_ycsb import corfu_bridge_jar_path

# bench/scripts is one level up. ycsb_config.derive() resolves the `nodes:`
# block into the cloudlab.hosts / corfu.endpoint / s3.endpoint keys read below,
# so those are computed in exactly one place.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ycsb_config import derive as _derive_addresses

"""
Multi-process YCSB loader that emulates multiple distributed writers by
spawning N YCSB processes in parallel, each writing a disjoint key-range
partition. For ozonedb-corfu all writers target the same Corfu stream
(true shared-log distributed write test); for other backends each writer
writes to its own local directory.

Writer i loads keys [i * chunk, (i+1) * chunk) via YCSB's insertstart /
insertcount properties. recordcount is kept as the global total so keys
are drawn from the same global namespace.

After all writer processes exit, the parent parses their result files
and writes an aggregate summary to:
  {ks}-{rc}-insert-{db}_agg_w{N}_t{thread}.result

Set `num_writers` under `local.load` in ycsb.yaml, or pass --num_writers.
"""

ozonedb_home = os.environ.get("OZONEDB_HOME")


YCSB_DB_CLASSNAMES = {
    "ozonedb": "site.ycsb.db.OzoneDBClient",
    "rocksdb": "site.ycsb.db.rocksdb.RocksDBClient",
    "jdbc": "site.ycsb.db.JdbcDBClient",
}


def _resolve_binding(db_name):
    if db_name in ("ozonedb", "ozonedb-corfu"):
        return "ozonedb"
    if db_name == "sqlite":
        return "jdbc"
    return db_name


_classpath_cache = {}


def _resolve_ycsb_classpath(ycsb_path, binding):
    """Resolve the full YCSB classpath for `binding` by running Maven once,
    serially, in the parent process. Results are cached so parallel writer
    processes can skip bin/ycsb's Maven invocation entirely and avoid racing
    on ~/.m2 and the binding's target/ directory.
    """
    if binding in _classpath_cache:
        return _classpath_cache[binding]
    project = binding + "-binding"
    print(f"[classpath] resolving {project} via Maven (one-time, serial)...")
    try:
        out = subprocess.check_output(
            [
                "mvn",
                "-pl", f"site.ycsb:{project}",
                "-am",
                "package",
                "-DskipTests",
                "dependency:build-classpath",
                "-DincludeScope=compile",
                "-Dmdep.outputFilterFile=true",
            ],
            cwd=ycsb_path,
            text=True,
        )
    except subprocess.CalledProcessError as err:
        raise RuntimeError(
            f"Failed to resolve classpath for {project} via Maven:\n{err.output}"
        )
    cp_lines = [ln for ln in out.splitlines() if ln.startswith("classpath=")]
    if not cp_lines:
        raise RuntimeError(f"Maven produced no classpath= line for {project}")
    maven_cp = cp_lines[-1][len("classpath="):]

    db_dir = os.path.join(ycsb_path, binding)
    target_dir = os.path.join(db_dir, "target")
    target_jars = []
    if os.path.isdir(target_dir):
        for root, _dirs, files in os.walk(target_dir):
            for fn in files:
                if fn.endswith(".jar") and fn.startswith(project):
                    target_jars.append(os.path.join(root, fn))

    cp_parts = [os.path.join(db_dir, "conf")] + target_jars + [maven_cp]
    classpath = os.pathsep.join(cp_parts)
    _classpath_cache[binding] = classpath
    print(
        f"[classpath] {project} resolved "
        f"({len(target_jars)} target jar(s) + mvn deps)"
    )
    return classpath


def _java_binary():
    jh = os.environ.get("JAVA_HOME")
    if jh:
        return os.path.join(jh, "bin", "java")
    return "java"


# Suffix appended to the result-file engine token when strict reads are on.
LINEARIZABLE_LABEL_SUFFIX = "-linearizable"


def _truthy(v):
    """The spellings of true that reach the corfu settings dict (YAML bool,
    CLI/JSON strings, the odd 1)."""
    return v in (True, "true", "True", "1", 1)


def linearizable_corfu_settings(corfu_settings):
    """Copy of `corfu_settings` with strict reads forced on.

    Every get then fences on the global log tail (one sequencer round-trip,
    see src/db/db.cpp) instead of trusting the background tailer. Metadata
    rejects linearizable_reads together with trust_background_tail, so the
    two are always set as a pair. This is the ONE place the override lives:
    run_local_ycsb_multiproc.py --linearizable and consistency.py
    --linearizable both call it, so the throughput and consistency runs
    cannot drift apart in what "linearizable" means.
    """
    s = dict(corfu_settings or {})
    s["linearizable_reads"] = True
    s["trust_background_tail"] = False
    return s


def result_label(db_name, corfu_settings):
    """Engine token for result filenames: db_name plus the read mode.

    db_name itself is branched on by the runners (binding, cached-data
    handling, thread sweep, scratch paths) and must not change. The read
    mode rides along as a suffix -- `ozonedb-corfu-linearizable` -- so a
    linearizable run can never produce the same filenames as a default run.
    Before this, the only trace of the mode was corfu.linearizable_reads in
    ycsb.yaml, and a forgotten toggle yielded two identical "default" sweeps.
    Derived from the EFFECTIVE settings, not just the --linearizable flag,
    so the YAML route is labelled too.
    """
    if db_name == "ozonedb-corfu" and _truthy(
        (corfu_settings or {}).get("linearizable_reads")
    ):
        return db_name + LINEARIZABLE_LABEL_SUFFIX
    return db_name


def _make_corfu_config_per_writer(writer_idx, db_path, corfu_settings, s3_settings):
    """Write a per-writer shared_config_w{i}.json.

    Corfu endpoint/stream stays shared across writers (that's the point),
    but db_path must be unique so each process has its own local metadata
    scratch directory.
    """
    base_json = os.path.join(
        ozonedb_home, "src/config/corfu/shared_config_base.json"
    )
    out_json = os.path.join(
        ozonedb_home, f"src/config/corfu/shared_config_w{writer_idx}.json"
    )
    with open(base_json, "r") as f:
        data = json.load(f)
    data["db_path"] = db_path
    data["corfu_jar_path"] = corfu_bridge_jar_path()
    if corfu_settings:
        if "endpoint" in corfu_settings:
            data["corfu_endpoint"] = corfu_settings["endpoint"]
        if "stream_name" in corfu_settings:
            data["corfu_stream_name"] = corfu_settings["stream_name"]
        if "jvm_opts" in corfu_settings:
            data["corfu_jvm_opts"] = corfu_settings["jvm_opts"]
        # Compaction-tuning passthrough — used by the compaction-contention
        # experiment script to override shared_config_base.json's defaults.
        # Names match shared_config_base.json keys 1:1.
        for k in (
            "log_file_size_limit",
            "base_file_number_limit",
            "level_size",
            "level_file_size_limit",
            "max_level",
        ):
            if k in corfu_settings:
                data[k] = corfu_settings[k]
        # Read-consistency knobs. Metadata parses every config value as a
        # string, so normalize YAML booleans to "true"/"false" rather than
        # letting json.dump emit a bare true/false literal.
        for k in ("trust_background_tail", "linearizable_reads"):
            if k in corfu_settings:
                data[k] = "true" if _truthy(corfu_settings[k]) else "false"
    if s3_settings:
        data["sstable_backend"] = "s3"
        if "endpoint" in s3_settings:
            data["s3_endpoint"] = s3_settings["endpoint"]
        if "region" in s3_settings:
            data["s3_region"] = s3_settings["region"]
        if "bucket" in s3_settings:
            data["s3_bucket"] = s3_settings["bucket"]
        if "access_key" in s3_settings:
            data["s3_access_key"] = s3_settings["access_key"]
        if "secret_key" in s3_settings:
            data["s3_secret_key"] = s3_settings["secret_key"]
        if "use_path_style" in s3_settings:
            data["s3_use_path_style"] = (
                "true" if s3_settings["use_path_style"] else "false"
            )
        if "sstable_dir" in s3_settings:
            data["sstable_dir"] = s3_settings["sstable_dir"]
    else:
        for k in (
            "sstable_backend",
            "s3_endpoint",
            "s3_region",
            "s3_bucket",
            "s3_access_key",
            "s3_secret_key",
            "s3_use_path_style",
            "sstable_dir",
        ):
            data.pop(k, None)
    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w") as f:
        json.dump(data, f, indent=4)
    print(
        f"[writer {writer_idx}] corfu config written: {out_json} (db_path={db_path})"
    )
    return out_json


def _make_local_config_per_writer(writer_idx, db_path):
    """Write a per-writer shared_config_w{i}.json for the local-FS backend.

    Parallel writers must each get their own config file — otherwise they
    race on a single shared_config.json and all end up pointed at whichever
    db_path was written last.
    """
    base_json = os.path.join(
        ozonedb_home, "src/config/local/shared_config_base.json"
    )
    out_json = os.path.join(
        ozonedb_home, f"src/config/local/shared_config_w{writer_idx}.json"
    )
    with open(base_json, "r") as f:
        data = json.load(f)
    data["db_path"] = db_path
    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w") as f:
        json.dump(data, f, indent=4)
    print(
        f"[writer {writer_idx}] local config written: {out_json} (db_path={db_path})"
    )
    return out_json


def partition_records(total, num_writers):
    """Split `total` records into num_writers contiguous (start, count) pairs."""
    chunk = total // num_writers
    parts = []
    offset = 0
    for i in range(num_writers):
        count = chunk if i < num_writers - 1 else (total - offset)
        parts.append((offset, count))
        offset += count
    return parts


def spawn_parallel(jobs):
    """Launch (cmd, stdout_path) pairs in parallel, return list of return codes."""
    procs = []
    fds = []
    for cmd, out_path in jobs:
        subprocess.run(["rm", "-rf", out_path])
        f = open(out_path, "a")
        fds.append(f)
        print(" ".join(cmd))
        procs.append(subprocess.Popen(cmd, stdout=f, stderr=f))
    rcs = [p.wait() for p in procs]
    for f in fds:
        f.close()
    return rcs


_YCSB_LINE = re.compile(r"^\[([^\]]+)\],\s*([^,]+?),\s*(.+?)\s*$")


def parse_ycsb_result(path):
    """Return {section: {metric: value_str}} parsed from a YCSB result file."""
    sections = {}
    if not os.path.exists(path):
        return sections
    with open(path, "r", errors="replace") as f:
        for line in f:
            m = _YCSB_LINE.match(line.strip())
            if not m:
                continue
            section, metric, value = m.group(1), m.group(2).strip(), m.group(3).strip()
            sections.setdefault(section, {})[metric] = value
    return sections


def _fnum(d, key):
    try:
        return float(d.get(key, ""))
    except (TypeError, ValueError):
        return None


def write_aggregate(agg_path, per_writer_files, wall_ms, num_writers):
    """Parse per-writer YCSB outputs and emit an aggregate summary."""
    parsed = [(p, parse_ycsb_result(p)) for p in per_writer_files]

    op_sections = set()
    for _, sec in parsed:
        for k in sec.keys():
            if k != "OVERALL":
                op_sections.add(k)

    total_ops = 0
    writer_runtimes_ms = []
    writer_throughputs = []
    for _, sec in parsed:
        overall = sec.get("OVERALL", {})
        rt = _fnum(overall, "RunTime(ms)")
        tp = _fnum(overall, "Throughput(ops/sec)")
        if rt is not None:
            writer_runtimes_ms.append(rt)
        if tp is not None:
            writer_throughputs.append(tp)
        for op in op_sections:
            ops = _fnum(sec.get(op, {}), "Operations")
            if ops is not None:
                total_ops += ops

    agg_lines = []
    agg_lines.append(f"[AGGREGATE], NumWriters, {num_writers}")
    agg_lines.append(f"[AGGREGATE], WallTime(ms), {int(wall_ms)}")
    agg_lines.append(f"[AGGREGATE], TotalOperations, {int(total_ops)}")
    if wall_ms > 0:
        agg_lines.append(
            f"[AGGREGATE], Throughput(ops/sec), {total_ops / (wall_ms / 1000.0):.2f}"
        )
    if writer_throughputs:
        agg_lines.append(
            f"[AGGREGATE], SumWriterThroughput(ops/sec), {sum(writer_throughputs):.2f}"
        )
        agg_lines.append(
            f"[AGGREGATE], AvgWriterThroughput(ops/sec), {sum(writer_throughputs) / len(writer_throughputs):.2f}"
        )
    if writer_runtimes_ms:
        agg_lines.append(
            f"[AGGREGATE], MaxWriterRunTime(ms), {int(max(writer_runtimes_ms))}"
        )
        agg_lines.append(
            f"[AGGREGATE], MinWriterRunTime(ms), {int(min(writer_runtimes_ms))}"
        )

    for op in sorted(op_sections):
        ops_sum = 0.0
        lat_weighted = 0.0
        max_p99 = 0.0
        max_p95 = 0.0
        max_max_lat = 0.0
        any_data = False
        for _, sec in parsed:
            s = sec.get(op, {})
            ops = _fnum(s, "Operations")
            if ops is None:
                continue
            any_data = True
            ops_sum += ops
            avg = _fnum(s, "AverageLatency(us)")
            if avg is not None:
                lat_weighted += avg * ops
            p95 = _fnum(s, "95thPercentileLatency(us)")
            p99 = _fnum(s, "99thPercentileLatency(us)")
            mx = _fnum(s, "MaxLatency(us)")
            if p95 is not None and p95 > max_p95:
                max_p95 = p95
            if p99 is not None and p99 > max_p99:
                max_p99 = p99
            if mx is not None and mx > max_max_lat:
                max_max_lat = mx
        if not any_data:
            continue
        agg_lines.append(f"[{op}], Operations, {int(ops_sum)}")
        if ops_sum > 0:
            agg_lines.append(
                f"[{op}], WeightedAverageLatency(us), {lat_weighted / ops_sum:.2f}"
            )
        agg_lines.append(f"[{op}], MaxOver95thPercentileLatency(us), {int(max_p95)}")
        agg_lines.append(f"[{op}], MaxOver99thPercentileLatency(us), {int(max_p99)}")
        agg_lines.append(f"[{op}], MaxMaxLatency(us), {int(max_max_lat)}")

    agg_lines.append("")
    agg_lines.append("# Per-writer result files:")
    for p in per_writer_files:
        agg_lines.append(f"#   {p}")

    with open(agg_path, "w") as f:
        f.write(
            f"# Aggregated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
            f"across {num_writers} writer processes\n"
        )
        f.write("\n".join(agg_lines) + "\n")
    print(f"[aggregate] wrote {agg_path}")


def load_ycsb(
    record_cnt,
    key_size,
    db_names,
    ycsb_data_path,
    threads,
    repeated,
    num_writers,
    corfu_settings=None,
    s3_settings=None,
):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
    if num_writers < 1:
        raise ValueError("num_writers must be >= 1")

    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/local")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")
    subprocess.run(["mkdir", "-p", result_path])

    os.chdir(ycsb_path)
    for each_record_cnt in record_cnt:
        each_record_cnt = str(each_record_cnt)
        total_records = int(each_record_cnt)
        for each_key_size in key_size:
            subprocess.run(
                [
                    "python3",
                    os.path.join(script_path, "generate_workload.py"),
                    "--key_size",
                    each_key_size,
                    "--record_cnt",
                    each_record_cnt,
                    "--workload_name",
                    "a",
                    "--operation_cnt",
                    each_record_cnt,
                ]
            )
            workload_path = os.path.join(
                ycsb_path,
                "workloads/generated_workloads",
                f"workloada_{each_key_size}_{each_record_cnt}_{each_record_cnt}",
            )

            partitions = partition_records(total_records, num_writers)

            for db_name in db_names:
                if db_name == "raw_sync_io":
                    print(
                        "raw_sync_io is not supported by the multi-proc loader; skipping"
                    )
                    continue

                if db_name in ("ozonedb", "ozonedb-corfu"):
                    thread_list = [str(t) for t in threads]
                else:
                    thread_list = ["1"]

                binding = _resolve_binding(db_name)
                base_cp = _resolve_ycsb_classpath(ycsb_path, binding)
                db_classname = YCSB_DB_CLASSNAMES[binding]
                java_bin = _java_binary()

                for repeat_round in range(repeated):
                    print(
                        f"[round {repeat_round}] db={db_name} writers={num_writers} partitions={partitions}"
                    )
                    for thread in thread_list:
                        jobs = []
                        per_writer_files = []
                        for writer_idx, (start, count) in enumerate(partitions):
                            per_writer_data_path = os.path.join(
                                ycsb_data_path,
                                f"cached_data-{db_name}-{each_key_size}-{each_record_cnt}-w{writer_idx}/",
                            )
                            subprocess.run(["rm", "-rf", per_writer_data_path])

                            ycsb_props = [
                                "-threads", thread,
                                "-s",
                                "-P", workload_path,
                                "-p", f"recordcount={total_records}",
                                "-p", f"insertstart={start}",
                                "-p", f"insertcount={count}",
                                "-p", f"operationcount={count}",
                                "-p", "status.interval=1",
                            ]
                            extra_cp_entries = []

                            if db_name == "rocksdb":
                                ycsb_props += [
                                    "-p", f"rocksdb.dir={per_writer_data_path}"
                                ]
                            elif db_name == "ozonedb":
                                cfg = _make_local_config_per_writer(
                                    writer_idx, per_writer_data_path
                                )
                                ycsb_props += ["-p", f"shared_config={cfg}"]
                            elif db_name == "ozonedb-corfu":
                                cfg = _make_corfu_config_per_writer(
                                    writer_idx,
                                    per_writer_data_path,
                                    corfu_settings,
                                    s3_settings,
                                )
                                ycsb_props += ["-p", f"shared_config={cfg}"]
                                extra_cp_entries.append(corfu_bridge_jar_path())
                            elif db_name == "sqlite":
                                os.makedirs(per_writer_data_path, exist_ok=True)
                                db_file = os.path.join(per_writer_data_path, "mydb.db")
                                sqlite_cfg = os.path.join(
                                    ozonedb_home,
                                    f"bench/scripts/config/sqlite_w{writer_idx}.properties",
                                )
                                with open(sqlite_cfg, "w") as pf:
                                    pf.write("db.driver=org.sqlite.JDBC\n")
                                    pf.write(
                                        f"db.url=jdbc:sqlite:{db_file}?journal_mode=WAL&cache_size=-163840&synchronous=FULL\n"
                                    )
                                conn = sqlite3.connect(db_file)
                                cur = conn.cursor()
                                cur.execute("PRAGMA cache_size = -163840;")
                                cur.execute("PRAGMA synchronous = FULL;")
                                cur.execute("PRAGMA journal_mode = WAL;")
                                cur.execute(
                                    "CREATE TABLE usertable ( "
                                    "YCSB_KEY VARCHAR(255),  "
                                    "FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, "
                                    "FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT);"
                                )
                                conn.commit()
                                conn.close()
                                ycsb_props += ["-P", sqlite_cfg]
                                extra_cp_entries.append(
                                    os.path.join(
                                        ycsb_path,
                                        "jdbc/target/dependency/sqlite-jdbc-3.49.1.0.jar",
                                    )
                                )

                            full_cp = (
                                os.pathsep.join(extra_cp_entries + [base_cp])
                                if extra_cp_entries
                                else base_cp
                            )
                            cmd = (
                                [
                                    java_bin,
                                    "-cp", full_cp,
                                    "site.ycsb.Client",
                                    "-db", db_classname,
                                ]
                                + ycsb_props
                                + ["-load"]
                            )

                            result_file = os.path.join(
                                result_path,
                                f"{each_key_size}-{each_record_cnt}-insert-{db_name}_w{writer_idx}of{num_writers}_t{thread}.result",
                            )
                            jobs.append((cmd, result_file))
                            per_writer_files.append(result_file)

                        wall_start = time.time()
                        rcs = spawn_parallel(jobs)
                        wall_ms = (time.time() - wall_start) * 1000.0
                        if any(r != 0 for r in rcs):
                            print(f"[warning] writer return codes: {rcs}")

                        agg_file = os.path.join(
                            result_path,
                            f"{each_key_size}-{each_record_cnt}-insert-{db_name}_agg_w{num_writers}_t{thread}.result",
                        )
                        write_aggregate(
                            agg_file, per_writer_files, wall_ms, num_writers
                        )

                        if db_name == "sqlite":
                            for writer_idx in range(num_writers):
                                per_writer_data_path = os.path.join(
                                    ycsb_data_path,
                                    f"cached_data-{db_name}-{each_key_size}-{each_record_cnt}-w{writer_idx}/",
                                )
                                db_file = os.path.join(per_writer_data_path, "mydb.db")
                                conn = sqlite3.connect(db_file)
                                cur = conn.cursor()
                                cur.execute(
                                    "CREATE INDEX idx_ycsb_key ON usertable (YCSB_KEY);"
                                )
                                conn.commit()
                                conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Multi-process YCSB loader (distributed writers emulation)."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=os.path.join(
            os.environ.get("OZONEDB_HOME") or "", "bench/scripts/config/ycsb.yaml"
        ),
    )
    parser.add_argument(
        "--num_writers",
        type=int,
        default=None,
        help="Number of parallel writer processes (overrides local.load.num_writers).",
    )
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = _derive_addresses(yaml.safe_load(f))

    load_config = config["local"]["load"]
    record_cnts = load_config["record_cnt"]
    key_sizes = load_config["key_size"]
    db_names = load_config["db_name"]
    ycsb_data_path = load_config["ycsb_data_path"]
    threads = load_config["threads"]
    repeated = load_config["repeated"]
    num_writers = (
        args.num_writers
        if args.num_writers is not None
        else load_config.get("num_writers", 2)
    )
    corfu_settings = config.get("corfu")
    s3_settings = config.get("s3")
    os.makedirs(ycsb_data_path, exist_ok=True)

    print(f"Launching {num_writers} parallel writer processes")
    load_ycsb(
        record_cnts,
        key_sizes,
        db_names,
        ycsb_data_path,
        threads,
        repeated,
        num_writers,
        corfu_settings,
        s3_settings,
    )
