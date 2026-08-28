import sys
import subprocess
import os
import argparse
import yaml
import time
from datetime import datetime

from load_local_ycsb import corfu_bridge_jar_path
from load_local_ycsb_multiproc import (
    _make_corfu_config_per_writer,
    _make_local_config_per_writer,
    _resolve_binding,
    _resolve_ycsb_classpath,
    _java_binary,
    CASSANDRA_MODES,
    SERVER_BACKENDS,
    YCSB_DB_CLASSNAMES,
    cache_warm_corfu_settings,
    cassandra_mode_settings,
    cassandra_ycsb_props,
    disk_cache_corfu_settings,
    disk_cache_label_token,
    CORFU_CLIENTS,
    corfu_client_corfu_settings,
    linearizable_corfu_settings,
    log_trim_corfu_settings,
    lru_cache_corfu_settings,
    partition_records,
    result_label,
    spawn_parallel,
    write_aggregate,
)

# bench/scripts is one level up. ycsb_config.derive() resolves the `nodes:`
# block into the cloudlab.hosts / corfu.endpoint / s3.endpoint keys read below,
# so those are computed in exactly one place.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ycsb_config import derive as _derive_addresses

"""
Multi-process YCSB runner that emulates distributed readers/writers by
spawning N YCSB processes in parallel against the dataset loaded by
load_local_ycsb_multiproc.py.

Operation space is split across total_writers: each process executes
operationcount / total_writers operations over the full recordcount
key range. For ozonedb-corfu all processes share the Corfu stream;
for other backends each process reads from its own per-writer cached
dir keyed by the global writer index.

For multi-machine runs, pass --offset (starting writer index for
this machine) and --total_writers (total writers across all
machines). This machine runs writer indices [offset, offset +
num_writers). Defaults (offset=0, total_writers=num_writers) match
the original single-machine behavior.

Per-writer results are written as:
  {ks}-{opcnt}-{rc}-workload{w}-{label}_w{writer_idx}of{total}_t{thread}_trial{trial}.result
Aggregate is:
  ..._agg_w{total}_t{thread}_trial{trial}.result                    (single-machine)
  ..._agg_w{num}of{total}_off{offset}_t{thread}_trial{trial}.result (partial / multi-machine)

{label} is the db_name plus the read mode: `ozonedb-corfu-linearizable`
when --linearizable (or corfu.linearizable_reads) is on, else the plain
db_name. Only the label changes -- db_name still selects the binding and
the scratch/cached paths.

One invocation runs ONE trial. `local.run.repeated` must be 1: a repeat
would rewrite the very same _trial{N} files, so repetition is expressed
as trials (--trial N, driven by the caller), never as repeats.

Set `num_writers`, `offset`, `total_writers` under `local.run` in
ycsb.yaml, or pass the matching --flags.
"""

ozonedb_home = os.environ.get("OZONEDB_HOME")


def run_ycsb(
    workload_names,
    record_cnt,
    operation_cnts,
    key_size,
    db_names,
    repeated,
    ycsb_data_path,
    threads,
    num_writers,
    offset=0,
    total_writers=None,
    corfu_settings=None,
    s3_settings=None,
    max_exec_time=None,
    trial=1,
    linearizable=False,
    cassandra_settings=None,
    cassandra_consistency=None,
    lru_cache_bytes=None,
):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
    if num_writers < 1:
        raise ValueError("num_writers must be >= 1")
    if lru_cache_bytes is not None:
        unsupported = [d for d in db_names if d not in ("ozonedb", "ozonedb-corfu")]
        if unsupported:
            raise ValueError(
                f"--lru-cache-bytes only applies to ozonedb / ozonedb-corfu (got {unsupported})"
            )
        corfu_settings = lru_cache_corfu_settings(corfu_settings, lru_cache_bytes)
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if trial < 1:
        raise ValueError("trial must be >= 1")
    if repeated != 1:
        # Every repeat would write the identical _trial{N} filenames and
        # overwrite the previous round's numbers. Trials are the only
        # supported repetition: one invocation per --trial N.
        raise ValueError(
            f"local.run.repeated must be 1 (got {repeated}): repeats overwrite "
            "the _trial{N} result files. Run once per trial with --trial N instead."
        )
    if linearizable:
        unsupported = [d for d in db_names if d != "ozonedb-corfu"]
        if unsupported:
            raise ValueError(
                f"--linearizable only applies to db_name ozonedb-corfu (got {unsupported})"
            )
        # Same override consistency.py --linearizable applies; the label
        # below picks it up from the effective settings.
        corfu_settings = linearizable_corfu_settings(corfu_settings)
    if cassandra_consistency:
        unsupported = [d for d in db_names if d != "cassandra"]
        if unsupported:
            raise ValueError(
                f"--cassandra_consistency only applies to db_name cassandra (got {unsupported})"
            )
        cassandra_settings = cassandra_mode_settings(cassandra_settings, cassandra_consistency)
    if total_writers is None:
        total_writers = num_writers
    if total_writers < offset + num_writers:
        raise ValueError(
            f"total_writers ({total_writers}) must be >= offset + num_writers "
            f"({offset} + {num_writers} = {offset + num_writers})"
        )

    record_cnt = str(record_cnt)
    total_records = int(record_cnt)
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    run_tag = os.environ.get("OZONEDB_RUN_TAG") or datetime.now().strftime(
        "%Y%m%d-%H%M%S"
    )
    result_path = os.path.join(ozonedb_home, "bench", "results/local", run_tag)
    os.makedirs(result_path, exist_ok=True)
    print(f"[results] writing to {result_path}")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")

    os.chdir(ycsb_path)
    for each_operation_cnt in operation_cnts:
        each_operation_cnt = str(each_operation_cnt)
        total_ops = int(each_operation_cnt)
        op_partitions = partition_records(total_ops, total_writers)
        local_partitions = op_partitions[offset : offset + num_writers]

        for each_key_size in key_size:
            for workload_name in workload_names:
                subprocess.run(
                    [
                        "python3",
                        os.path.join(script_path, "generate_workload.py"),
                        "--workload_name",
                        workload_name,
                        "--key_size",
                        each_key_size,
                        "--operation_cnt",
                        each_operation_cnt,
                        "--record_cnt",
                        record_cnt,
                    ]
                )
                workload_path = os.path.join(
                    ycsb_path,
                    "workloads/generated_workloads",
                    f"workload{workload_name}_{each_key_size}_{each_operation_cnt}_{record_cnt}",
                )

                for db_name in db_names:
                    if db_name == "ozonedb" or db_name in SERVER_BACKENDS:
                        thread_list = [str(t) for t in threads]
                    else:
                        thread_list = ["1"]

                    binding = _resolve_binding(db_name)
                    base_cp = _resolve_ycsb_classpath(ycsb_path, binding)
                    db_classname = YCSB_DB_CLASSNAMES[binding]
                    java_bin = _java_binary()
                    # Filenames carry the read mode; db_name (paths, binding,
                    # thread sweep) does not.
                    label = result_label(
                        db_name, corfu_settings, cassandra_settings, lru_cache_bytes
                    )

                    for thread in thread_list:
                        print(
                            f"[trial {trial}] "
                            f"workload={workload_name} db={db_name} label={label} thread={thread}"
                        )
                        jobs = []
                        per_writer_files = []
                        for local_idx, (_, writer_ops) in enumerate(local_partitions):
                            writer_idx = offset + local_idx
                            cached_data_path = os.path.join(
                                ycsb_data_path,
                                f"cached_data-{db_name}-{each_key_size}-{record_cnt}-w{writer_idx}/",
                            )
                            run_data_path = os.path.join(
                                ycsb_data_path,
                                f"{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-w{writer_idx}/",
                            )

                            if db_name not in SERVER_BACKENDS:
                                if not os.path.exists(cached_data_path):
                                    print(
                                        f"cached_data_path {cached_data_path} does not exist, skipping writer {writer_idx}..."
                                    )
                                    continue
                                subprocess.run(["rm", "-rf", run_data_path])
                                subprocess.run(
                                    ["cp", "-r", cached_data_path, run_data_path]
                                )
                            else:
                                subprocess.run(["rm", "-rf", run_data_path])
                                os.makedirs(run_data_path, exist_ok=True)

                            ycsb_props = [
                                "-threads",
                                thread,
                                "-s",
                                "-P",
                                workload_path,
                                "-p",
                                f"recordcount={total_records}",
                                "-p",
                                f"operationcount={writer_ops}",
                                "-p",
                                "status.interval=1",
                            ]
                            if max_exec_time:
                                ycsb_props += [
                                    "-p",
                                    f"maxexecutiontime={int(max_exec_time)}",
                                ]
                            extra_cp_entries = []

                            if db_name == "rocksdb":
                                ycsb_props += ["-p", f"rocksdb.dir={run_data_path}"]
                            elif db_name == "ozonedb":
                                cfg = _make_local_config_per_writer(
                                    writer_idx, run_data_path, lru_cache_bytes
                                )
                                ycsb_props += ["-p", f"shared_config={cfg}"]
                            elif db_name == "ozonedb-corfu":
                                cfg = _make_corfu_config_per_writer(
                                    writer_idx,
                                    run_data_path,
                                    corfu_settings,
                                    s3_settings,
                                )
                                ycsb_props += ["-p", f"shared_config={cfg}"]
                                extra_cp_entries.append(corfu_bridge_jar_path())
                            elif db_name == "cassandra":
                                ycsb_props += cassandra_ycsb_props(cassandra_settings)
                            elif db_name == "sqlite":
                                db_file = os.path.join(run_data_path, "mydb.db")
                                sqlite_cfg = os.path.join(
                                    ozonedb_home,
                                    f"bench/scripts/config/sqlite_w{writer_idx}.properties",
                                )
                                with open(sqlite_cfg, "w") as pf:
                                    pf.write("db.driver=org.sqlite.JDBC\n")
                                    pf.write(f"db.url=jdbc:sqlite:{db_file}\n")
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
                                    "-cp",
                                    full_cp,
                                    "site.ycsb.Client",
                                    "-db",
                                    db_classname,
                                ]
                                + ycsb_props
                                + ["-t"]
                            )

                            result_file = os.path.join(
                                result_path,
                                f"{each_key_size}-{each_operation_cnt}-{record_cnt}-workload{workload_name}-{label}_w{writer_idx}of{total_writers}_t{thread}_trial{trial}.result",
                            )
                            jobs.append((cmd, result_file))
                            per_writer_files.append(result_file)

                        if not jobs:
                            continue

                        wall_start = time.time()
                        rcs = spawn_parallel(jobs)
                        wall_ms = (time.time() - wall_start) * 1000.0
                        if any(r != 0 for r in rcs):
                            print(f"[warning] writer return codes: {rcs}")

                        if num_writers == total_writers and offset == 0:
                            agg_tag = f"_agg_w{total_writers}"
                        else:
                            agg_tag = (
                                f"_agg_w{num_writers}of{total_writers}_off{offset}"
                            )
                        agg_file = os.path.join(
                            result_path,
                            f"{each_key_size}-{each_operation_cnt}-{record_cnt}-workload{workload_name}-{label}{agg_tag}_t{thread}_trial{trial}.result",
                        )
                        write_aggregate(
                            agg_file, per_writer_files, wall_ms, num_writers
                        )

                        for local_idx in range(num_writers):
                            writer_idx = offset + local_idx
                            run_data_path = os.path.join(
                                ycsb_data_path,
                                f"{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-w{writer_idx}/",
                            )
                            subprocess.run(["rm", "-rf", run_data_path])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Multi-process YCSB runner (distributed writers emulation)."
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
        help="Number of parallel writer processes (overrides local.run.num_writers).",
    )
    parser.add_argument(
        "--workloads",
        type=str,
        default=None,
        help="Comma-separated workload letters (overrides local.run.workload_name).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=None,
        help="Starting writer index for this machine (overrides local.run.offset, default 0).",
    )
    parser.add_argument(
        "--total_writers",
        type=int,
        default=None,
        help="Total writer count across all machines (overrides local.run.total_writers, default = num_writers).",
    )
    parser.add_argument(
        "--trial",
        type=int,
        default=None,
        help="Trial index (>=1) to tag this run's result files as _trial{N}. Caller should drive the trial loop; this script runs one trial per invocation (overrides local.run.trial, default 1).",
    )
    parser.add_argument(
        "--max_exec_time",
        type=int,
        default=None,
        help="Cap each YCSB run at this many seconds via maxexecutiontime (overrides local.run.max_exec_time).",
    )
    parser.add_argument(
        "--linearizable",
        action="store_true",
        help="Strict reads (ozonedb-corfu only): every writer's generated shared_config gets "
             "linearizable_reads=true + trust_background_tail=false, and result files are "
             "labelled ozonedb-corfu-linearizable instead of ozonedb-corfu. Prefer this over "
             "toggling corfu.linearizable_reads in ycsb.yaml.",
    )
    parser.add_argument(
        "--log-trim",
        action="store_true",
        help="Log trimming (ozonedb-corfu only): global writer 0 checkpoints the Corfu "
             "stream to the SSTable bucket every corfu.log_trim.interval_ms and trims "
             "behind the checkpoint. Every writer loads the newest checkpoint at open. "
             "Prefer this over toggling corfu.log_trim.enabled in ycsb.yaml.",
    )
    parser.add_argument(
        "--cache-warm",
        action="store_true",
        help="ozonedb-corfu only: warm compaction outputs into every writer's block "
             "cache when a COMPACT is applied (cache_warm_enabled=true in each "
             "generated shared_config; bench/PLAN-compaction-cache.md). Result "
             "files get a -warm token.",
    )
    parser.add_argument(
        "--cache-warm-max-level", type=int, default=None,
        help="With --cache-warm: warm outputs of levels up to N (engine default 1); "
             "label token -wlN.",
    )
    parser.add_argument(
        "--cache-warm-max-fraction", type=float, default=None,
        help="With --cache-warm: warm an output only if its bytes are at most this "
             "fraction of lru_cache_bytes (engine default 0.25); label token -wf<percent>.",
    )
    parser.add_argument("--disk-cache-bytes", type=int, default=None,
                        help="Disk-cache tier capacity per writer in bytes (bench/PLAN-disk-cache.md); off when absent")
    parser.add_argument("--disk-cache-dir", default=None,
                        help="Root of the per-writer disk-cache dirs (default /tank/cache, the SSD mounted by setup_disk_cache.sh)")
    parser.add_argument("--disk-cache-keep-pages", action="store_true",
                        help="Do not drop the page cache after tier reads (A/B of the SSD cost)")
    parser.add_argument("--disk-cache-mode", choices=("file", "chunk"), default=None,
                        help="Tier entry unit: whole SSTables (file, the default) or disk_cache_entry_bytes chunks; label -ch<entry>")
    parser.add_argument("--disk-cache-entry-bytes", type=int, default=None,
                        help="Chunk size in chunk mode (power of two >= 4096, engine default 65536)")
    parser.add_argument("--disk-cache-admission", choices=("always", "frequency"), default=None,
                        help="always: evict to fit (default); frequency: TinyLFU contest for non-free budget; label -adm")
    parser.add_argument(
        "--corfu-client",
        choices=CORFU_CLIENTS,
        default=None,
        help="ozonedb-corfu only: which Corfu client every writer runs, written as "
             "corfu_client into each generated shared_config. native = the C++ client "
             "(PLAN-native-corfu.md, default), jni = the embedded JVM + CorfuBridge; "
             "native result files get a -native token. Prefer this over a ycsb.yaml edit.",
    )
    parser.add_argument(
        "--db_name",
        type=str,
        default=None,
        help="Comma-separated backends (overrides local.run.db_name), e.g. cassandra. "
             "Prefer this over editing ycsb.yaml on every client.",
    )
    parser.add_argument(
        "--cassandra_consistency",
        choices=sorted(CASSANDRA_MODES),
        default=None,
        help="Cassandra only: force the consistency mode (overrides cassandra.consistency "
             "in ycsb.yaml); result files are labelled cassandra-<mode>. The analogue of "
             "--linearizable for the baseline.",
    )
    parser.add_argument(
        "--lru-cache-bytes",
        type=int,
        default=None,
        help="ozonedb / ozonedb-corfu: per-process block cache in bytes, written as "
             "lru_cache_bytes into every generated shared_config; result files get a "
             "-lru64m style token (bench/PLAN-cost.md phase 2). Default: the base "
             "config's 512 MB, no token.",
    )
    parser.add_argument(
        "--record_cnt",
        type=int,
        default=None,
        help="Dataset size in records (overrides local.run.record_cnt); must equal "
             "what the load phase used. Prefer this over a ycsb.yaml edit.",
    )
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = _derive_addresses(yaml.safe_load(f))

    run_config = config["local"]["run"]
    workload_names = run_config["workload_name"]
    if args.workloads:
        workload_names = [w.strip() for w in args.workloads.split(",") if w.strip()]
    record_cnts = run_config["record_cnt"]
    if args.record_cnt is not None:
        record_cnts = args.record_cnt
    operation_cnts = run_config["operation_cnt"]
    key_sizes = run_config["key_size"]
    db_names = run_config["db_name"]
    if args.db_name:
        db_names = [d.strip() for d in args.db_name.split(",") if d.strip()]
    repeated = run_config["repeated"]
    ycsb_data_path = run_config["ycsb_data_path"]
    threads = run_config["threads"]
    num_writers = (
        args.num_writers
        if args.num_writers is not None
        else run_config.get("num_writers", 2)
    )
    offset = (
        args.offset
        if args.offset is not None
        else run_config.get("offset", 0)
    )
    total_writers = (
        args.total_writers
        if args.total_writers is not None
        else run_config.get("total_writers", num_writers)
    )
    corfu_settings = config.get("corfu")
    if args.log_trim:
        corfu_settings = log_trim_corfu_settings(corfu_settings)
    if args.cache_warm:
        corfu_settings = cache_warm_corfu_settings(
            corfu_settings, args.cache_warm_max_level, args.cache_warm_max_fraction
        )
    if args.disk_cache_bytes is not None:
        corfu_settings = disk_cache_corfu_settings(corfu_settings, args.disk_cache_bytes, args.disk_cache_dir, args.disk_cache_keep_pages,
                                                   mode=args.disk_cache_mode, entry_bytes=args.disk_cache_entry_bytes,
                                                   admission=args.disk_cache_admission)
    if args.corfu_client:
        corfu_settings = corfu_client_corfu_settings(corfu_settings, args.corfu_client)
    s3_settings = config.get("s3")
    max_exec_time = (
        args.max_exec_time
        if args.max_exec_time is not None
        else run_config.get("max_exec_time")
    )
    trial = (
        args.trial
        if args.trial is not None
        else run_config.get("trial", 1)
    )

    print(
        f"Launching {num_writers} parallel runner processes "
        f"(db={db_names}, offset={offset}, total_writers={total_writers}, trial={trial}, "
        f"read_mode={'linearizable' if args.linearizable else 'default'}, "
        f"log_trim={'on' if args.log_trim else 'yaml'}, "
        f"cache_warm={'on' if args.cache_warm else 'yaml'}, "
        f"corfu_client={args.corfu_client or 'yaml'}, "
        f"cassandra_consistency={args.cassandra_consistency or 'yaml'}, "
        f"record_cnt={record_cnts}, "
        f"lru_cache_bytes={args.lru_cache_bytes or 'base config'}, "
        f"disk_cache={disk_cache_label_token(corfu_settings) or 'off'})"
    )
    run_ycsb(
        workload_names,
        record_cnts,
        operation_cnts,
        key_sizes,
        db_names,
        repeated,
        ycsb_data_path,
        threads,
        num_writers,
        offset=offset,
        total_writers=total_writers,
        corfu_settings=corfu_settings,
        s3_settings=s3_settings,
        max_exec_time=max_exec_time,
        trial=trial,
        linearizable=args.linearizable,
        cassandra_settings=config.get("cassandra"),
        cassandra_consistency=args.cassandra_consistency,
        lru_cache_bytes=args.lru_cache_bytes,
    )
