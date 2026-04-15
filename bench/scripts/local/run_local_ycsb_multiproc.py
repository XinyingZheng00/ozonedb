import subprocess
import os
import argparse
import yaml
import time

from load_local_ycsb import (
    generate_config_for_ozonedb_local,
    corfu_bridge_jar_path,
)
from load_local_ycsb_multiproc import (
    _make_corfu_config_per_writer,
    partition_records,
    spawn_parallel,
    write_aggregate,
)

"""
Multi-process YCSB runner that emulates distributed readers/writers by
spawning N YCSB processes in parallel against the dataset loaded by
load_local_ycsb_multiproc.py.

Operation space is split across writers: each process executes
operationcount / N operations over the full recordcount key range.
For ozonedb-corfu all processes share the Corfu stream; for other
backends each process reads from its own per-writer cached dir.

After all processes exit, per-writer results are aggregated into:
  {ks}-{opcnt}-{rc}-workload{w}-{db}_agg_w{N}_t{thread}.result

Set `num_writers` under `local.run` in ycsb.yaml, or pass --num_writers.
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
    corfu_settings=None,
    s3_settings=None,
):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
    if num_writers < 1:
        raise ValueError("num_writers must be >= 1")

    record_cnt = str(record_cnt)
    total_records = int(record_cnt)
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/local")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")

    os.chdir(ycsb_path)
    for each_operation_cnt in operation_cnts:
        each_operation_cnt = str(each_operation_cnt)
        total_ops = int(each_operation_cnt)
        op_partitions = partition_records(total_ops, num_writers)

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
                    if db_name in ("ozonedb", "ozonedb-corfu"):
                        thread_list = [str(t) for t in threads]
                    else:
                        thread_list = ["1"]

                    for _round in range(repeated):
                        for thread in thread_list:
                            jobs = []
                            per_writer_files = []
                            for writer_idx, (_, writer_ops) in enumerate(op_partitions):
                                cached_data_path = os.path.join(
                                    ycsb_data_path,
                                    f"cached_data-{db_name}-{each_key_size}-{record_cnt}-w{writer_idx}/",
                                )
                                run_data_path = os.path.join(
                                    ycsb_data_path,
                                    f"{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-w{writer_idx}/",
                                )

                                if db_name != "ozonedb-corfu":
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

                                ycsb_db_name = (
                                    "ozonedb"
                                    if db_name == "ozonedb-corfu"
                                    else db_name
                                )
                                cmd = [
                                    "python3",
                                    "bin/ycsb",
                                    "run",
                                    ycsb_db_name,
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

                                if db_name == "rocksdb":
                                    cmd += ["-p", f"rocksdb.dir={run_data_path}"]
                                elif db_name == "ozonedb":
                                    cfg = generate_config_for_ozonedb_local(run_data_path)
                                    cmd += ["-p", f"shared_config={cfg}"]
                                elif db_name == "ozonedb-corfu":
                                    cfg = _make_corfu_config_per_writer(
                                        writer_idx,
                                        run_data_path,
                                        corfu_settings,
                                        s3_settings,
                                    )
                                    cmd += [
                                        "-p",
                                        f"shared_config={cfg}",
                                        "-cp",
                                        corfu_bridge_jar_path(),
                                    ]
                                elif db_name == "sqlite":
                                    db_file = os.path.join(run_data_path, "mydb.db")
                                    sqlite_cfg = os.path.join(
                                        ozonedb_home,
                                        f"bench/scripts/config/sqlite_w{writer_idx}.properties",
                                    )
                                    with open(sqlite_cfg, "w") as pf:
                                        pf.write("db.driver=org.sqlite.JDBC\n")
                                        pf.write(f"db.url=jdbc:sqlite:{db_file}\n")
                                    cmd[3] = "jdbc"
                                    cmd += [
                                        "-P",
                                        sqlite_cfg,
                                        "-cp",
                                        os.path.join(
                                            ycsb_path,
                                            "jdbc/target/dependency/sqlite-jdbc-3.49.1.0.jar",
                                        ),
                                    ]

                                result_file = os.path.join(
                                    result_path,
                                    f"{each_key_size}-{each_operation_cnt}-{record_cnt}-workload{workload_name}-{db_name}_w{writer_idx}of{num_writers}_t{thread}.result",
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

                            agg_file = os.path.join(
                                result_path,
                                f"{each_key_size}-{each_operation_cnt}-{record_cnt}-workload{workload_name}-{db_name}_agg_w{num_writers}_t{thread}.result",
                            )
                            write_aggregate(
                                agg_file, per_writer_files, wall_ms, num_writers
                            )

                            for writer_idx in range(num_writers):
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
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    run_config = config["local"]["run"]
    workload_names = run_config["workload_name"]
    record_cnts = run_config["record_cnt"]
    operation_cnts = run_config["operation_cnt"]
    key_sizes = run_config["key_size"]
    db_names = run_config["db_name"]
    repeated = run_config["repeated"]
    ycsb_data_path = run_config["ycsb_data_path"]
    threads = run_config["threads"]
    num_writers = (
        args.num_writers
        if args.num_writers is not None
        else run_config.get("num_writers", 2)
    )
    corfu_settings = config.get("corfu")
    s3_settings = config.get("s3")

    print(f"Launching {num_writers} parallel runner processes")
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
        corfu_settings,
        s3_settings,
    )
