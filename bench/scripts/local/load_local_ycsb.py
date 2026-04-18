import subprocess
import os
import argparse
import yaml
import json
import sqlite3
import time
from datetime import datetime

"""
--rocksdb.write-buffer-size

The amount of data to build up in each in-memory buffer (backed by a log file) before closing the buffer and queuing it to be flushed into standard storage. Default: 64MiB. Larger values may improve performance, especially for bulk loads.

--rocksdb.max-write-buffer-number

The maximum number of write buffers that built up in memory. If this number is reached before the buffers can be flushed, writes will be slowed or stalled. Default: 2.
"""


ozonedb_home = os.environ.get("OZONEDB_HOME")


def generate_config_for_ozonedb_local(new_dir):
    json_file_path = ozonedb_home + "/src/config/local/shared_config_base.json"
    result_json_file_path = (
        ozonedb_home + "/src/config/local/shared_config.json"
    )

    with open(json_file_path, "r") as f:
        data = json.load(f)
    data["db_path"] = new_dir
    with open(result_json_file_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"db_path has been updated to: {data['db_path']}")
    return result_json_file_path


def corfu_bridge_jar_path():
    return os.path.join(
        ozonedb_home, "ozonedb-jni-maven/corfu-bridge/target/corfu-bridge-1.0-all.jar"
    )


def generate_config_for_ozonedb_corfu(new_dir, corfu_settings, s3_settings=None):
    json_file_path = ozonedb_home + "/src/config/corfu/shared_config_base.json"
    result_json_file_path = (
        ozonedb_home + "/src/config/corfu/shared_config.json"
    )

    with open(json_file_path, "r") as f:
        data = json.load(f)
    data["db_path"] = new_dir
    data["corfu_jar_path"] = corfu_bridge_jar_path()
    if corfu_settings:
        if "endpoint" in corfu_settings:
            data["corfu_endpoint"] = corfu_settings["endpoint"]
        if "stream_name" in corfu_settings:
            data["corfu_stream_name"] = corfu_settings["stream_name"]
        if "jvm_opts" in corfu_settings:
            data["corfu_jvm_opts"] = corfu_settings["jvm_opts"]
    if s3_settings:
        # Paper §3.5 split: SSTables go to a regular object store instead
        # of the Corfu shared log. Any S3-API service works (S3, MinIO,
        # R2, Wasabi, B2, SeaweedFS).
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
            data["s3_use_path_style"] = "true" if s3_settings["use_path_style"] else "false"
        if "sstable_dir" in s3_settings:
            data["sstable_dir"] = s3_settings["sstable_dir"]
    else:
        # Strip the SSTable-backend block so SSTables fall back to the main
        # backend (Corfu) if the operator removed `s3:` from ycsb.yaml.
        for k in ("sstable_backend", "s3_endpoint", "s3_region", "s3_bucket",
                  "s3_access_key", "s3_secret_key", "s3_use_path_style",
                  "sstable_dir"):
            data.pop(k, None)
    os.makedirs(os.path.dirname(result_json_file_path), exist_ok=True)
    with open(result_json_file_path, "w") as f:
        json.dump(data, f, indent=4)
    print(
        f"corfu config written to: {result_json_file_path} (db_path={data['db_path']}, endpoint={data['corfu_endpoint']}, sstable_backend={data.get('sstable_backend', 'corfu')})"
    )
    return result_json_file_path


def print_status(start_time, operations_done, total_operations, result_file_insert):
    elapsed = time.time() - start_time
    current_ops_sec = operations_done / elapsed if elapsed > 0 else 0
    est_completion = (
        (total_operations - operations_done) / current_ops_sec
        if current_ops_sec > 0
        else 0
    )
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")[:-3]
    with open(result_file_insert, "a") as res_f:
        res_f.write(
            f"{timestamp} {int(elapsed)} sec: {operations_done} operations; {current_ops_sec:.2f} current ops/sec; est completion in {int(est_completion)} seconds [INSERT: Count={operations_done}]\n"
        )


def parse_size(size_str):
    units = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
    if size_str[-2:] in units:
        return int(size_str[:-2]) * units[size_str[-2:]]
    return int(size_str)


def load_ycsb(
    record_cnt,
    key_size,
    db_names,
    ycsb_data_path,
    threads,
    repeated,
    corfu_settings=None,
    s3_settings=None,
):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")

    # Define paths based on OZONEDB_HOME
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/local")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")
    subprocess.run(["mkdir", "-p", result_path])

    os.chdir(ycsb_path)  # enter into ycsb dir
    for each_record_cnt in record_cnt:
        each_record_cnt = str(each_record_cnt)
        for each_key_size in key_size:
            # for load, the workload is set to a and operation_cnt is set to record_cnt
            subprocess.run(
                [
                    "python3",
                    script_path + "/generate_workload.py",
                    "--key_size",
                    each_key_size,
                    "--record_cnt",
                    each_record_cnt,
                    "--workload_name",
                    "a",
                    "--operation_cnt",
                    each_record_cnt,
                ]
            )  # generate workload
            workload_path = (
                ycsb_path
                + "/workloads/generated_workloads/"
                + "workloada"
                + "_"
                + each_key_size
                + "_"
                + each_record_cnt
                + "_"
                + each_record_cnt
            )

            for db_name in db_names:
                result_file_insert = os.path.join(
                    result_path,
                    f"{each_key_size}-{each_record_cnt}-insert-{db_name}_t1.result",
                )
                cached_data_path = os.path.join(
                    ycsb_data_path,
                    f"cached_data-{db_name}-{each_key_size}-{each_record_cnt}/",
                )
                subprocess.run(
                    ["rm", "-rf", result_file_insert]
                )  # remove the result data if is exist

                if db_name == "raw_sync_io":
                    data = b"a" * parse_size(each_key_size)
                    total_bytes = int(each_record_cnt) * parse_size(each_key_size)

                    for repeat_round in range(repeated):
                        file_path = os.path.join(
                            result_path,
                            f"sync_test_{each_key_size}_{each_record_cnt}_{repeat_round}.dat",
                        )
                        if os.path.exists(file_path):
                            os.remove(file_path)

                        print(
                            f"Starting raw sync IO round {repeat_round + 1}/{repeated}"
                        )
                        start_time = time.time()

                        ops_done = 0
                        status_interval = 10000

                        with open(file_path, "wb") as f:
                            for i in range(int(each_record_cnt)):
                                f.write(data)
                                f.flush()
                                os.fsync(f.fileno())
                                ops_done += 1
                                if ops_done % status_interval == 0 or ops_done == int(
                                    each_record_cnt
                                ):
                                    print_status(
                                        start_time,
                                        ops_done,
                                        int(each_record_cnt),
                                        result_file_insert,
                                    )

                        elapsed = time.time() - start_time
                        throughput = total_bytes / elapsed / (1024 * 1024)

                        with open(result_file_insert, "a") as res_f:
                            res_f.write(
                                f"[OVERALL], RunTime(ms), {int(elapsed * 1000)}\n"
                            )
                            res_f.write(
                                f"[OVERALL], Throughput(ops/sec), {ops_done / elapsed:.2f}\n"
                            )
                            res_f.write(f"[INSERT], Operations, {ops_done}\n")
                            res_f.write(
                                f"[INSERT], AverageLatency(us), {int(elapsed * 1e6 / ops_done)}\n"
                            )
                            res_f.write(f"[INSERT], Return=OK, {ops_done}\n")

                        os.remove(file_path)

                    continue

                ycsb_db_name = "ozonedb" if db_name == "ozonedb-corfu" else db_name
                command = [
                    "python3",
                    f"bin/ycsb",
                    "load",
                    ycsb_db_name,
                    "-threads",
                    str(1),
                    "-s",
                    "-P",
                    workload_path,
                ]
                if db_name == "rocksdb":
                    command.append("-p")
                    command.append("rocksdb.dir=" + cached_data_path)
                elif db_name == "ozonedb":
                    config = generate_config_for_ozonedb_local(cached_data_path)
                    command.append("-p")
                    command.append(f"shared_config={config}")
                elif db_name == "ozonedb-corfu":
                    config = generate_config_for_ozonedb_corfu(
                        cached_data_path, corfu_settings, s3_settings
                    )
                    command.append("-p")
                    command.append(f"shared_config={config}")
                    command.append("-cp")
                    command.append(corfu_bridge_jar_path())
                elif db_name == "sqlite":
                    command[3] = "jdbc"  # change to jdbc
                    # add url to db properties
                    config_path = os.path.join(
                        ozonedb_home, "bench/scripts/config/sqlite.properties"
                    )
                    db_file = os.path.join(cached_data_path, "mydb.db")
                    with open(config_path, "w") as f:
                        f.write("db.driver=org.sqlite.JDBC\n")
                        f.write(
                            f"db.url=jdbc:sqlite:{db_file}?journal_mode=WAL&cache_size=-163840&synchronous=FULL\n"
                        )
                    command.append("-P")
                    command.append(config_path)
                    command.append("-cp")
                    command.append(
                        ycsb_path + "/jdbc/target/dependency/sqlite-jdbc-3.49.1.0.jar"
                    )
                command.append("-p")
                command.append("status.interval=1")
                for _ in range(repeated):
                    print("Start round ", _)
                    subprocess.run(["rm", "-rf", cached_data_path])  # clean cached data
                    if db_name == "sqlite":
                        os.makedirs(cached_data_path, exist_ok=True)
                        with open(db_file, "w") as f:
                            print(f"Creating SQLite database at {db_file}")
                            pass
                        conn = sqlite3.connect(db_file)
                        cursor = conn.cursor()
                        # Page size. Default 4 KB.
                        # (128+32)MB/4KB
                        cursor.execute("PRAGMA cache_size = -163840;")
                        cursor.execute("PRAGMA synchronous = FULL;")
                        cursor.execute("PRAGMA journal_mode = WAL;")
                        cursor.execute(
                            "CREATE TABLE usertable ( \
                        YCSB_KEY VARCHAR(255),  \
                        FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, \
                        FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT);"
                        )
                        conn.commit()
                        conn.close()

                    if db_name == "ozonedb" or db_name == "ozonedb-corfu":
                        for thread in threads:
                            thread = str(thread)
                            subprocess.run(
                                ["rm", "-rf", cached_data_path]
                            )  # clean cached data
                            result_file_insert = os.path.join(
                                result_path,
                                f"{each_key_size}-{each_record_cnt}-insert-{db_name}_t{thread}.result",
                            )
                            subprocess.run(
                                ["rm", "-rf", result_file_insert]
                            )  # remove the result data if is exist
                            with open(result_file_insert, "a") as f:
                                command[5] = thread
                                print(" ".join(command))
                                subprocess.run(command, stdout=f, stderr=f)
                        continue

                    with open(result_file_insert, "a") as f:
                        print(" ".join(command))
                        subprocess.run(command, stdout=f, stderr=f)

                    if db_name == "sqlite":
                        # create index on the database
                        conn = sqlite3.connect(db_file)
                        cursor = conn.cursor()
                        cursor.execute(
                            "CREATE INDEX idx_ycsb_key ON usertable (YCSB_KEY);"
                        )
                        conn.commit()
                        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load YCSB tests with specified parameters."
    )

    parser.add_argument(
        "--config",
        type=str,
        default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml",
    )
    args = parser.parse_args()
    json_path = args.config

    with open(json_path, "r") as f:
        config = yaml.safe_load(f)

        load_config = config["local"]["load"]
        record_cnts = load_config["record_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        ycsb_data_path = load_config["ycsb_data_path"]
        threads = load_config["threads"]
        repeated = load_config["repeated"]
        corfu_settings = config.get("corfu")
        s3_settings = config.get("s3")
        os.makedirs(ycsb_data_path, exist_ok=True)

        load_ycsb(
            record_cnts,
            key_sizes,
            db_names,
            ycsb_data_path,
            threads,
            repeated,
            corfu_settings,
            s3_settings,
        )
