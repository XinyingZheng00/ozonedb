import argparse
import logging
import os
import pathlib
import subprocess
import sys
import threading
import time

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.rdbms.mysql_flexibleservers import MySQLManagementClient
from create_azure_vms import set_nested_config
from mysql_data_cache import check_cached_table, copy_table_from_cached_db
from run_pyairdb_ycsb_t import extract_text_between_markers, extract_total_and_current_ops_per_sec
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def server_exec(server, command, tmux_session=None, wait=True):
    if tmux_session is not None:
        if wait:
            command = f'tmux send -t {tmux_session} "{command}; tmux wait-for -S 0" ENTER'
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])
            subprocess.run(
                ["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, "tmux wait-for 0"]
            )
        else:
            command = f'tmux send -t {tmux_session} "{command}" ENTER'
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])
    else:
        subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])


def download_file_from_server(server, remote_file_path, local_destination_path):
    scp_command = [
        "scp",
        "-i",
        "~/.ssh/id_rsa",
        "-oStrictHostKeyChecking=no",
        f"{server}:{remote_file_path}",
        local_destination_path,
    ]
    subprocess.run(scp_command)


def setup_ycsb_t(node):
    server_exec(node, "tmux new -s ycsbt -d")
    server_exec(
        node, "cd airdb/ycsb-t; mvn clean -pl com.yahoo.ycsb:jdbc-binding -am install -DskipTests", tmux_session="ycsbt"
    )


def run_ycsb_t(node, i, ycsb_t_start, config, workload_config):
    workload = workload_config["workload"]
    operation_count = workload_config["operationcount"]
    threadcount = workload_config["threadcount"]
    ycsb_dir = "./bin/ycsb"
    log_folder = "./logs-mysql"

    server_exec(node, f"mkdir -p {log_folder}/{workload}", tmux_session="ycsbt")
    server_exec(node, "mkdir -p mysql_log", tmux_session="ycsbt")

    export_file = f"{log_folder}/{workload}/run_{operation_count}_t{threadcount}_{i}.txt"
    ycsb_log_file = f"{log_folder}/{workload}/run_{operation_count}_t{threadcount}_{i}.log"
    if config["vm"]["num"] > 1:
        operation_count = int(operation_count / config["vm"]["num"])

    ycsb_t_command = f"""{ycsb_dir} run jdbc -P ./workloads/{workload} -threads {threadcount} -s\
 -P ./mysql.properties -p operationcount={operation_count} -p exportfile={export_file} -p statusinterval=1  2>&1 | tee {ycsb_log_file}"""

    print(f"{node}: {ycsb_t_command}")
    ycsb_t_start.wait()
    print(f"{node}: Starting ycsb-t")
    server_exec(node, ycsb_t_command, tmux_session="ycsbt")
    print(f"{node}: ycsb-t complete")


def download_results(node, i, config, workload_config, args):
    workload = workload_config["workload"]
    operation_count = workload_config["operationcount"]
    threadcount = workload_config["threadcount"]
    log_folder = "~/airdb/ycsb-t/logs-mysql"

    export_file = f"{log_folder}/{workload}/run_{operation_count}_t{threadcount}_{i}.txt"
    ycsb_log_file = f"{log_folder}/{workload}/run_{operation_count}_t{threadcount}_{i}.log"
    download_file_from_server(
        node,
        export_file,
        f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload}/mysql/run_{operation_count}_t{threadcount}_{i}.txt",
    )
    download_file_from_server(
        node,
        ycsb_log_file,
        f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload}/mysql/run_{operation_count}_t{threadcount}_{i}.log",
    )


def cleanup(node):
    server_exec(node, "tmux kill-session -t ycsbt")


def parse_metrics(file_path):
    """Parse metrics of interest from a file."""
    metrics = {"RunTime(ms)": [], "Throughput(ops/sec)": [], "Latency(ms/txn)": []}
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(", ")
            if len(parts) == 3 and parts[1] in metrics:
                metric, value = parts[1], float(parts[2])
                metrics[metric].append(value)
    return metrics


def aggregate_and_average_metrics(file_paths):
    """Aggregate and average metrics from multiple files."""
    aggregated_metrics = {"RunTime(ms)": [], "Throughput(ops/sec)": [], "Latency(ms/txn)": []}
    for file_path in file_paths:
        file_metrics = parse_metrics(file_path)
        for key in aggregated_metrics:
            aggregated_metrics[key].extend(file_metrics[key])

    # Calculating averages and sums
    average_runtime = sum(aggregated_metrics["RunTime(ms)"]) / len(aggregated_metrics["RunTime(ms)"])
    total_throughput = sum(aggregated_metrics["Throughput(ops/sec)"])
    average_latency = sum(aggregated_metrics["Latency(ms/txn)"]) / len(aggregated_metrics["Latency(ms/txn)"])

    return average_runtime, total_throughput, average_latency


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ycsb-t on Azure VMs and Mysql")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    parser.add_argument("--results_dir", type=str, default="results")
    parser.add_argument(
        "--set", action="append", help="Override config options using dot notation (e.g., --set section.option=value)"
    )
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    if args.set:
        for override in args.set:
            key, value = override.split("=", 1)
            set_nested_config(config, key, yaml.safe_load(value))

    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    mysql_client = MySQLManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]
    storage_account_name = config["storage"]["account_name"]

    ip = mysql_client.servers.get(resource_group, config["mysql"]["server_name"]).fully_qualified_domain_name
    port = 3306

    username = config["azure"]["username"]
    password = config["azure"]["password"]

    cached_table = "cached"
    table_name = "usertable"

    vm_list = list(compute_client.virtual_machines.list(resource_group))
    if len(vm_list) < config["vm"]["num"]:
        print(f"Only {len(vm_list)} VMs found, expected {config['vm']['num']}")
        sys.exit(1)
    vm_list = vm_list[: config["vm"]["num"]]

    for workload_config in config["ycsb_t"]:
        if not os.path.exists(
            f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload_config['workload']}/mysql"
        ):
            pathlib.Path(
                f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload_config['workload']}/mysql"
            ).mkdir(parents=True, exist_ok=True)

        vm_setup = []
        ycsb_t_run = []
        result_downloader = []
        ycsb_t_cleanup = []
        cached = check_cached_table(ip, port, "ycsb", cached_table, username, password)
        if cached:
            copy_table_from_cached_db(ip, port, "ycsb", cached_table, table_name, username, password)
            print("Loaded cached data")
            time.sleep(60 * 5)
        else:
            print("No cached dataset found, running ycsb-t without reloading cached data")

        ycsb_t_start = threading.Event()

        for i, vm in enumerate(vm_list):
            ip_name = str(vm.name).replace(config["vm"]["name"], config["network"]["ip_name"])
            public_ip_address = network_client.public_ip_addresses.get(config["resource_group"]["name"], ip_name)
            server = f"{config['azure']['username']}@{public_ip_address.ip_address}"
            print(f"Starting setup for {public_ip_address.ip_address}")
            vm_setup.append(threading.Thread(target=setup_ycsb_t, args=(server,)))
            ycsb_t_run.append(threading.Thread(target=run_ycsb_t, args=(server, i, ycsb_t_start, config, workload_config)))
            result_downloader.append(threading.Thread(target=download_results, args=(server, i, config, workload_config, args)))
            ycsb_t_cleanup.append(threading.Thread(target=cleanup, args=(server,)))

        for t in vm_setup:
            t.start()

        for t in vm_setup:
            t.join()

        print("Setup complete")

        for t in ycsb_t_run:
            t.start()

        print("Waiting for 10 seconds")
        time.sleep(10)
        print("Starting ycsb-t experiment")
        ycsb_t_start.set()

        for t in ycsb_t_run:
            t.join()
        print("Experiment complete")

        time.sleep(10)
        print("Downloading results")
        for t in result_downloader:
            t.start()

        for t in result_downloader:
            t.join()

        print("Running cleanup")
        for t in ycsb_t_cleanup:
            t.start()

        for t in ycsb_t_cleanup:
            t.join()

        # if config["vm"]["num"] > 1:
        print("Aggregating results")
        file_paths = []
        log_paths = []
        for i in range(config["vm"]["num"]):
            workload = workload_config["workload"]
            operation_count = workload_config["operationcount"]
            threadcount = workload_config["threadcount"]
            file_paths.append(
                f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload}/mysql/run_{operation_count}_t{threadcount}_{i}.txt"
            )
            log_paths.append(
                f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload}/mysql/run_{operation_count}_t{threadcount}_{i}.log"
            )

        average_runtime, total_throughput, average_latency = aggregate_and_average_metrics(file_paths)
        with open(
            f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload_config['workload']}/mysql/run_aggregate_results.txt",
            "w",
        ) as file:
            file.write(f"Average runtime: {average_runtime}\n")
            file.write(f"Total throughput: {total_throughput}\n")
            file.write(f"Average latency: {average_latency}\n")
        
        with open(
            f"./{args.results_dir}/results_{config['vm']['num']}vm/{workload_config['workload']}/mysql/run_log_results.csv",
            "w",
        ) as file:
            for i, log_file in enumerate(log_paths):
                log_text = open(log_file, "r").read()
                file.write(f"{extract_total_and_current_ops_per_sec(log_text)}\n")
