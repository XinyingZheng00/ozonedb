import argparse
import logging
import os
import yaml
import sys
import subprocess
import threading
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient

sys.path.append('/home/xinying/Desktop/ozonedb/bench/scripts/cache')
from pyozonedb_data_cache import load_cached_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def server_exec(server, command, tmux_session=None, wait=True):
    """Executes a command on a remote server with optional tmux session support."""
    ssh_base_cmd = ["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server]
    if tmux_session:
        if wait:
            command = f"tmux send -t {tmux_session} '{command}; tmux wait-for -S 0' ENTER"
            subprocess.run(ssh_base_cmd + [command])
            subprocess.run(ssh_base_cmd + ["tmux wait-for 0"])
        else:
            command = f'tmux send -t {tmux_session} "{command}" ENTER'
            subprocess.run(ssh_base_cmd + [command])
    else:
        subprocess.run(ssh_base_cmd + [command])


def run_ycsb(node, i, ycsb_t_start, config, blob_client):
    workload_config = config["cloud"]["run"]
    workloads, record_cnt, operation_cnts, key_sizes, threads = (
        workload_config["workload_name"], str(workload_config["record_cnt"]), 
        workload_config["operation_cnt"], workload_config["key_size"], 
        workload_config["threads"]
    )
    
    ycsb_path = os.path.join("$OZONEDB_HOME", "ycsb")
    result_path = os.path.join("$OZONEDB_HOME", "bench", "results", "azure")
    script_path = os.path.join("$OZONEDB_HOME", "bench", "scripts")

    server_exec(node, "tmux kill-server")
    server_exec(node, "tmux new -s ozonedb_run -d")
    server_exec(node, f"mkdir -p {result_path}", tmux_session="ozonedb_run")
    server_exec(node, f"cd {ycsb_path}", tmux_session="ozonedb_run")

    for op_cnt in map(str, operation_cnts):
        for key_size in key_sizes:
            for workload in workloads:
                print(f"Generating workload for {workload} with key size {key_size} and operation count {op_cnt}")
                gen_command = f"python3 {script_path}/generate_workload.py --workload_name {workload} --key_size {key_size} --operation_cnt {op_cnt} --record_cnt {record_cnt}"
                server_exec(node, gen_command, tmux_session="ozonedb_run")
                workload_path = os.path.join(ycsb_path, "workloads/generated_workloads", f"workload{workload}_{key_size}_{op_cnt}_{record_cnt}")
                db = "ozonedb"
                run_result = os.path.join(result_path, f"{db}-{key_size}-workload{workload}-{op_cnt}-run.result")
                server_exec(node, "rm -rf" + run_result, tmux_session="ozonedb_run")
                
                run_data_path = f"usertable".lower() 
                gen_config_command = f"python3 {script_path}/generate_config_for_ozonedb.py --new_dir {run_data_path}/ --is_local 0"
                server_exec(node, gen_config_command, tmux_session="ozonedb_run")                    
                cached_data_path = f"cached-data-{db}-{key_size}-workload{workload}-{record_cnt}".lower()                   
                load_cached_data(blob_client, cached_data_path, run_data_path)
                command = ["python3", "bin/ycsb", "run", db,'-threads', f'{threads}', "-s", "-P", workload_path, "-p", f"shared_config=$OZONEDB_HOME/src/config/cloud/shared_config_rocksdb.json",  f"-p statusinterval=1 2>&1 | tee -a {run_result}"]
                
                ycsb_t_start.wait()
                server_exec(node, " ".join(command), tmux_session="ozonedb_run")
                    

def download_file_from_server(server, remote_file_path, local_destination_path):
    """Downloads a file from a remote server using SCP."""
    scp_command = ["scp", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", 
                   f"{server}:{remote_file_path}", local_destination_path]
    print(" ".join(scp_command))
    subprocess.run(scp_command)

def download_results(node, config):
    """Downloads experiment results from the server."""
    remote_result_path = "~/ozonedb/bench/results/azure"
    local_result_path = os.path.join('../', "results", "azure")
    import pathlib
    pathlib.Path(local_result_path).mkdir(parents=True, exist_ok=True)
    workload_config = config["cloud"]["load"]
    for op_cnt in map(str, workload_config["operation_cnt"]):
        for key_size in workload_config["key_size"]:
            for workload in workload_config["workload_name"]:
                db = "ozonedb"
                remote_insert_result = os.path.join(remote_result_path, f"{db}-{key_size}-workload{workload}-{op_cnt}-run.result")
                local_insert_result = os.path.join(local_result_path, f"{db}-{key_size}-workload{workload}-{op_cnt}-run.result")
                subprocess.run(["rm", "-rf", local_insert_result])
                download_file_from_server(node, remote_insert_result, local_insert_result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YCSB on Azure VMs and Ozonedb")
    parser.add_argument("--config", type=str, default="config/ycsb.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    credential = AzureCliCredential()
    subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
    if not subscription_id:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    # Azure resource clients
    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]
    storage_account_name = config["storage"]["account_name"]
    access_key = storage_client.storage_accounts.list_keys(resource_group, storage_account_name).keys[0].value
    blob_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=access_key,
    )

    # VM Setup
    resource_group = config["resource_group"]["name"]
    vm_list = list(compute_client.virtual_machines.list(resource_group))
    if len(vm_list) < config["vm"]["num"]:
        print(f"Only {len(vm_list)} VMs found, expected {config['vm']['num']}")
        sys.exit(1)
    vm_list = vm_list[:config["vm"]["num"]]

    # Threads for operations
    ycsb_run, result_downloader, ycsb_cleanup = [], [], []
    ycsb_t_start = threading.Event()
    
    for i, vm in enumerate(vm_list):
        ip_name = vm.name.replace(config["vm"]["name"], config["network"]["ip_name"])
        public_ip_address = network_client.public_ip_addresses.get(resource_group, ip_name)
        server = f"{config['azure']['username']}@{public_ip_address.ip_address}"
        
        ycsb_run.append(threading.Thread(target=run_ycsb, args=(server, i, ycsb_t_start, config, blob_client)))
        result_downloader.append(threading.Thread(target=download_results, args=(server, config)))
        # ycsb_cleanup.append(threading.Thread(target=cleanup, args=(server,)))

    print("Setup complete")
    
    # Run threads
    for t in ycsb_run: t.start()
    ycsb_t_start.set()
    for t in ycsb_run: t.join()

    print("Experiment complete")
    print("Downloading results")
    for t in result_downloader: t.start()
    for t in result_downloader: t.join()

    