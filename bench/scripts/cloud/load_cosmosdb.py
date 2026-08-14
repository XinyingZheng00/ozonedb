import argparse
import logging
import os
import subprocess
import sys
import threading
import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from azure.cosmos import CosmosClient

sys.path.append(os.environ.get("OZONEDB_HOME") + '/bench/scripts/cloud/cache')
from cosmos_data_cache import *

# Utility functions

def server_exec(server, command, tmux_session=None, wait=True):
    """Executes a command on a remote server with optional tmux session support."""
    ssh_base_cmd = ["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server]
    if tmux_session:
        if wait:
            command = f"tmux send -t {tmux_session} '{command}; tmux wait-for -S 0' ENTER"
            subprocess.run(ssh_base_cmd + [command])
            subprocess.run(ssh_base_cmd + ["tmux wait-for 0"])
        else:
            command = f"tmux send -t {tmux_session} '{command}' ENTER"
            subprocess.run(ssh_base_cmd + [command])
    else:
        subprocess.run(ssh_base_cmd + [command])


def load_ycsb(node, i, ycsb_t_start, config):
    workload_config = config["cloud"]["load"]
    record_cnt, key_sizes, threads, ycsb_data_path = (
        workload_config["record_cnt"], 
        workload_config["key_size"],
        workload_config["threads"],
        workload_config["ycsb_data_path"]
    )
    
    ycsb_path = os.path.join("$OZONEDB_HOME", "ycsb")
    result_path = os.path.join("$OZONEDB_HOME", "bench", "results", "cloud")
    script_path = os.path.join("$OZONEDB_HOME", "bench", "scripts")

    server_exec(node, "tmux kill-server")
    server_exec(node, "tmux new -s cosmosdb_load -d")
    server_exec(node, "source ~/.profile", tmux_session="cosmosdb_load")
    server_exec(node, f"mkdir -p {result_path}", tmux_session="cosmosdb_load")
    server_exec(node, f"cd {ycsb_path}", tmux_session="cosmosdb_load")
    

    for each_record_cnt in record_cnt:
        each_record_cnt = str(each_record_cnt)
        for each_key_size in key_sizes:
            gen_command = f"python3 {script_path}/generate_workload.py --workload_name a --key_size {each_key_size} --operation_cnt {each_record_cnt} --record_cnt {each_record_cnt}"
            server_exec(node, gen_command, tmux_session="cosmosdb_load")
            workload_path = os.path.join(ycsb_path, "workloads/generated_workloads", f"workloada_{each_key_size}_{each_record_cnt}_{each_record_cnt}")
            
            db_name = "azurecosmos"
            result_file_insert = os.path.join(result_path, f'{each_key_size}-{each_record_cnt}-insert-{db_name}_t{threads}.result')
            # server_exec(node, "rm -rf " + result_file_insert, tmux_session="cosmosdb_load")
            cached_data_path = f"cached-data-{db_name}-{each_key_size}-{each_record_cnt}".lower()
            table_name = "usertable"
            clear_cosmos_collection(cosmos_client, table_name)
            
            command = ["python3", "bin/ycsb", "load", db_name, '-threads', f'{threads}', "-s", "-P", workload_path, "-p", f"azure.account={account_name}", "-p", f"azurecosmos.primaryKey={cosmosdb_account_key}", "-p", f"azurecosmos.uri={cosmosdb_account_uri}", f"-p statusinterval=1 2>&1 | tee -a {result_file_insert}"]
            ycsb_t_start.wait()
            server_exec(node, " ".join(command), tmux_session="cosmosdb_load")
            copy_cosmos_collection(cosmos_client, table_name, cached_data_path)
            clear_cosmos_collection(cosmos_client, table_name)

def get_remote_ozonedb_home(server):
    """Fetches the OZONEDB_HOME environment variable from the remote server."""
    command = ["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no",
               server, "source .profile && echo $OZONEDB_HOME"]
   
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return result.stdout.strip()

def download_file_from_server(server, remote_file_path, local_destination_path):
    """Downloads a file from a remote server using SCP."""
    scp_command = ["scp", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", 
                   f"{server}:{remote_file_path}", local_destination_path]
    print(" ".join(scp_command))
    subprocess.run(scp_command)

def download_results(node, config):
    """Downloads experiment results from the server."""
    remote_ozonedb_home = get_remote_ozonedb_home(node)
    if not remote_ozonedb_home:
        raise RuntimeError(f"Failed to retrieve OZONEDB_HOME from {node}")
    remote_result_path = os.path.join(remote_ozonedb_home, "bench", "results", "cloud")
    local_result_path = os.path.join(os.environ.get("OZONEDB_HOME"),"bench", "results", "cloud")
    import pathlib
    pathlib.Path(local_result_path).mkdir(parents=True, exist_ok=True)
    workload_config = config["cloud"]["load"]
    for each_key_size in workload_config["key_size"]:
        for each_record_cnt in workload_config["record_cnt"]:
            db_name = "azurecosmos"
            threads = workload_config["threads"]
            remote_insert_result = os.path.join(remote_result_path, f'{each_key_size}-{each_record_cnt}-insert-{db_name}_t{threads}.result')
            local_insert_result = os.path.join(local_result_path, f'{each_key_size}-{each_record_cnt}-insert-{db_name}_t{threads}.result')
            subprocess.run(["rm", "-rf", local_insert_result])
            download_file_from_server(node, remote_insert_result, local_result_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load ycsb-t data on Azure VMs and Azuresql")
    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    
    resource_group = config["resource_group"]["name"]
    storage_account_name = config["storage"]["account_name"]
    username = config["azure"]["username"]
    # Was config["azure"]["password"] -- committed to a tracked file.
    password = os.environ.get("AZURE_VM_PASSWORD")
    if not password:
        raise EnvironmentError("AZURE_VM_PASSWORD is not set (was ycsb.yaml azure.password).")
    account_name = config["cosmosdb"]["account_name"]

    cosmos_mgmt_client = CosmosDBManagementClient(credential, subscription_id)
    account = cosmos_mgmt_client.database_accounts.get(resource_group, account_name)
    cosmosdb_account_uri = account.document_endpoint
    keys = cosmos_mgmt_client.database_accounts.list_keys(resource_group, account_name)
    cosmosdb_account_key = keys.primary_master_key
    cosmos_client = CosmosClient(cosmosdb_account_uri, cosmosdb_account_key)
    
    vm_list = list(compute_client.virtual_machines.list(resource_group))
    if len(vm_list) < config["vm"]["num"]:
        print(f"Only {len(vm_list)} VMs found, expected {config['vm']['num']}")
        sys.exit(1)
    vm_list = vm_list[: config["vm"]["num"]]
    
    # delete all results in all vms
    result_path = os.path.join("$OZONEDB_HOME", "bench", "results", "cloud")    
    workload_config = config["cloud"]["load"]
    record_cnt, key_sizes, repeated, threads = (
        workload_config["record_cnt"], 
        workload_config["key_size"], 
        workload_config["repeated"],
        workload_config["threads"]
    )
    for each_record_cnt in record_cnt:
        each_record_cnt = str(each_record_cnt)
        for each_key_size in key_sizes:
            for i, vm in enumerate(vm_list): 
                ip_name = vm.name.replace(config["vm"]["name"], config["network"]["ip_name"])
                public_ip_address = network_client.public_ip_addresses.get(resource_group, ip_name)
                server = f"{config['azure']['username']}@{public_ip_address.ip_address}"
                db_name = "azurecosmos"
                result_file_insert = os.path.join(result_path, f'{each_key_size}-{each_record_cnt}-insert-{db_name}_t{threads}.result')
                server_exec(server, "source ~/.profile && rm -rf " + result_file_insert)
    
    
    result_downloader = []
    for _ in range(repeated):
        print("Start Round", _ + 1)
        ycsb_t_run = []
        ycsb_t_start = threading.Event()
        
        for i, vm in enumerate(vm_list):
            ip_name = str(vm.name).replace(config["vm"]["name"], config["network"]["ip_name"])
            public_ip_address = network_client.public_ip_addresses.get(resource_group, ip_name)
            server = f"{config['azure']['username']}@{public_ip_address.ip_address}"
            
            ycsb_t_run.append(threading.Thread(target=load_ycsb, args=(server, i, ycsb_t_start, config)))
            if _ == repeated - 1:
                result_downloader.append(threading.Thread(target=download_results, args=(server, config)))

        print("Setup complete")

        for t in ycsb_t_run: t.start()
        ycsb_t_start.set()
        for t in ycsb_t_run: t.join()
        
        print("Experiment complete")
        print("Downloading results")
        for t in result_downloader: t.start()
        for t in result_downloader: t.join()
