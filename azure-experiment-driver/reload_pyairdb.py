import argparse
import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.storage import StorageManagementClient
from create_azure_vms import set_nested_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def server_exec(server, command, tmux_session=None, wait=True):
    if tmux_session is not None:
        if wait:
            command = f'tmux send -t {tmux_session} "{command}; tmux wait-for -S 0" ENTER'
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, "tmux wait-for 0"])
        else:
            command = f'tmux send -t {tmux_session} "{command}" ENTER'
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])
    else:
        subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])


def reload_vm(config, vm, network_client, storage_access_key):
    ip_name = str(vm.name).replace(
        config["vm"]["name"], config["network"]["ip_name"]
    )
    public_ip_address = network_client.public_ip_addresses.get(
        config["resource_group"]["name"], ip_name
    )
    print(f"Reloading pyairdb on {public_ip_address.ip_address}")
    server = f"{config['azure']['username']}@{public_ip_address.ip_address}"

    server_exec(server, "tmux kill-session -t airdb")
    server_exec(server, "tmux new -s airdb -d")

    server_exec(server, "conda create -y -n py311 python=3.11", tmux_session="airdb")
    server_exec(server, "conda activate py311", tmux_session="airdb")

    server_exec(server, "cd airdb/pyairdb; sudo apt install -y protobuf-compiler", tmux_session="airdb")
    server_exec(server, "pip install -r requirements.txt", tmux_session="airdb")
    server_exec(server, "bash compile_proto.sh", tmux_session="airdb")
    server_exec(server, "pip install --editable .", tmux_session="airdb")
    
    server_exec(server, f"export AZURE_STORAGE_ACCESS_KEY={storage_access_key}", tmux_session="airdb")
    server_exec(server,f"export AZURE_STORAGE_ACCOUNT_NAME={config['storage']['account_name']}",tmux_session="airdb")

    server_exec(server, "cd ..", tmux_session="airdb")
    server_exec(server, "cd ycsb-t", tmux_session="airdb")
    server_exec(server, "pip install websockets", tmux_session="airdb")

    server_exec(server, "python pyairdb_api.py 2>&1 | tee pyairdb_server_log/pyairdb_api_server.log", tmux_session="airdb", wait=False)
    server_exec(server, f'while ! nc -z localhost 11111; do sleep 10; echo "Waiting for WebSocket server at {server}..."; done; echo "WebSocket server is up!"')

    
    print(f"Reloaded pyairdb on {public_ip_address.ip_address}")


def reload_pyairdb_server(config):
    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]
    storage_account_name = config["storage"]["account_name"]

    storage_access_key = (
        storage_client.storage_accounts.list_keys(resource_group, storage_account_name)
        .keys[0]
        .value
    )
    if storage_access_key is None:
        raise ValueError(f"Storage account {storage_account_name} not found")

    vms = list(compute_client.virtual_machines.list(resource_group))
    vms = vms[:config["vm"]["num"]]
    with ThreadPoolExecutor(max_workers=len(vms)) as executor:
        future_to_vm = {
            executor.submit(reload_vm, config, vm, network_client, storage_access_key): vm for vm in vms
        }
        
        for future in as_completed(future_to_vm):
            vm = future_to_vm[future]
            try:
                future.result()
            except Exception as exc:
                print(f'{vm.name} generated an exception: {exc}')
            else:
                print(f'{vm.name} reloaded successfully.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup VMs with AirDB for ycsb-t benchmarking")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    parser.add_argument("--set", action="append", help="Override config options using dot notation (e.g., --set section.option=value)")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    if args.set:
        for override in args.set:
            key, value = override.split("=", 1)
            set_nested_config(config, key, yaml.safe_load(value))  # `yaml.safe_load` helps interpret types correctly

    reload_pyairdb_server(config)
