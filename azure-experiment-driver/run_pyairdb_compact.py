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

from reload_pyairdb import reload_pyairdb_server

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ycsb-t on Azure VMs and PyAirDB")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential,subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]
    storage_account_name = config["storage"]["account_name"]

    vm_list = list(compute_client.virtual_machines.list(resource_group))
    vm = vm_list[0]
    ip_name = str(vm.name).replace(config["vm"]["name"], config["network"]["ip_name"])
    public_ip_address = network_client.public_ip_addresses.get(resource_group, ip_name)
    server = f"{config['azure']['username']}@{public_ip_address.ip_address}"

    server_exec(server, f"cd ./airdb/ycsb-t; conda activate py311; python compact.py;")

    time.sleep(10)

    reload_pyairdb_server(config)