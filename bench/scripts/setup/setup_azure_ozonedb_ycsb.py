import argparse
import logging
import os
import subprocess
import threading

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

is_airdb_setup = False
lock = threading.Lock()

def server_exec(server, command, tmux_session=None, wait=True):
    if tmux_session is not None:
        if wait:
            command = f"tmux send -t {tmux_session} '{command}; tmux wait-for -S 0' ENTER"
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, "tmux wait-for 0"])
        else:
            command = f'tmux send -t {tmux_session} "{command}" ENTER'
            subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])
    else:
        subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server, command])


def common_init(node, cfg):
    # git clone ozonedb
    server_exec(node, "tmux new -s ozonedb_setup -d")
    # server_exec(node, 'echo -en "\n\n" | ssh-keygen -t rsa', tmux_session="ozonedb_setup")
    # server_exec(node, 'ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts',  tmux_session="ozonedb_setup")
    # access_token = cfg["github_token"]
    # server_exec(node, f'curl -H "Authorization: token {access_token}" --data "{{\\"title\\":\\"key:$(hostname)\\",\\"key\\":\\"$(cat ~/.ssh/id_rsa.pub)\\"}}" https://api.github.com/user/keys',  tmux_session="ozonedb_setup",  tmux_session="ozonedb_setup")

    # server_exec(node, f"git clone --branch={cfg['ozonedb_branch']} git@github.com:XinyingZheng00/ozonedb.git",  tmux_session="ozonedb_setup")
    # server_exec(node, f'cd ozonedb && git submodule update --init --recursive',  tmux_session="ozonedb_setup")
    # server_exec(node, "echo \"export OZONEDB_HOME=$(pwd)/ozonedb\" >> ~/.profile",  tmux_session="ozonedb_setup")
    server_exec(node, "source ~/.profile",  tmux_session="ozonedb_setup")
    # server_exec(node, "sudo apt update", tmux_session="ozonedb_setup")
    # server_exec(node, "sudo apt install -y cmake maven python3-pip zip pkg-config",  tmux_session="ozonedb_setup")
    
    server_exec(node, "cd $OZONEDB_HOME/src/include/ozonedb/protobuf && rm -rf *",  tmux_session="ozonedb_setup")
    server_exec(node, "mkdir -p $OZONEDB_HOME/build && cd $OZONEDB_HOME/build && cmake .. && make -j$(nproc)",  tmux_session="ozonedb_setup")

    # server_exec(node, "sudo apt install openjdk-8-jdk -y",  tmux_session="ozonedb_setup")
    # server_exec(node, 'echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.profile &&source ~/.profile&& echo "export PATH=$JAVA_HOME/bin:$PATH" >> ~/.profile',  tmux_session="ozonedb_setup")
    server_exec(node, "bash $OZONEDB_HOME/bench/scripts/update_jni.sh",  tmux_session="ozonedb_setup")
    server_exec(node, "tmux kill-server")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup VMs with Ozonedb for ycsb benchmarking")
    parser.add_argument("--config", type=str, default="config/ycsb.yaml", help="Path to the config YAML file")
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

    storage_access_key = (
        storage_client.storage_accounts.list_keys(resource_group, storage_account_name)
        .keys[0]
        .value
    )
    if storage_access_key is None:
        raise ValueError(f"Storage account {storage_account_name} not found")

    config["storage"]["access_key"] = storage_access_key
    print(config["storage"]["access_key"])
    print(config["storage"]["account_name"])

    vm_setup = []
    for vm in compute_client.virtual_machines.list(resource_group):
        ip_name = str(vm.name).replace(
            config["vm"]["name"], config["network"]["ip_name"]
        )
        public_ip_address = network_client.public_ip_addresses.get(
            config["resource_group"]["name"], ip_name
        )
        print(f"Starting setup for {public_ip_address.ip_address}")
        server = f"{config['azure']['username']}@{public_ip_address.ip_address}"
        vm_setup.append(threading.Thread(target=common_init, args=(server, config)))

    for thread in vm_setup:
        thread.start()

    for thread in vm_setup:
        thread.join()

    logger.info("Setup complete")
