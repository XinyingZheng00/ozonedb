import argparse
import logging
import os
import subprocess
import threading
import time

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.rdbms.mysql_flexibleservers import MySQLManagementClient
from mysql_data_cache import check_cached_table


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

is_mysql_setup = False
lock = threading.Lock()

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


def common_init(node, cfg, mysql_properties, setup_command):
    server_exec(node, "sudo apt update")
    server_exec(node, "sudo apt install -y maven") # for ycsb-t
    server_exec(node, "sudo apt install python3-pip -y")
    server_exec(node, "sudo apt install mysql-client -y")
    server_exec(node, "sudo apt install python-is-python3 -y")
    server_exec(node, "pip install mysql-connector-python")
    server_exec(node, "tmux kill-server")

    access_token = open(os.path.expanduser(cfg["github_token"])).read().strip()

    # server_exec(node, 'echo -en "\n\n" | ssh-keygen -t rsa')
    # server_exec(node, 'ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts')
    # server_exec(node, f'curl -H "Authorization: token {access_token}" --data \'{{"title":"key:$(hostname)","key":"$(cat ~/.ssh/id_rsa.pub)"}}\' https://api.github.com/user/keys')
    # server_exec(node, f"git clone --branch={cfg['airdb_branch']} github.com:illinoisdata/airdb.git")
    server_exec(node, f"git clone --branch={cfg['airdb_branch']} https://{cfg['github_username']}:{access_token}@github.com/illinoisdata/airdb.git")
    server_exec(node, f"cd airdb; git reset --hard; git checkout {cfg['airdb_branch']}; git pull --rebase")

    cached_table = "cached"
    username = cfg["azure"]["username"]
    password = cfg["azure"]["password"]

    server_exec(node, f"echo '{mysql_properties}' > airdb/ycsb-t/mysql.properties")
    cached, _ = check_cached_table(ip, port, "ycsb", cached_table, username, password)
    if cached:
        print("cached, not running setup")
        return
    time.sleep(10)
    global is_mysql_setup, lock
    with lock:
        if not is_mysql_setup:
            print(f"Setting up mysql with {node}", flush=True)
            server_exec(node, setup_command)
            is_mysql_setup = True
        else:
            print(f"Skipping mysql setup with {node}", flush=True)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup VMs with AirDB for ycsb-t benchmarking")
    parser.add_argument("--config", type=str, default="example-config.yaml")
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
    mysql_client = MySQLManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]

    ip = mysql_client.servers.get(resource_group, config["mysql"]["server_name"]).fully_qualified_domain_name
    port = 3306
    db_url = f"jdbc:mysql://{ip}:{port}/ycsb"
    # upload mysql.properties to each vm
    mysql_properties = f"""
db.url={db_url}
db.user={config['azure']['username']}
db.passwd={config['azure']['password']}
"""

    setup_command = f"python airdb/ycsb-t/setup_mysql_db.py --ip {ip} --port {port} --user {config['azure']['username']} --password {config['azure']['password']}"

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
        vm_setup.append(threading.Thread(target=common_init, args=(server, config, mysql_properties, setup_command)))

    for thread in vm_setup:
        thread.start()

    for thread in vm_setup:
        thread.join()

    logger.info("Setup complete")
