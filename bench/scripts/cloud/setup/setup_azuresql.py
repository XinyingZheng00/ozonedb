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
from azure.mgmt.sql import SqlManagementClient



logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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


def common_init(node, config):
    # server_exec(node, "sudo apt update")
    # server_exec(node, "sudo apt install -y maven") # for ycsb-t
    # server_exec(node, "sudo apt install python3-pip -y")
    # server_exec(node, "sudo apt install python-is-python3 -y")
    server_exec(node, "tmux kill-server")
    server_exec(node, "tmux new -s azuresql_setup -d")
    server_exec(node, "source ~/.profile", tmux_session="azuresql_setup")
    server_exec(node, "cd $OZONEDB_HOME/bench/scripts", tmux_session="azuresql_setup")
    # server_exec(node, "wget -O sqljdbc.tar.gz https://go.microsoft.com/fwlink/?linkid=2283563", tmux_session="azuresql_setup")
    # server_exec(node, "tar -xvzf sqljdbc.tar.gz", tmux_session="azuresql_setup")
    # server_exec(node, "rm sqljdbc.tar.gz", tmux_session="azuresql_setup")
    # server_exec(node, "pip install pyodbc", tmux_session="azuresql_setup")
    # server_exec(node, "curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc", tmux_session="azuresql_setup")
    # server_exec(node, "curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list", tmux_session="azuresql_setup")
    # server_exec(node, "sudo apt-get update", tmux_session="azuresql_setup")
    # server_exec(node, "sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18", tmux_session="azuresql_setup")
    # # optional: for bcp and sqlcmd
    # # server_exec(node, "sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18")
    # # server_exec(node, """echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc""")
    # # server_exec(node, "source ~/.bashrc")
    # # server_exec(node, "sudo apt install unixodbc-dev -y")

    # access_token = open(os.path.expanduser(config["github_token"])).read().strip()
#     username = config["azure"]["username"]
#     password = config["azure"]["password"]

    server = azuresql_client.servers.get(resource_group, config["azure_sql"]["server_name"]).fully_qualified_domain_name
    port = 1433
#     db_url = f"jdbc:sqlserver://{server}:{port};database=ycsb;sendStringParametersAsUnicode=false;disableStatementPooling=false;statementPoolingCacheSize=10;responseBuffering=full"

#     azure_sql_properties = f"""
# db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
# db.url={db_url}
# db.user={config['azure']['username']}
# db.passwd={config['azure']['password']}
# """
    # server_exec(node, f'echo "{azure_sql_properties}" > azuresql.properties', tmux_session="azuresql_setup")

    
# db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
# db.url=jdbc:sqlserver://ozonedb-azuresql-server.database.windows.net:1433;databaseName=ycsb;sendStringParametersAsUnicode=false;disableStatementPooling=false;statementPoolingCacheSize=10;responseBuffering=full
# db.user=ycsb
# db.passwd=strong-password-here
# db.batchsize=5000
# jdbc.batchupdateapi=true

    setup_command = f"python setup_db/setup_azuresql_db.py --server {server} --port {port} --user \"{config['azure']['username']}\" --password \"{config['azure']['password']}\""
    server_exec(node, setup_command, tmux_session="azuresql_setup")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup VMs with AirDB for ycsb-t benchmarking")
    parser.add_argument("--config", type=str, default="config/ycsb.yaml")
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
    azuresql_client = SqlManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]

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
