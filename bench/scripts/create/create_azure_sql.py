import argparse
import os

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.sql.models import Database, FirewallRule
from azure.core.exceptions import ResourceNotFoundError

from create_azure_postgres import get_public_ip

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision Azure SQL Database")
    parser.add_argument("--config", type=str, default="config/ycsb.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    if not subscription_id:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_group = config["resource_group"]["name"]
    server_name = config["azure_sql"]["server_name"]
    location = config["azure"]["location"]
    username = config["azure"]["username"]
    password = config["azure"]["password"]

    credential = AzureCliCredential()
    resource_client = ResourceManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    sql_client = SqlManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)

    # Check if the SQL Server exists
    try:
        server = sql_client.servers.get(resource_group_name=resource_group, server_name=server_name)
        print(f"Server '{server_name}' already exists.")
    except ResourceNotFoundError:
        print(f"Server '{server_name}' not found. Creating...")
        server_result = sql_client.servers.begin_create_or_update(
            resource_group_name=resource_group,
            server_name=server_name,
            parameters={
                'location': location,
                'administrator_login': username,
                'administrator_login_password': password,
                'version': '12.0',  # Specify the SQL Server version
            }
        ).result()
        print(f"Server '{server_name}' created successfully.")

    print("Creating Azure SQL Database...")
    database = sql_client.databases.begin_create_or_update(
        resource_group_name=resource_group,
        server_name=server_name,
        database_name="ycsb",
        parameters=Database(
            location=location,
            sku={
                "name": config["azure_sql"]["sku_name"],
                "tier": config["azure_sql"]["sku_tier"],
            },
            max_size_bytes=53687091200,  # 50 GB
            auto_pause_delay=60,  # 60 minutes
            min_capacity=1,  # 1 vCores
            read_scale="Disabled",
            zone_redundant=False,
        ),
    ).result()

    print("Azure SQL Database created successfully")

    # Create firewall rules for each VM's IP
    for vm in compute_client.virtual_machines.list(resource_group):
        ip_name = str(vm.name).replace(config["vm"]["name"], config["network"]["ip_name"])
        public_ip_address = network_client.public_ip_addresses.get(resource_group, ip_name)

        if public_ip_address.ip_address:
            firewall_rule_name = f"{vm.name}_FirewallRule"
            start_ip_address = public_ip_address.ip_address
            end_ip_address = public_ip_address.ip_address  # Same as start IP for a single IP rule

            print(f"Creating firewall rule '{firewall_rule_name}' for VM {vm.name}...")
            sql_client.firewall_rules.create_or_update(
                resource_group_name=config["resource_group"]["name"],
                server_name=config["azure_sql"]["server_name"],
                firewall_rule_name=firewall_rule_name,
                parameters=FirewallRule(start_ip_address=start_ip_address, end_ip_address=end_ip_address),
            )

            print(f"Firewall rule '{firewall_rule_name}' created for IP {public_ip_address.ip_address}")

    local_ip_address = get_public_ip()

    print(f"Creating firewall rule 'LocalMachine_FirewallRule' for local machine...")
    sql_client.firewall_rules.create_or_update(
        resource_group_name=config["resource_group"]["name"],
        server_name=config["azure_sql"]["server_name"],
        firewall_rule_name="LocalMachine_FirewallRule",
        parameters=FirewallRule(
            start_ip_address=local_ip_address, end_ip_address=local_ip_address  # Same as start IP for a single IP rule
        ),
    )

    print(f"Firewall rule 'LocalMachine_FirewallRule' created for local IP {local_ip_address}")

    print(f"Azure SQL Server '{server_name}' created in resource group '{resource_group}'")
