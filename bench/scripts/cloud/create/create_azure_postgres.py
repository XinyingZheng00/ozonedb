import argparse
import os

import yaml
import requests

from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.rdbms.postgresql_flexibleservers import PostgreSQLManagementClient
from azure.mgmt.rdbms.postgresql_flexibleservers.models import Server, Sku, Storage
from azure.mgmt.rdbms.postgresql_flexibleservers.models import FirewallRule

def get_public_ip():
    response = requests.get('https://httpbin.org/ip')
    return response.json()['origin']


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision Azure Flexible Server for PostgreSQL")
    parser.add_argument("--config", type=str, default="config/ycsb.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    if not subscription_id:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_group = config["resource_group"]["name"]
    vnet_name = config["network"]["vnet_name"]
    subnet_name = config["postgres"]["subnet_name"]
    server_name = config["postgres"]["server_name"]
    location = config["azure"]["location"]
    username = config["azure"]["username"]
    password = config["azure"]["password"]

    credential = AzureCliCredential()
    resource_client = ResourceManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    postgres_client = PostgreSQLManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)

    # Create PostgreSQL Flexible Server with private access
    print("Creating PostgreSQL Flexible Server...")
    server_params = Server(
        location=location,
        sku=Sku(name=config["postgres"]["sku_name"], tier=config["postgres"]["sku_tier"]),
        administrator_login=username,
        administrator_login_password=password,
        storage=Storage(storage_size_gb=config["postgres"]["storage_size"]),
        version="16",
        create_mode="Create",
    )

    postgres_client.servers.begin_create(
        resource_group, server_name, server_params
    ).result()
    
    # Create firewall rules for each VM's IP
    for vm in compute_client.virtual_machines.list(resource_group):
        ip_name = str(vm.name).replace(
            config["vm"]["name"], config["network"]["ip_name"]
        )
        public_ip_address = network_client.public_ip_addresses.get(
            resource_group, ip_name
        )
        
        if public_ip_address.ip_address:
            firewall_rule_name = f"{vm.name}_FirewallRule"
            start_ip_address = public_ip_address.ip_address
            end_ip_address = public_ip_address.ip_address  # Same as start IP for a single IP rule

            print(f"Creating firewall rule '{firewall_rule_name}' for VM {vm.name}...")
            postgres_client.firewall_rules.begin_create_or_update(
                resource_group_name=config["resource_group"]["name"],
                server_name=config["postgres"]["server_name"],
                firewall_rule_name=firewall_rule_name,
                parameters=FirewallRule(
                    start_ip_address=start_ip_address,
                    end_ip_address=end_ip_address
                )
            ).result()

            print(f"Firewall rule '{firewall_rule_name}' created for IP {public_ip_address.ip_address}")


    local_ip_address = get_public_ip()

    print(f"Creating firewall rule 'LocalMachine_FirewallRule' for local machine...")
    postgres_client.firewall_rules.begin_create_or_update(
        resource_group_name=config["resource_group"]["name"],
        server_name=config["postgres"]["server_name"],
        firewall_rule_name='LocalMachine_FirewallRule',
        parameters=FirewallRule(
            start_ip_address=local_ip_address,
            end_ip_address=local_ip_address  # Same as start IP for a single IP rule
        )
    ).result()

    print(f"Firewall rule 'LocalMachine_FirewallRule' created for local IP {local_ip_address}")

    print(
        f"PostgreSQL Flexible Server '{server_name}' created in resource group '{resource_group}'"
    )
