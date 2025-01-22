import argparse
import os

import yaml

from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from create_azure_postgres import get_public_ip

from azure.mgmt.rdbms.mysql_flexibleservers import MySQLManagementClient
from azure.mgmt.rdbms.mysql_flexibleservers.models import Server, Sku, Storage, FirewallRule, ServerVersion

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision Azure Flexible Server for MySQL")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    if not subscription_id:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_group = config["resource_group"]["name"]
    vnet_name = config["network"]["vnet_name"]
    subnet_name = config["mysql"]["subnet_name"]
    server_name = config["mysql"]["server_name"]
    location = config["azure"]["location"]
    username = config["azure"]["username"]
    password = config["azure"]["password"]

    credential = AzureCliCredential()
    resource_client = ResourceManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    mysql_client = MySQLManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)


    # Create MySQL Flexible Server with private access
    print("Creating MySQL Flexible Server...")
    server_params = Server(
        location=location,
        sku=Sku(name=config["mysql"]["sku_name"], tier=config["mysql"]["sku_tier"]),
        administrator_login=username,
        administrator_login_password=password,
        storage=Storage(storage_size_gb=config["mysql"]["storage_size"]),
        version=ServerVersion.EIGHT0_21,
        create_mode="Create",
    )

    try:
        mysql_client.servers.begin_create(
            resource_group, server_name, server_params
        ).result()
    except Exception as e:
        print(f"Exception {e}")
    
    print("MySQL Flexible Server created")
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
            mysql_client.firewall_rules.begin_create_or_update(
                resource_group_name=config["resource_group"]["name"],
                server_name=config["mysql"]["server_name"],
                firewall_rule_name=firewall_rule_name,
                parameters=FirewallRule(
                    start_ip_address=start_ip_address,
                    end_ip_address=end_ip_address
                )
            ).result()

            print(f"Firewall rule '{firewall_rule_name}' created for IP {public_ip_address.ip_address}")

    local_ip_address = get_public_ip()

    print(f"Creating firewall rule 'LocalMachine_FirewallRule' for local machine...")
    mysql_client.firewall_rules.begin_create_or_update(
        resource_group_name=config["resource_group"]["name"],
        server_name=config["mysql"]["server_name"],
        firewall_rule_name='LocalMachine_FirewallRule',
        parameters=FirewallRule(
            start_ip_address=local_ip_address,
            end_ip_address=local_ip_address  # Same as start IP for a single IP rule
        )
    ).result()

    print(f"Firewall rule 'LocalMachine_FirewallRule' created for local IP {local_ip_address}")

    print(
        f"mySQL Flexible Server '{server_name}' created in resource group '{resource_group}'"
    )
