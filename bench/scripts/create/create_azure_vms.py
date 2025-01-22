import argparse
import os

import requests
import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.network.models import NetworkSecurityGroup, SecurityRule
from azure.mgmt.resource import ResourceManagementClient


def get_public_ip():
    try:
        response = requests.get("https://api.ipify.org?format=json")
        ip = response.json()["ip"]
        return ip
    except requests.RequestException as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision VMs")
    parser.add_argument("--config", type=str, default="config/ycsb.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    credential = AzureCliCredential()

    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    location = config["azure"]["location"]
    resource_group_name = config["resource_group"]["name"]

    # Obtain the management objects for resources, networks, and compute
    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)

    # Provision the resource group
    print("Provisioning a resource group...")
    try:
        rg_result = resource_client.resource_groups.get(resource_group_name)
        print(f"Using existing resource group {rg_result.name} in the {rg_result.location} region")
    except:
        rg_result = resource_client.resource_groups.create_or_update(
            resource_group_name, {"location": location}
        )
        print(f"Provisioned resource group {rg_result.name} in the {rg_result.location} region")


    vnet_name = config["network"]["vnet_name"]
    subnet_name = config["network"]["subnet_name"]
    base_ip_name = config["network"]["ip_name"]
    ip_config_name = config["network"]["ip_config_name"]
    base_nic_name = config["network"]["nic_name"]

    print("Provisioning virtual network...")
    try:
        vnet_result = network_client.virtual_networks.get(resource_group_name, vnet_name)
        print(f"Using existing virtual network {vnet_result.name}")
    except:
        vnet_poller = network_client.virtual_networks.begin_create_or_update(
            resource_group_name,
            vnet_name,
            {
                "location": location,
                "address_space": {
                    "address_prefixes": [config["network"]["vnet_address_prefix"]]
                },
            },
        )
        vnet_result = vnet_poller.result()
        print(f"Provisioned virtual network {vnet_result.name}")

    # Provision the subnet
    print("Provisioning subnet...")
    try:
        subnet_result = network_client.subnets.get(
            resource_group_name, vnet_name, subnet_name
        )
        print(f"Using existing subnet {subnet_result.name}")
    except:
        subnet_poller = network_client.subnets.begin_create_or_update(
            resource_group_name,
            vnet_name,
            subnet_name,
            {"address_prefix": config["network"]["subnet_address_prefix"]},
        )
        subnet_result = subnet_poller.result()
        print(f"Provisioned subnet {subnet_result.name}")

    # Provision Network Security Group and Security Rule
    print("Provisioning Network Security Group...")

    my_ip = get_public_ip()
    nsg_name = "myNsg"
    try:
        nsg_result = network_client.network_security_groups.get(resource_group_name, nsg_name)
        print(f"Using existing Network Security Group {nsg_result.name}")
    except:
        # If NSG does not exist, create it
        print("Provisioning Network Security Group...")
        nsg_params = NetworkSecurityGroup(location=location)
        nsg_result = network_client.network_security_groups.begin_create_or_update(
            resource_group_name, nsg_name, nsg_params
        ).result()
        print(f"Provisioned Network Security Group {nsg_result.name}")

    # Check for an existing SSH rule and update or create as necessary
    ssh_rule_name = "AllowSSHFromMyIP"
    rules = nsg_result.security_rules
    ssh_rule = next((rule for rule in rules if rule.name == ssh_rule_name), None)

    # If SSH rule exists but IP has changed, update it
    if ssh_rule and ssh_rule.source_address_prefix != my_ip:
        print("Updating existing SSH rule with new IP...")
        ssh_rule.source_address_prefix = my_ip
    elif not ssh_rule:
        # If no existing rule for SSH from my IP, add it
        print("Adding new SSH rule for current IP...")
        access_ssh_rule = SecurityRule(
            name=ssh_rule_name,
            access="Allow",
            description="Allow SSH access from my IP",
            destination_address_prefix="*",
            destination_port_range="22",
            direction="Inbound",
            priority=1000,  # Ensure priority does not conflict with existing rules
            protocol="Tcp",
            source_address_prefix=my_ip,
            source_port_range="*",
        )
        rules.append(access_ssh_rule)

    nsg_params = NetworkSecurityGroup(location=location, security_rules=rules)
    nsg_result = network_client.network_security_groups.begin_create_or_update(
        resource_group_name, nsg_name, nsg_params
    ).result()
    print(f"Updated Network Security Group {nsg_result.name} with SSH access for current IP")

    # VM configuration from config
    num_vms = config["vm"]["num"]
    base_vm_name = config["vm"]["name"]
    username = config["azure"]["username"]
    password = config["azure"]["password"]
    vm_size = config["vm"]["vm_size"]
    image_reference = config["vm"]["image_reference"]
    ssh_key = config["vm"]["ssh_key"]

    ssh_key_data = open(os.path.expanduser(ssh_key)).read()

    try:
        vm_list = list(compute_client.virtual_machines.list(resource_group_name))
    except:
        vm_list = []


    for vm_index in range(len(vm_list), num_vms):
        vm_name = f"{base_vm_name}-{vm_index}"
        nic_name = f"{base_nic_name}-{vm_index}"
        ip_name = f"{base_ip_name}-{vm_index}"

        # Provision an IP address
        print("Provisioning IP address...")
        ip_poller = network_client.public_ip_addresses.begin_create_or_update(
            resource_group_name,
            ip_name,
            {
                "location": location,
                "sku": {"name": "Standard"},
                "public_ip_allocation_method": "Static",
                "public_ip_address_version": "IPV4",
            },
        )
        ip_address_result = ip_poller.result()
        print(f"Provisioned IP address {ip_address_result.name}")

        # Provision the network interface client
        print("Provisioning network interface...")
        nic_poller = network_client.network_interfaces.begin_create_or_update(
            resource_group_name,
            nic_name,
            {
                "location": location,
                "ip_configurations": [
                    {
                        "name": ip_config_name,
                        "subnet": {"id": subnet_result.id},
                        "public_ip_address": {"id": ip_address_result.id},
                    }
                ],
                "network_security_group": {"id": nsg_result.id},
            },
        )
        nic_result = nic_poller.result()
        print(f"Provisioned network interface {nic_result.name}")

        print(f"Provisioning virtual machine {vm_name}; this might take a few minutes.")
        vm_poller = compute_client.virtual_machines.begin_create_or_update(
            resource_group_name,
            vm_name,
            {
                "location": location,
                "storage_profile": {"image_reference": image_reference},
                "hardware_profile": {"vm_size": vm_size},
                "os_profile": {
                    "computer_name": vm_name,
                    "admin_username": username,
                    "admin_password": password,
                    "linux_configuration": {
                        "disable_password_authentication": False,
                        "ssh": {
                            "public_keys": [
                                {
                                    "path": f"/home/{username}/.ssh/authorized_keys",
                                    "key_data": ssh_key_data,
                                }
                            ]
                        }
                    }
                },
                "network_profile": {"network_interfaces": [{"id": nic_result.id}]},
            },
        )
        vm_result = vm_poller.result()
        print(f"Provisioned virtual machine {vm_result.name}, public IP address {ip_address_result.ip_address}")
