import argparse
import os

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision VMs")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    parser.add_argument("-f", "--force", action="store_true", default=False)
    args = parser.parse_args()

    # Load configuration from YAML file
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

    resource_client = ResourceManagementClient(credential, subscription_id)
    rg_result = resource_client.resource_groups.get(config["resource_group"]["name"])

    if rg_result is None:
        raise ValueError(f"Resource group {config['resource_group']['name']} not found")
    
    if not args.force:
        response = input(
            f"Found {rg_result.name} in the {rg_result.location} region with resource ID {rg_result.id}\n"
            f"Are you sure you want to delete {rg_result.name}? (yes/no): "
        )
    else:
        response = "yes"

    # Check the user's response
    if response.lower() in ["yes", "y"]:
        print(f"Deleting {rg_result.name}...")
        rg_result = resource_client.resource_groups.begin_delete(
            config["resource_group"]["name"]
        )
        rg_result.wait()
        print(f"Resource group {config['resource_group']['name']} deleted.")
    else:
        print("Deletion cancelled.")
