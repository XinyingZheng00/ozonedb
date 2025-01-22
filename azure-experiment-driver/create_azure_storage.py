import argparse
import os

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision Azure Storage Account")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    credential = AzureCliCredential()

    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_client = ResourceManagementClient(credential, subscription_id)

    location = config["azure"]["location"]
    resource_group_name = config["resource_group"]["name"]

    print("Provisioning a resource group...")
    try:
        rg_result = resource_client.resource_groups.get(resource_group_name)
        print(f"Using existing resource group {rg_result.name} in the {rg_result.location} region")
    except:
        rg_result = resource_client.resource_groups.create_or_update(
            resource_group_name, {"location": location}
        )
        print(f"Provisioned resource group {rg_result.name} in the {rg_result.location} region")

    
    storage_client = StorageManagementClient(credential, subscription_id)
    storage_account_name = config["storage"]["account_name"]

    availability_result = storage_client.storage_accounts.check_name_availability(
        { "name": storage_account_name }
    )

    if not availability_result.name_available:
        print(f"Storage name {storage_account_name} is already in use. Try another name.")
        exit()

    poller = storage_client.storage_accounts.begin_create(resource_group_name, storage_account_name,
        {
            "location": location,
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"}, 
        }
    )
    # poller = storage_client.storage_accounts.begin_create(resource_group_name, storage_account_name,
    #     {
    #         "location" : location,
    #         "kind": "BlockBlobStorage",
    #         "sku": {"name": "Premium_LRS"},
    #     }
    # )

    account_result = poller.result()
    print(f"Provisioned storage account {account_result.name}")
    keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)
    print(f"Primary key for storage account: {keys.keys[0].value}")

