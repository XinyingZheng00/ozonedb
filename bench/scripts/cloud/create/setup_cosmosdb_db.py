import os
import argparse
from azure.identity import AzureCliCredential
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceExistsError
import yaml

# Default values
DEFAULT_DB = 'ycsb'
DEFAULT_COLLECTION = 'usertable'
DEFAULT_PARTITION_KEY = "id"

def setup_cosmosdb(cosmos_client, db, collection, partition_key):
    try:
        # Create the database if it does not exist
        database = cosmos_client.create_database_if_not_exists(id=db)
        print(f"Database '{db}' checked/created successfully.")

        # Create the container (collection) with specified throughput (RUs) and partition key
        try:
            database.delete_container(collection)
        except:
            print(f"Container '{collection}' does not exist.")
            pass
        
        container = database.create_container_if_not_exists(
            id=collection,
            partition_key=PartitionKey(path=f'/{partition_key}'),
        )
        print(f"Container '{collection}' checked/created with partition key '{partition_key}'.")
        
    except CosmosResourceExistsError:
        print("Database and container already exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Setup PostgreSQL for benchmarking.')
    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    parser.add_argument('--db', default=DEFAULT_DB, help='Database name')
    parser.add_argument('--collection', default=DEFAULT_COLLECTION, help='Collection name')
    parser.add_argument('--partition_key', default=DEFAULT_PARTITION_KEY, help='Partition key')
    
    args = parser.parse_args()
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)
    resource_group = config["resource_group"]["name"]
    account_name = config["cosmosdb"]["account_name"]
    
    # Required environment variables
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    if not subscription_id :
        raise ValueError("Please set AZURE_SUBSCRIPTION_ID variable.")

    # Authentication and client setup
    credential = AzureCliCredential()
    cosmos_mgmt_client = CosmosDBManagementClient(credential, subscription_id)
    account = cosmos_mgmt_client.database_accounts.get(resource_group, account_name)
    cosmosdb_account_uri = account.document_endpoint
    # Get the primary key for Cosmos DB account
    keys = cosmos_mgmt_client.database_accounts.list_keys(resource_group, account_name)
    cosmosdb_account_key = keys.primary_master_key
    cosmos_client = CosmosClient(cosmosdb_account_uri, cosmosdb_account_key)

    print(cosmosdb_account_uri)
    print(cosmosdb_account_key)
    # Create the database
    setup_cosmosdb(cosmos_client, args.db, args.collection, args.partition_key)
    
    print("Setup complete.")