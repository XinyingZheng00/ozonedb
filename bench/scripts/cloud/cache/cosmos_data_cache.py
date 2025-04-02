from azure.cosmos import PartitionKey

# Configuration
ACCOUNT_URI = "your_account_uri"
ACCOUNT_KEY = "your_account_key"
DEFAULT_DB = 'ycsb'
DEFAULT_COLLECTION = 'usertable'
DEFAULT_PARTITION_KEY = "id"

def copy_cosmos_collection(client, source_container, dest_container):
    db = client.get_database_client(DEFAULT_DB)
    source_container = db.get_container_client(source_container)
    dest_container = clear_cosmos_collection(client, dest_container)

    # Copy items from source to destination
    item_count = 0
    for item in source_container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ):
        dest_container.create_item(body=item)
        item_count += 1
        if item_count % 100 == 0:  # Log progress every 100 items
            print(f"Copied {item_count} items")

    print(f"Copy completed. Total items copied: {item_count}")

    # Validate item count
    source_count = list(source_container.query_items(
        query="SELECT VALUE COUNT(1) FROM c",
        enable_cross_partition_query=True
    ))[0]
    dest_count = list(dest_container.query_items(
        query="SELECT VALUE COUNT(1) FROM c",
        enable_cross_partition_query=True
    ))[0]

    print(f"Source container item count: {source_count}")
    print(f"Destination container item count: {dest_count}")
    print(f"Copy {'successful' if source_count == dest_count else 'incomplete'}")

def clear_cosmos_collection(client, container):
    db = client.get_database_client(DEFAULT_DB)
    try:
        db.delete_container(container)
        print(f"Container '{container}' deleted.")
    except:
        print(f"Container '{container}' does not exist.")
        pass
    container = db.create_container_if_not_exists(
            id=container,
            partition_key=PartitionKey(path=f'/{DEFAULT_PARTITION_KEY}'),
        )
    return container

