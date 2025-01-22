
import logging
from azure.storage.blob import BlobServiceClient, ContainerClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_container_client(storage_client: BlobServiceClient, container_name: str) -> ContainerClient:
    """Get the container client for a specific container."""
    return storage_client.get_container_client(container_name)

def check_cached_data(storage_client: BlobServiceClient, cached_data_path) -> bool:
    """Check if data is already cached in the data_cache directory."""
    container_client = get_container_client(storage_client, "ycsb-data")
    blobs = list_blobs_in_directory(container_client, cached_data_path)
    return len(blobs) > 0

def list_blobs_in_directory(container_client: ContainerClient, directory_name: str):
    """List all blobs in a specified directory."""
    return list(container_client.list_blobs(name_starts_with=directory_name))

def delete_blobs_in_directory(container_client: ContainerClient, directory_name: str):
    """Delete all blobs in the specified directory."""
    try:
        # Attempt to get properties of the container
        container_client.get_container_properties()
    except Exception as e:
        print(f"Container does not exist or could not be accessed: {e}")
        return
        
    blobs = list_blobs_in_directory(container_client, directory_name)
    for blob in blobs:
        container_client.delete_blob(blob.name)
    print(f"Deleted {len(blobs)} blobs in {directory_name}")

def copy_blobs_between_directories(container_client: ContainerClient, source_directory: str, target_directory: str):
    """Copy all blobs from the source directory to the target directory."""
    source_blobs = list_blobs_in_directory(container_client, source_directory)
    for blob in source_blobs:
        print(f"Copying {blob.name} to {target_directory}")
        target_blob_name = blob.name.replace(source_directory, target_directory, 1)
        source_blob_url = f"{container_client.primary_endpoint}/{blob.name}"
        target_blob_client = container_client.get_blob_client(target_blob_name)
        target_blob_client.start_copy_from_url(source_blob_url)

def load_cached_data(storage_client: BlobServiceClient, cached_data_dir: str, runtime_data_dir: str):
    """Copy data from data_cache to ycsbt_bench_dir after clearing ycsbt_bench_dir."""
    container_client = get_container_client(storage_client, "ycsb-data")

    delete_blobs_in_directory(container_client, runtime_data_dir)
    copy_blobs_between_directories(container_client, cached_data_dir, runtime_data_dir)
