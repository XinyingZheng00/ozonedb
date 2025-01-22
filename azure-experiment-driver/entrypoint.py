import argparse
import os

from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrypoint to run ycsb-t")
    parser.add_argument("--workload", type=str, default="workloadc")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--operation", type=str, oneof=["load", "run"], default="run")
    parser.add_argument("--operationcount", type=int, default=10000)
    parser.add_argument("--insertstart", type=int, default=0)
    parser.add_argument("--recordcount", type=int, default=10000)
    parser.add_argument("--db", type=str, oneof=["airdb", "postgresql", "mysql", "azuresql"], default="airdb")
    parser.add_argument("--username", type=str, default="dohyun1357")
    parser.add_argument("--password", type=str, default="dh12346!")
    parser.add_argument("--server_name", type=str, default="")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--resourcegroup", type=str, default="airdb")
    parser.add_argument("--storageaccount", type=str, default="airdbstorage")
    parser.add_argument("--container", type=int, default=0)
    args = parser.parse_args()

    credential = AzureCliCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")
    
    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    postgres_client = PostgreSQLManagementClient(credential, subscription_id)

    resource_group = args.resourcegroup
    storage_account_name = args.storageaccount
    access_key = storage_client.storage_accounts.list_keys(resource_group, storage_account_name).keys[0].value
    blob_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=access_key,
    )

    ycsb_dir = "./bin/ycsb"
    log_folder = f"./logs-{args.db}"
    run_command = f"{ycsb_dir} {args.operation}"
    export_file = f"{log_folder}/{args.workload}/{args.db}_{args.operation}_{args.operationcount}_{args.threads}_{args.container}.txt"
    latency_file = f"{log_folder}/{args.workload}/{args.db}_{args.operation}_{args.operationcount}_{args.threads}_{args.container}_latency.txt"
    ycsb_log_file = f"{log_folder}/{args.workload}/{args.db}_{args.operation}_{args.operationcount}_{args.threads}_{args.container}.log"

    if args.db == "airdb":
        if args.operation == "load":
            run_command += f" pyairdb -P workloads/{args.workload} -threads {args.threads} -s"
            run_command += f" -p insertstart={args.insertstart} -p insertcount={args.operationcount}"
            run_command += f" -p exportfile={export_file} -p statusinterval=1 -p latencyfile={latency_file} 2>&1 | tee {ycsb_log_file}"

        elif args.operation == "run":
            run_command += f" pyairdb -P ./workloads/{args.workload} -threads {args.threads} -s"
            run_command += f" -p operationcount={args.operationcount} -p recordcount={args.recordcount}"
            run_command += f" -p exportfile={export_file} -p statusinterval=1 -p latencyfile={latency_file} 2>&1 | tee {ycsb_log_file}"

        else:
            raise ValueError("Invalid operation type")
    elif args.db == "postgresql":

        ip = postgres_client.servers.get(resource_group, args.server_name).fully_qualified_domain_name
        port = 5432
        db_url = f"jdbc:postgresql://{ip}:{port}/ycsb"

        postgres_properties = f"""
db.url={db_url}
db.user={args.username}
db.passwd={args.password}
"""
        # write postgres.properties file
        with open("postgres.properties", "w") as f:
            f.write(postgres_properties)

        cached_table = "cached"
        table_name = "usertable"

        if args.operation == "load":
            run_command += f" jdbc -P workloads/{args.workload} -threads {args.threads} -s -P ./postgres.properties"
            run_command += f" -p insertstart={args.insertstart} -p insertcount={args.operationcount}"
            run_command += f" -p exportfile={export_file} -p statusinterval=1 -p latencyfile={latency_file} 2>&1 | tee {ycsb_log_file}"

        elif args.operation == "run":
            run_command += f" jdbc -P workloads/{args.workload} -threads {args.threads} -s -P ./postgres.properties"
            run_command += f" -p operationcount={args.operationcount} -p recordcount={args.recordcount}"
            run_command += f" -p exportfile={export_file} -p statusinterval=1 -p latencyfile={latency_file} 2>&1 | tee {ycsb_log_file}"
        
        else:
            raise ValueError("Invalid operation type")
    elif args.db == "mysql":
        
    else:
        raise ValueError("Invalid database type")
    
