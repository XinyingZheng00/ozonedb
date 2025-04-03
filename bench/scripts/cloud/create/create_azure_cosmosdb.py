import argparse
import os
import yaml
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup VMs with Ozonedb for ycsb benchmarking")
    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    args = parser.parse_args()
    
    # Load the configuration file
    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)
    account_name = config["cosmosdb"]["account_name"]
    resource_group = config["resource_group"]["name"]
    consistency_level = config["cosmosdb"]["consistency_level"]
    location = config["azure"]["location"]
    mode = config["cosmosdb"]["mode"]
    if mode == "serverless":
        capabilities = "EnableServerless"
    elif mode == "provisioned":
        capabilities = "EnableProvisioned"
    
    command = f"az cosmosdb create --name {account_name} \
--resource-group {resource_group} \
--default-consistency-level {consistency_level} \
--locations regionName={location} \
failoverPriority=0 isZoneRedundant=False \
--capabilities {capabilities}"
    print(f"Please creating Azure CosmosDB account with command: {command}")
    # subprocess.run(command, shell=True, check=True)

        