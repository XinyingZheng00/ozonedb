import argparse
import logging
import os
import sys

import yaml
from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.rdbms.mysql_flexibleservers import MySQLManagementClient
from create_azure_vms import set_nested_config

import mysql.connector

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def check_cached_table(server, port, dbname, cached_table, username, password):
    """Check if the cached table exists in the database and count the rows."""
    conn = None
    try:
        conn = mysql.connector.connect(host=server, port=port, database=dbname, user=username, password=password)
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check if the cached table exists
            cur.execute(f"SHOW TABLES LIKE '{cached_table}';")
            table_exists = cur.fetchone()

            if not table_exists:
                print("Cache not found")
                return False, 0

            # Count the number of rows in the cached table
            cur.execute(f"SELECT COUNT(*) FROM {cached_table};")
            row_count = cur.fetchone()[0]
            print("Found cached")
            return True, row_count

    except Exception as e:
        print(f"An error occurred: {e}")
        return False, 0

    finally:
        if conn:
            conn.close()


def copy_table_from_cached_db(server, port, db_name, cached_table, original_table, username, password):
    """Copy data back from the cached table to the original table."""
    conn = mysql.connector.connect(host=server, port=port, database=db_name, user=username, password=password)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {original_table};")
        cur.execute(f"INSERT INTO {original_table} SELECT * FROM {cached_table};")
    conn.close()


def copy_table_to_cached_db(server, port, db_name, original_table, cached_table, username, password):
    """Copy data from the original table to a cached table within the same database."""
    conn = mysql.connector.connect(host=server, port=port, database=db_name, user=username, password=password)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {cached_table} LIKE {original_table};")
        cur.execute(f"TRUNCATE TABLE {cached_table};")
        cur.execute(f"INSERT INTO {cached_table} SELECT * FROM {original_table};")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check data cache for ycsb-t on Azure VMs and MySQL")
    parser.add_argument("--config", type=str, default="example-config.yaml")
    parser.add_argument("-cache_current", action="store_true")
    parser.add_argument("-load_cache", action="store_true")
    parser.add_argument(
        "--set", action="append", help="Override config options using dot notation (e.g., --set section.option=value)"
    )
    args = parser.parse_args()

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    if args.set:
        for override in args.set:
            key, value = override.split("=", 1)
            set_nested_config(config, key, yaml.safe_load(value))

    credential = AzureCliCredential()
    subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
    if subscription_id is None:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is not set")

    resource_client = ResourceManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    mysql_client = MySQLManagementClient(credential, subscription_id)

    resource_group = config["resource_group"]["name"]
    storage_account_name = config["storage"]["account_name"]

    ip = mysql_client.servers.get(resource_group, config["mysql"]["server_name"]).fully_qualified_domain_name
    port = 3305

    username = config["azure"]["username"]
    password = config["azure"]["password"]

    cached_table = "cached"
    table_name = "usertable"

    if args.load_cache:
        print("Loading cached data")
        cached = check_cached_table(ip, port, "ycsb", cached_table, username, password)
        if cached[0]:
            print(f"Loading cached data from {cached_table} to {table_name} with {cached[1]} rows")
            copy_table_from_cached_db(ip, port, "ycsb", cached_table, table_name, username, password)
            sys.exit(0)

    if args.cache_current:
        print("Caching current data")
        copy_table_to_cached_db(ip, port, "ycsb", table_name, cached_table, username, password)
        sys.exit(0)
