import json
import os

ozonedb_home = os.environ.get("OZONEDB_HOME")

def generate_config_for_ozonedb_cloud(new_dir):
    json_file_path = ""
    result_json_file_path = ""
    json_file_path = ozonedb_home + "/src/config/cloud/shared_config_rocksdb_base.json"
    result_json_file_path = ozonedb_home + "/src/config/cloud/shared_config_rocksdb.json"

    with open(json_file_path, 'r') as f:
        data = json.load(f)
    data['db_path'] = new_dir.lower()
    with open(result_json_file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"db_path has been updated to: {data['db_path']}")
    return result_json_file_path

import argparse
if __name__ == "__main__":
    #read from args
    parser = argparse.ArgumentParser()
    parser.add_argument("--new_dir", type=str)
    args = parser.parse_args()
    generate_config_for_ozonedb_cloud(args.new_dir)