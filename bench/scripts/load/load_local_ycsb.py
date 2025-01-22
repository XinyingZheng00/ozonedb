import subprocess
import os
import argparse
import yaml
import json

ozonedb_home = os.environ.get("OZONEDB_HOME")

def generate_config_for_ozonedb_local(new_dir):
    json_file_path = ozonedb_home + "/src/config/local/shared_config_rocksdb_base.json"
    result_json_file_path = ozonedb_home + "/src/config/local/shared_config_rocksdb.json"

    with open(json_file_path, 'r') as f:
        data = json.load(f)
    data['db_path'] = new_dir
    with open(result_json_file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"db_path has been updated to: {data['db_path']}")
    return result_json_file_path

def load_ycsb(workload_names, record_cnt, operation_cnt, key_size, db_names, ycsb_data_path):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")

    # Define paths based on OZONEDB_HOME
    record_cnt = str(record_cnt)
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/ycsb")
    log_path = os.path.join(ozonedb_home, "bench", "results/")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")

    os.chdir(ycsb_path) # enter into ycsb dir
    for each_operation_cnt in operation_cnt:
        each_operation_cnt = str(each_operation_cnt)
        for each_key_size in key_size:
            for workload_name in workload_names:
                subprocess.run(['python3', script_path +'/generate_workload.py', '--workload_name', workload_name,'--key_size', each_key_size, '--operation_cnt', each_operation_cnt, "--record_cnt", record_cnt]) #generate workload 
                workload_path = ycsb_path + "/workloads/generated_workloads/workload" + workload_name + "_" + each_key_size + "_" + each_operation_cnt + "_" + record_cnt
                for db_name in db_names:
                    result_file_insert = os.path.join(result_path, f'{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-insert.result')
                    result_client_log = os.path.join(result_path, f'{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-insert-client.log')
                    cached_data_path = ycsb_data_path + "/" + "cached_data-"+f'{db_name}-{each_key_size}-workload{workload_name}-{record_cnt}/'
                    subprocess.run(['rm', '-rf', result_file_insert]) # remove the result data if is exist
                    subprocess.run(['rm', '-rf', result_client_log])
                    subprocess.run(['rm', '-rf', cached_data_path]) # clean cached data            
                    command = ["python3", f'bin/ycsb', 'load', db_name, '-threads', '64','-s', '-P', workload_path]
                    if db_name == "rocksdb":
                        command.append("-p")
                        command.append("rocksdb.dir="+cached_data_path)
                    elif db_name == "ozonedb":
                        config = generate_config_for_ozonedb_local(cached_data_path)
                        command.append("-p")
                        command.append(f"shared_config={config}")
                    # command.append(f"-p statusinterval=1")
                    # command.append(f" >> {result_file_insert}")
                    subprocess.run(command)
                    subprocess.run(['mv', os.path.join(log_path, "client.log"), result_client_log]) 
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load YCSB tests with specified parameters.")

    parser.add_argument(
        '--config',
        type=str,
        default='ycsb.yaml',
        help='workload configuration.'
    )
    args = parser.parse_args()
    json_path = args.config
    
    with open(json_path, 'r') as f:
        config = yaml.safe_load(f)

        # Access parameters from config
        load_config = config["local"]["load"]
        workload_names = load_config["workload_name"]
        record_cnts = load_config["record_cnt"]
        operation_cnts = load_config["operation_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        repeated = load_config["repeated"]
        ycsb_data_path = load_config["ycsb_data_path"]
        os.makedirs(ycsb_data_path, exist_ok=True)
        # Run the YCSB function with the provided arguments
        load_ycsb(workload_names, record_cnts, operation_cnts, key_sizes, db_names, ycsb_data_path)
