import subprocess
import os
import argparse
import yaml
from load_local_ycsb import generate_config_for_ozonedb_local
ozonedb_home = os.environ.get("OZONEDB_HOME")

def run_ycsb(workload_names, record_cnt, operation_cnts, key_size, db_names, repeated, ycsb_data_path):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
    # Define paths based on OZONEDB_HOME
    record_cnt = str(record_cnt)
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/ycsb")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")

    os.chdir(ycsb_path) # enter into ycsb dir
    for each_operation_cnt in operation_cnts:
        each_operation_cnt = str(each_operation_cnt)
        for each_key_size in key_size:
            for workload_name in workload_names:
                subprocess.run(['python3', script_path +'/generate_workload.py', '--workload_name', workload_name,'--key_size', each_key_size, '--operation_cnt', each_operation_cnt, "--record_cnt", record_cnt]) #generate workload 
                workload_path = ycsb_path + "/workloads/generated_workloads/workload" + workload_name + "_" + each_key_size + "_" + each_operation_cnt + "_" + record_cnt
                for db_name in db_names:
                    result_file_readwrite = os.path.join(result_path, f'{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-readwrite.result')
                    result_client_log = os.path.join(result_path, f'{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}-readwrite-client.log')
                    cached_data_path = ycsb_data_path + "/" + "cached_data-"+f'{db_name}-{each_key_size}-workload{workload_name}-{record_cnt}/'
                    run_data_path = ycsb_data_path + "/" + f'{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}/'
                    subprocess.run(['rm', '-rf', result_file_readwrite]) 
                    subprocess.run(['rm', '-rf', result_client_log])
                    command = ['python3', f'bin/ycsb', 'run', db_name, '-s','-P', workload_path]
                    if db_name == "rocksdb":
                        command.append("-p")
                        command.append("rocksdb.dir="+run_data_path)
                    elif db_name == "ozonedb":
                        config = generate_config_for_ozonedb_local(run_data_path)
                        command.append("-p")
                        command.append(f"shared_config={config}")
                    command.append(f"-p statusinterval=1 2>&1 | tee -a {result_file_readwrite}")
                    for _ in range(repeated):
                        subprocess.run(['rm', '-rf', run_data_path]) # clean db data
                        subprocess.run(['cp', '-r', cached_data_path, run_data_path]) 
                        subprocess.run(command)
                        subprocess.run(['rm', '-rf', run_data_path]) # clean db data
                    subprocess.run(['mv', os.path.join(result_path, "client.log"), result_client_log]) 
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YCSB tests with specified parameters.")

    parser.add_argument(
        '--config',
        type=str,
        help='workload configuration.'
    )
    args = parser.parse_args()
    json_path = args.config
    with open(json_path, 'r') as f:
        config = yaml.safe_load(f)
        # Access parameters from config
        load_config = config["local"]["run"]
        workload_names = load_config["workload_name"]
        record_cnts = load_config["record_cnt"]
        operation_cnts = load_config["operation_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        repeated = load_config["repeated"]
        ycsb_data_path = load_config["ycsb_data_path"]

        # Run the YCSB function with the provided arguments
        run_ycsb(workload_names, record_cnts, operation_cnts, key_sizes, db_names, repeated, ycsb_data_path)