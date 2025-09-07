import subprocess
import os
import argparse
import yaml
from load_lazy_ycsb import server_exec

ozonedb_home = os.environ.get("OZONEDB_HOME")
data_shard_server = ["Xinying@hp075.utah.cloudlab.us", "Xinying@hp050.utah.cloudlab.us"]

def run_ycsb(workload_names, record_cnt, operation_cnts, key_size, db_names, repeated, threads):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
    
    # Define paths based on OZONEDB_HOME
    record_cnt = str(record_cnt)
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/lazylog")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")

    os.chdir(ycsb_path) # enter into ycsb dir
    for each_operation_cnt in operation_cnts:
        each_operation_cnt = str(each_operation_cnt)
        for each_key_size in key_size:
            for workload_name in workload_names:
                subprocess.run(['python3', script_path +'/generate_workload.py', '--workload_name', workload_name,'--key_size', each_key_size, '--operation_cnt', each_operation_cnt, "--record_cnt", record_cnt]) #generate workload 
                workload_path = ycsb_path + "/workloads/generated_workloads/workload" + workload_name + "_" + each_key_size + "_" + each_operation_cnt + "_" + record_cnt
                
                for db_name in db_names:
                    result_file_readwrite = os.path.join(result_path, f'{each_key_size}-{each_operation_cnt}-{record_cnt}-workload{workload_name}-{db_name}_t1.result')
                    cached_data_path = "/data/datalog" + "/" + "cached_data-"+f'{db_name}-{each_key_size}-{record_cnt}/'
                    subprocess.run(['rm', '-rf', result_file_readwrite])
                    command = ['python3', f'bin/ycsb', 'run', db_name, '-threads', str(1), '-s','-P', workload_path, '-p', 'status.interval=1']
                    if db_name == "lazykv":
                        command.insert(0, "sudo") 
                    for _ in range(repeated):
                        print("Start round ", _)
                        for node in data_shard_server:
                            server_exec(node, f"sudo cp {cached_data_path}/*.dat /data/datalog/")
                        with open(result_file_readwrite, "a") as f:
                            subprocess.run(command, stdout=f, stderr=f)
                        for node in data_shard_server:
                            server_exec(node, f"sudo rm -rf /data/datalog/*.dat")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YCSB tests with specified parameters.")

    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    
    args = parser.parse_args()
    json_path = args.config
    
    with open(json_path, 'r') as f:
        config = yaml.safe_load(f)
        
        load_config = config["lazy"]["run"]
        workload_names = load_config["workload_name"]
        record_cnts = load_config["record_cnt"]
        operation_cnts = load_config["operation_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        repeated = load_config["repeated"]
        threads = load_config["threads"]

        run_ycsb(workload_names, record_cnts, operation_cnts, key_sizes, db_names, repeated, threads)