import subprocess
import os
import argparse
import yaml
import json
import sqlite3
import time
from datetime import datetime

data_shard_server = ["Xinying@hp075.utah.cloudlab.us", "Xinying@hp050.utah.cloudlab.us"]
ozonedb_home = os.environ.get("OZONEDB_HOME")

def server_exec(server, command, tmux_session=None, wait=True):
    """Executes a command on a remote server with optional tmux session support."""
    ssh_base_cmd = ["ssh", "-i", "~/.ssh/id_rsa", "-oStrictHostKeyChecking=no", "-p", "22", server]
    if tmux_session:
        if wait:
            command = f"tmux send -t {tmux_session} '{command}; tmux wait-for -S 0' ENTER"
            subprocess.run(ssh_base_cmd + [command])
            subprocess.run(ssh_base_cmd + ["tmux wait-for 0"])
        else:
            command = f"tmux send -t {tmux_session} '{command}' ENTER"
            subprocess.run(ssh_base_cmd + [command])
    else:
        subprocess.run(ssh_base_cmd + [command])

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

def print_status(start_time, operations_done, total_operations, result_file_insert):
    elapsed = time.time() - start_time
    current_ops_sec = operations_done / elapsed if elapsed > 0 else 0
    est_completion = (total_operations - operations_done) / current_ops_sec if current_ops_sec > 0 else 0
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]
    with open(result_file_insert, "a") as res_f:
        res_f.write(f"{timestamp} {int(elapsed)} sec: {operations_done} operations; {current_ops_sec:.2f} current ops/sec; est completion in {int(est_completion)} seconds [INSERT: Count={operations_done}]\n")


def parse_size(size_str):
    units = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
    if size_str[-2:] in units:
        return int(size_str[:-2]) * units[size_str[-2:]]
    return int(size_str)

def load_ycsb(record_cnt, key_size, db_names, threads, repeated):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")

    # Define paths based on OZONEDB_HOME
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/lazylog")
    script_path = os.path.join(ozonedb_home, "bench", "scripts")
    subprocess.run(['mkdir', '-p', result_path])

    os.chdir(ycsb_path) # enter into ycsb dir
    for each_record_cnt in record_cnt:
        each_record_cnt = str(each_record_cnt)
        for each_key_size in key_size:
            #for load, the workload is set to a and operation_cnt is set to record_cnt
            subprocess.run(['python3', script_path +'/generate_workload.py','--key_size', each_key_size, "--record_cnt", each_record_cnt, 
                            '--workload_name', "a", '--operation_cnt', each_record_cnt,
                            ]) #generate workload 
            workload_path = ycsb_path + "/workloads/generated_workloads/" + "workloada" + "_" + each_key_size + "_" + each_record_cnt +"_" + each_record_cnt
            
            for db_name in db_names:
                result_file_insert = os.path.join(result_path, f'{each_key_size}-{each_record_cnt}-insert-{db_name}_t1.result')
                cached_data_path = os.path.join('/data/datalog', f'cached_data-{db_name}-{each_key_size}-{each_record_cnt}/')
                subprocess.run(['rm', '-rf', result_file_insert]) # remove the result data if is exist

                command = ["python3", f'bin/ycsb', 'load', db_name, '-threads', str(1), '-s', '-P', workload_path, "-p", "status.interval=1"]
                if db_name == "lazykv":
                    command.insert(0, "sudo") 
                for _ in range(repeated):
                    print("Start round ", _)
                    for node in data_shard_server:
                        server_exec(node, f"sudo rm -rf /data/datalog/*.dat")
                    with open(result_file_insert, "a") as f:
                        print(" ".join(command))
                        subprocess.run(command, stdout=f, stderr=f)        
                # copy the data to cached_data_path
                for node in data_shard_server:
                    server_exec(node, f"sudo rm -rf {cached_data_path}")
                    server_exec(node, f"sudo mkdir -p {cached_data_path}")
                    server_exec(node, f"sudo cp /data/datalog/*.dat {cached_data_path}")
                    server_exec(node, f"sudo rm -rf /data/datalog/*.dat")
                
                                            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load YCSB tests with specified parameters.")

    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    args = parser.parse_args()
    json_path = args.config
    
    with open(json_path, 'r') as f:
        config = yaml.safe_load(f)
        
        load_config = config["lazy"]["load"]
        record_cnts = load_config["record_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        threads = load_config["threads"]
        repeated = load_config["repeated"]
        
        load_ycsb(record_cnts, key_sizes, db_names, threads, repeated)
