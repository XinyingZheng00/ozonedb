import subprocess
import os
import argparse
import yaml
from load_local_ycsb import (
    generate_config_for_ozonedb_local,
    generate_config_for_ozonedb_corfu,
    corfu_bridge_jar_path,
)
ozonedb_home = os.environ.get("OZONEDB_HOME")

def run_ycsb(workload_names, record_cnt, operation_cnts, key_size, db_names, repeated, ycsb_data_path, threads, corfu_settings=None, s3_settings=None, max_exec_time=None):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
    
    # Define paths based on OZONEDB_HOME
    record_cnt = str(record_cnt)
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/local")
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
                    cached_data_path = ycsb_data_path + "/" + "cached_data-"+f'{db_name}-{each_key_size}-{record_cnt}/'
                    # For corfu-backed ozonedb the post-load state lives on the
                    # remote Corfu stream, not in a local directory. Don't
                    # require cached_data_path to exist and don't copy it.
                    if db_name != "ozonedb-corfu" and not os.path.exists(cached_data_path):
                        print(f"cached_data_path {cached_data_path} does not exist, skipping...")
                        continue
                    run_data_path = ycsb_data_path + "/" + f'{db_name}-{each_key_size}-workload{workload_name}-{each_operation_cnt}/'
                    subprocess.run(['rm', '-rf', result_file_readwrite])
                    ycsb_db_name = "ozonedb" if db_name == "ozonedb-corfu" else db_name
                    command = ['python3', f'bin/ycsb', 'run', ycsb_db_name, '-threads', str(1), '-s','-P', workload_path, '-p', 'status.interval=1']
                    if max_exec_time:
                        command += ['-p', f'maxexecutiontime={int(max_exec_time)}']
                    if db_name == "rocksdb":
                        command.append("-p")
                        command.append("rocksdb.dir="+run_data_path)
                    elif db_name == "ozonedb":
                        config = generate_config_for_ozonedb_local(run_data_path)
                        command.append("-p")
                        command.append(f"shared_config={config}")
                    elif db_name == "ozonedb-corfu":
                        config = generate_config_for_ozonedb_corfu(run_data_path, corfu_settings, s3_settings)
                        command.append("-p")
                        command.append(f"shared_config={config}")
                        command.append("-cp")
                        command.append(corfu_bridge_jar_path())
                    elif db_name == "sqlite":
                        command[3] = "jdbc"
                        db_file = os.path.join(run_data_path, "mydb.db")
                        config_path = os.path.join(ozonedb_home, "bench/scripts/config/sqlite.properties")
                        with open(config_path, 'w') as f:
                            f.write("db.driver=org.sqlite.JDBC\n")
                            f.write(f"db.url=jdbc:sqlite:{db_file}\n")
                        command.append("-P")
                        command.append(config_path)
                        command.append("-cp")
                        command.append(ycsb_path + "/jdbc/target/dependency/sqlite-jdbc-3.49.1.0.jar")
                    
                    for _ in range(repeated):
                        if db_name == "ozonedb" or db_name == "ozonedb-corfu":
                            for thread in threads:
                                thread = str(thread)
                                subprocess.run(['rm', '-rf', run_data_path]) # clean cached data
                                if db_name == "ozonedb-corfu":
                                    # Corfu state is remote; just create an
                                    # empty local dir for metadata scratch.
                                    os.makedirs(run_data_path, exist_ok=True)
                                else:
                                    subprocess.run(['cp', '-r', cached_data_path, run_data_path])
                                result_file_readwrite = os.path.join(result_path, f'{each_key_size}-{each_operation_cnt}-{record_cnt}-workload{workload_name}-{db_name}_t{thread}.result')
                                subprocess.run(['rm', '-rf', result_file_readwrite]) # remove the result data if is exist
                                with open(result_file_readwrite, "a") as f:
                                    command[5] = thread
                                    print(" ".join(command))
                                    subprocess.run(command, stdout=f, stderr=f)
                                subprocess.run(['rm', '-rf', run_data_path])
                            continue
                    
                        subprocess.run(['rm', '-rf', run_data_path]) # clean db data
                        subprocess.run(['cp', '-r', cached_data_path, run_data_path]) 
                        with open(result_file_readwrite, "a") as f:
                            subprocess.run(command, stdout=f, stderr=f)
                        subprocess.run(['rm', '-rf', run_data_path])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YCSB tests with specified parameters.")

    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    
    args = parser.parse_args()
    json_path = args.config
    
    with open(json_path, 'r') as f:
        config = yaml.safe_load(f)
        
        load_config = config["local"]["run"]
        workload_names = load_config["workload_name"]
        record_cnts = load_config["record_cnt"]
        operation_cnts = load_config["operation_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        repeated = load_config["repeated"]
        ycsb_data_path = load_config["ycsb_data_path"]
        threads = load_config["threads"]
        corfu_settings = config.get("corfu")
        s3_settings = config.get("s3")
        max_exec_time = load_config.get("max_exec_time")

        run_ycsb(workload_names, record_cnts, operation_cnts, key_sizes, db_names, repeated, ycsb_data_path, threads, corfu_settings, s3_settings, max_exec_time)