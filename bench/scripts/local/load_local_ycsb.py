import subprocess
import os
import argparse
import yaml
import json
import sqlite3

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

def load_ycsb(record_cnt, key_size, db_names, ycsb_data_path, threads, repeated):
    if not ozonedb_home:
        raise EnvironmentError("OZONEDB_HOME environment variable is not set.")

    # Define paths based on OZONEDB_HOME
    ycsb_path = os.path.join(ozonedb_home, "ycsb")
    result_path = os.path.join(ozonedb_home, "bench", "results/local")
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
                result_file_insert = os.path.join(result_path, f'{each_key_size}-{each_record_cnt}-insert-{db_name}_t{threads}.result')
                cached_data_path = os.path.join(ycsb_data_path, f'cached_data-{db_name}-{each_key_size}-{each_record_cnt}/')
                subprocess.run(['rm', '-rf', result_file_insert]) # remove the result data if is exist
                command = ["python3", f'bin/ycsb', 'load', db_name, '-threads', str(threads), '-s', '-P', workload_path]
                if db_name == "rocksdb":
                    command.append("-p")
                    command.append("rocksdb.dir="+cached_data_path)
                elif db_name == "ozonedb":
                    config = generate_config_for_ozonedb_local(cached_data_path)
                    command.append("-p")
                    command.append(f"shared_config={config}")
                elif db_name == "sqlite":
                    command[3] = "jdbc" # change to jdbc
                    os.makedirs(cached_data_path, exist_ok=True)
                    db_file = os.path.join(cached_data_path, "mydb.db")
                    with open(db_file, "w") as f:
                        print(f"Creating SQLite database at {db_file}")
                        pass
                    conn = sqlite3.connect(db_file)
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA cache_size = 40960;")
                    cursor.execute("PRAGMA synchronous = FULL;") # vs OFF
                    cursor.execute("CREATE TABLE usertable ( \
	                YCSB_KEY VARCHAR(255) PRIMARY KEY,  \
                    FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, \
                    FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT);")
                    conn.commit()
                    conn.close()                
                    #add url to db properties
                    config_path = os.path.join(ozonedb_home, "bench/scripts/config/sqlite.properties")
                    with open(config_path, 'w') as f:
                        f.write("db.driver=org.sqlite.JDBC\n")
                        f.write(f"db.url=jdbc:sqlite:{db_file}\n")
                    command.append("-P")
                    command.append(config_path)
                    command.append("-cp")
                    command.append(ycsb_path + "/jdbc/target/dependency/sqlite-jdbc-3.49.1.0.jar")
                # command.append(f"-p statusinterval=1")
                for _ in range(repeated):
                    subprocess.run(['rm', '-rf', cached_data_path]) # clean cached data  
                    with open(result_file_insert, "a") as f:
                        subprocess.run(command, stdout=f, stderr=f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load YCSB tests with specified parameters.")

    parser.add_argument("--config", type=str, default=os.environ.get("OZONEDB_HOME") + "/bench/scripts/config/ycsb.yaml")
    args = parser.parse_args()
    json_path = args.config
    
    with open(json_path, 'r') as f:
        config = yaml.safe_load(f)
        
        load_config = config["local"]["load"]
        record_cnts = load_config["record_cnt"]
        key_sizes = load_config["key_size"]
        db_names = load_config["db_name"]
        ycsb_data_path = load_config["ycsb_data_path"]
        threads = load_config["threads"]
        repeated = load_config["repeated"]
        os.makedirs(ycsb_data_path, exist_ok=True)
        
        load_ycsb(record_cnts, key_sizes, db_names, ycsb_data_path, threads, repeated)
