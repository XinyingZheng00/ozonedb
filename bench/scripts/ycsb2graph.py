import os
import re
import sys
import glob
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np

data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
ozonedb_home = os.environ.get("OZONEDB_HOME")
workload_description = {
    "workloada": "50% read, 50% write",
    "workloadb": "95% read, 5% write",
    "workloadc": "100% read",
    "workloadd": "95% read, 5% insert",
    "workloadf": "50% read, 50% read-modify-write"
}
if not ozonedb_home:
    raise EnvironmentError("OZONEDB_HOME environment variable is not set.")

def get_data_from_file(file_path):
    db_name = os.path.basename(file_path).split('-')[0] + os.path.basename(file_path).split('-')[1]
    workload = os.path.basename(file_path).split('-')[2]
    op_cnt = int(os.path.basename(file_path).split('-')[3])

    with open(file_path, 'r') as file:
        for line in file:
            # Match lines for operations and latencies
            match = re.match(r'\[(.*?)\], (.*?), (.*)', line)
            if match:
                key, metric, value = match.groups()
                value = float(value)
                if key in ["OVERALL", "INSERT", "READ", "UPDATE"]:
                    if key == "OVERALL" and not ("insert" in file_path):
                        metric = metric.split('(')[0]
                        data[workload][metric][op_cnt][db_name].append(value)
                    else:
                        if metric == "AverageLatency(us)":
                            data[workload][key][op_cnt][db_name].append(value)

def plot_individual_graphs():
    # Iterate over each workload and metric, then plot each in a separate figure
    for workload, each_data in data.items():
        for key in each_data.keys():
            plt.figure(figsize=(10, 6))
            
            op_counts = sorted(each_data[key].keys())
            db_names = sorted(each_data[key][op_counts[0]].keys())
            
            for db_name in db_names:
                # Extract values for each operation count and compute statistics
                db_values = [each_data[key][op_count][db_name] for op_count in op_counts]
                # Calculate the average and standard deviation
                 # remove the max and min value
                for vals in db_values:
                    if len(vals) <= 1:
                        continue
                    vals.remove(max(vals))
                    vals.remove(min(vals))
                
                averages = [np.mean(vals) for vals in db_values]
                errors = [np.std(vals) for vals in db_values]  # standard deviation as error

                # Plot with error bars
                plt.errorbar(op_counts, averages, yerr=errors, label=db_name, capsize=5)

            # Add titles and labels
            
            plt.title(f'{key.capitalize()} Performance for {workload.capitalize()}[{workload_description[workload]}]')
            plt.xlabel('Operation Count')
            yval = ""
            if key == "Runtime":
                yval = "Time (ms)"
            elif key == "Throughput":
                yval = "Throughput (ops/sec)"
            else:
                yval = "Average Latency (us)"
            plt.ylabel(yval)
            plt.yscale('log')
            plt.legend()
            plt.grid()
            
            # Save each metric graph as a separate PDF file
            import pathlib
            pathlib.Path(os.path.join(ozonedb_home, "bench", "graphs", sys.argv[1])).mkdir(parents=True, exist_ok=True)
            output_path = os.path.join(ozonedb_home, "bench", "graphs", sys.argv[1], f"{workload}_{key}.pdf")
            print(output_path)  # Print path for debugging
            plt.savefig(output_path)
            plt.close()

def main(result_files):
    for file in result_files:
        get_data_from_file(file)
    plot_individual_graphs()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 ycsb2graph.py result-path [result-files...]")
        sys.exit(1)

    # Search file in the result dir
    result_files = []
    result_path = os.path.join(ozonedb_home, "bench", "results/" + sys.argv[1] + "/")
    
    for pattern in sys.argv[2:]:
        result_files.extend(glob.glob(result_path + pattern))
    if not result_files:
        print("No result files found.")
        sys.exit(1)
    for file in result_files:
        print(file)
    main(result_files)
