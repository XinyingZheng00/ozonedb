import os
import re
import sys
import glob
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np

data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
ozonedb_home = os.environ.get("OZONEDB_HOME")
workload_description = {
    "insert": "100% insert",
    "workloada": "50% read, 50% write",
    "workloadb": "95% read, 5% write",
    "workloadc": "100% read",
    # "workloadd": "95% read, 5% insert",
    "workloadf": "50% read, 50% read-modify-write"
}
if not ozonedb_home:
    raise EnvironmentError("OZONEDB_HOME environment variable is not set.")
op_cnt = 0
key_size = ''

def get_data_from_file(file_path):
    db_name = os.path.basename(file_path).split('-')[-1].split('.')[0]
    workload = os.path.basename(file_path).split('-')[-2] #workload can be insert or workload abcde

    with open(file_path, 'r') as file:
        for line in file:
            # Match lines for operations and latencies
            match = re.match(r'\[(.*?)\], (.*?), (.*)', line)
            if match:
                operation, metric, value = match.groups()
                value = float(value)
                if operation == "OVERALL":
                    data[metric][workload][db_name].append(value)

def extract_numeric_suffix(db_name):
    """Extracts the numeric part from database names like 'ozonedb_t1' -> 1, 'ozonedb_t16' -> 16"""
    match = re.search(r'(\d+)$', db_name)  # Find the numeric suffix at the end
    return int(match.group(1)) if match else float('inf')  # Return as int for correct sorting


def plot_individual_graphs():
    """
    Generates bar plots for each performance metric, where the x-axis represents workloads,
    and each workload is subdivided by database name. Error bars indicate standard deviation.
    """
    for metric, workloads in data.items():
        fig, ax = plt.subplots(figsize=(12, 6))
        workload_labels = sorted(workloads.keys())
        print(f"workload_labels: {workload_labels}")
        
        db_names = sorted(
            workloads[workload_labels[0]].keys(),
            key=lambda db: (db != "sqlite_t1", db != "rocksdb_t1", not db.startswith("ozonedb"), extract_numeric_suffix(db))
        )
        print(f"db_names: {db_names}")
        
        num_dbs = len(db_names)
        max_width = 0.8  # Fraction of available space for bars
        width = min(0.15, max_width / max(num_dbs, 1))  # Adjust dynamically, with a min bound

        x = np.arange(len(workloads))  # X-axis positions for workloads
        
        for i, db_name in enumerate(db_names):
            means, stds = [], []
            for workload in workload_labels:
                values = workloads[workload].get(db_name, [])
                means.append(np.mean(values) if values else 0)
                stds.append(np.std(values) if values else 0)
            
            ax.bar(x + i * width, means, width, yerr=stds, label=db_name, capsize=5)
        
        ax.set_xlabel("Workload", fontsize=14)
        ax.set_ylabel(metric, fontsize=14)
        ax.set_title(f"{metric} Comparison", fontsize=16, fontweight='bold')
        ax.set_xticks(x + (width * (len(db_names) - 1) / 2))  # Center labels
        ax.set_xticklabels([w for w in workload_labels], ha="center", fontsize=14)
        ax.yaxis.set_tick_params(labelsize=14)  
        ax.legend()
        ax.grid(axis="y", linestyle="--", alpha=0.7)
        
        plt.tight_layout()
        metric = metric.split('(')[0]
        plt.savefig(f"{ozonedb_home}/bench/results/{sys.argv[1]}/{key_size}-{op_cnt}-{metric}.pdf", dpi=300, bbox_inches='tight')
        


"""
import matplotlib.pyplot as plt
import numpy as np
labels = ['CORR', 'INDE', 'ANTI', 'Hotel4D', 'House6D', 'NBA8D']
y1=[3.27266, 5.31523, 0.117191, 0.0705803]
y2=[11.4954, 10.8171, 0.214985, 0.141177]
y3=[741.089, 1205.42, 7.68237, 6.20992]
y4=[1.23935, 1.54859, 0.128388, 0.0795759]
y5=[1636.23, 16447.3, 34.7355, 16.6843]
y6=[1e5, 1e5, 1e5, 1305.57]

y=[y1, y2, y3, y4, y5, y6]
y=np.array(y).T

x = np.arange(len(labels))  # the label locations
width = 0.2  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2-width*1, y[0], width, label='CSA', color='r')
rects2 = ax.bar(x - width/2-width*0, y[1], width, label='CSA+', color='m')
rects3 = ax.bar(x - width/2+width*1, y[2], width, label='MDA', color='g')
rects4 = ax.bar(x - width/2+width*2, y[3], width, label='MDA+', color='b')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time (sec)')
# ax.set_title('Scores by group and gender')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.set_yscale('log')

# ax.bar_label(rects1, padding=3)
# ax.bar_label(rects2, padding=3)
# ax.bar_label(rects3, padding=3)
# ax.bar_label(rects4, padding=3)
fig.tight_layout()

plt.show()


"""


def main(result_files):
    for file in result_files:
        if file.endswith(".result"): 
            print(f"Processing file: {file}") 
            get_data_from_file(file)
    plot_individual_graphs()

if __name__ == "__main__":
# latecy and throughput plot for each operation count and key size
    if len(sys.argv) < 2:
        print("Usage: python3 latency.py result-path [result-files...]")
        sys.exit(1)

    result_files = []
    result_path = os.path.join(ozonedb_home, "bench", "results/" + sys.argv[1] + "/")
    result_files.extend(glob.glob(result_path + sys.argv[2]))
    
    if not result_files:
        print("No result files found.")
        sys.exit(1)
    
    key_size = os.path.basename(result_files[0]).split('-')[0]
    op_cnt = os.path.basename(result_files[0]).split('-')[1]
    
    main(result_files)
