import glob
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
import re
import os

started = False
elapsed_time = defaultdict(list)
throughput = defaultdict(list)
ozonedb_home = os.environ.get("OZONEDB_HOME")


def extract_numeric_suffix(db_name):
    """Extracts the numeric part from database names like 'ozonedb_t1' -> 1, 'ozonedb_t16' -> 16"""
    match = re.search(r'(\d+)$', db_name)  # Find the numeric suffix at the end
    return int(match.group(1)) if match else float('inf')  # Return as int for correct sorting


def extract_throughput(file_path, db_name):
    global started  
#2024-12-12 16:19:25:813 10 sec: 19999 operations; 1999.7 current ops/sec; est completion in 8 minutes [INSERT: Count=20002, Max=298495, Min=3404, Avg=15098.67, 90=27711, 99=49407, 99.9=78975, 99.99=124543] 
    # Regular expression to match the desired log lines
    log_pattern = re.compile(r".* (\d+) sec: .* current ops/sec;.*")

    with open(file_path, 'r') as file:
        for line in file:
            if "Starting test." in line:
                if started:
                    break
                started = True
            match = log_pattern.match(line)
            if match:
                # Extract elapsed time and throughput
                time_sec = int(match.group(1))
                ops_per_sec = float(re.search(r"(\d+\.?\d*) current ops/sec;", line).group(1))
                elapsed_time[db_name].append(time_sec)
                throughput[db_name].append(ops_per_sec / 1024)


def plot_throughput(result_graph_name):
    global elapsed_time, throughput
    plt.figure(figsize=(12, 6))
    elapsed_time = sorted(
        elapsed_time.items(),
        key=lambda x: extract_numeric_suffix(x[0])
    )  # Sort by numeric suffix in db_name
    print(elapsed_time)
    for db_name, times in elapsed_time:
        label = db_name.split("-")[-1]
        plt.plot(times, throughput[db_name], marker='o', label=label, linestyle='-')
    
    plt.title('Throughput Over Time', fontsize=16, fontweight='bold')
    plt.xlabel('Elapsed Time (seconds)', fontsize=14)
    plt.ylabel('Throughput (MB/sec)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(title="Database", fontsize=12)
    plt.tight_layout()
    plt.savefig(result_graph_name, dpi=300, bbox_inches='tight')
    plt.show()

# data_metrics = {
#     "times": [],
#     "throughputs": [],
#     "log_compaction_times": [],
#     "sst_compaction_times": [],
#     "log_compaction_start_times": [],
#     "sst_compaction_start_times": [],
#     "rocksdb_flush_start_times": [],
#     "rocksdb_flush_end_times": [],
#     "rocksdb_compaction_start_times": [],
#     "rocksdb_compaction_end_times": []
# }

# def parse_line(label, line, data_list, time_split=" at "):
#     if label in line:
#         try:
#             parts = line.split(time_split)
#             data_list.append(int(parts[1].strip()))  # Takes only the time part
#         except ValueError:
#             print(f"Error parsing line: {line}")
#             exit()

# def extract_event(file_path):
#     with open(file_path, 'r') as file:
#         for line in file:        
#             parse_line("Rocksdb Flush Started", line, data_metrics["rocksdb_flush_start_times"])
#             parse_line("Rocksdb Flush Completed", line, data_metrics["rocksdb_flush_end_times"])
#             parse_line("Rocksdb Compaction Started", line, data_metrics["rocksdb_compaction_start_times"])
#             parse_line("Rocksdb Compaction Completed", line, data_metrics["rocksdb_compaction_end_times"])
#             parse_line("Log Compaction Started", line, data_metrics["log_compaction_start_times"])
#             parse_line("SST Compaction Started", line, data_metrics["sst_compaction_start_times"])
#             parse_line("Log Compaction Completed", line, data_metrics["log_compaction_times"])
#             parse_line("SST Compaction Completed", line, data_metrics["sst_compaction_times"])

# def plot_throughput(graph_name):
#     plt.figure(figsize=(40, 6))
#     plt.plot(data_metrics["times"], data_metrics["throughputs"], marker='o', linestyle='-', color='b', markersize=1)
#     plt.title('Throughput Over Time')

#     # Map event types to plot settings
#     event_styles = {
#         "rocksdb_flush_start_times": ("orange", "--", "Rocksdb Flush Started"),
#         "rocksdb_flush_end_times": ("green", "--", "Rocksdb Flush Completed"),
#         "rocksdb_compaction_start_times": ("black", "-", "Rocksdb Compaction Started"),
#         "rocksdb_compaction_end_times": ("red", "-", "Rocksdb Compaction Completed"),
#         "log_compaction_start_times": ("red", "--", "Log Compaction Started"),
#         "log_compaction_times": ("black", "--", "Log Compaction Completed"),
#         "sst_compaction_start_times": ("orange", "--", "SST Compaction Started"),
#         "sst_compaction_times": ("green", "--", "SST Compaction Completed")
#     }

#     # Plot event lines
#     for event, (color, style, label) in event_styles.items():
#         if data_metrics[event]:
#             plt.axvline(x=data_metrics[event][0], color=color, linestyle=style, label=label)
#             for event_time in data_metrics[event][1:]:
#                 plt.axvline(x=event_time, color=color, linestyle=style)

#     plt.ylim(0, 1500)
#     plt.xlabel('Time (ms)')
#     plt.ylabel('Throughput (MB/s)')
#     plt.legend(loc='upper right')
#     plt.grid(True)

#     # Save the plot as a PNG file
#     plt.savefig(graph_name)
#     plt.show()


def main():
    global started
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
    workload = os.path.basename(result_files[0]).split('-')[-2]
    for file in result_files:
        if file.endswith(".result"):
            started = False
            db_name = os.path.basename(file).split('-')[-1].split('.')[0]
            print(db_name)  
            extract_throughput(file, db_name)
    result_graph_name = os.path.join(result_path, f"{key_size}-{op_cnt}-{workload}-throughput_over_time.pdf")
    plot_throughput(result_graph_name)
        
    # extract_event(insert_event_path)
    # extract_event(readwrite_event_path)

if __name__ == "__main__":
    main()
