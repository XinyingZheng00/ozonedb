import matplotlib.pyplot as plt
import argparse
import os

# Data storage for various metrics
data_metrics = {
    "times": [],
    "throughputs": [],
    "log_compaction_times": [],
    "sst_compaction_times": [],
    "log_compaction_start_times": [],
    "sst_compaction_start_times": [],
    "rocksdb_flush_start_times": [],
    "rocksdb_flush_end_times": [],
    "rocksdb_compaction_start_times": [],
    "rocksdb_compaction_end_times": []
}

# Helper to parse a line with a specific label and append to a list
def parse_line(label, line, data_list, time_split=" at "):
    if label in line:
        try:
            parts = line.split(time_split)
            data_list.append(int(parts[1].strip()))  # Takes only the time part
        except ValueError:
            print(f"Error parsing line: {line}")
            exit()

# Function to extract throughput values from the text
def extract_throughput(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if "Throughput" in line:
                try:
                    throughput = float(line.split(": ")[1].split("at")[0].replace(" MB/sec", "").strip())
                    time = int(line.split(" at ")[1])
                    data_metrics["times"].append(time)
                    data_metrics["throughputs"].append(throughput)
                except ValueError:
                    print(f"Error parsing throughput line: {line}")
                    exit()

def extract_event(file_path):
    with open(file_path, 'r') as file:
        for line in file:        
            parse_line("Rocksdb Flush Started", line, data_metrics["rocksdb_flush_start_times"])
            parse_line("Rocksdb Flush Completed", line, data_metrics["rocksdb_flush_end_times"])
            parse_line("Rocksdb Compaction Started", line, data_metrics["rocksdb_compaction_start_times"])
            parse_line("Rocksdb Compaction Completed", line, data_metrics["rocksdb_compaction_end_times"])
            parse_line("Log Compaction Started", line, data_metrics["log_compaction_start_times"])
            parse_line("SST Compaction Started", line, data_metrics["sst_compaction_start_times"])
            parse_line("Log Compaction Completed", line, data_metrics["log_compaction_times"])
            parse_line("SST Compaction Completed", line, data_metrics["sst_compaction_times"])


# Function to plot the throughput graph with events as vertical lines
def plot_throughput(graph_name):
    plt.figure(figsize=(40, 6))
    plt.plot(data_metrics["times"], data_metrics["throughputs"], marker='o', linestyle='-', color='b', markersize=1)
    plt.title('Throughput Over Time')

    # Map event types to plot settings
    event_styles = {
        "rocksdb_flush_start_times": ("orange", "--", "Rocksdb Flush Started"),
        "rocksdb_flush_end_times": ("green", "--", "Rocksdb Flush Completed"),
        "rocksdb_compaction_start_times": ("black", "-", "Rocksdb Compaction Started"),
        "rocksdb_compaction_end_times": ("red", "-", "Rocksdb Compaction Completed"),
        "log_compaction_start_times": ("red", "--", "Log Compaction Started"),
        "log_compaction_times": ("black", "--", "Log Compaction Completed"),
        "sst_compaction_start_times": ("orange", "--", "SST Compaction Started"),
        "sst_compaction_times": ("green", "--", "SST Compaction Completed")
    }

    # Plot event lines
    for event, (color, style, label) in event_styles.items():
        if data_metrics[event]:
            plt.axvline(x=data_metrics[event][0], color=color, linestyle=style, label=label)
            for event_time in data_metrics[event][1:]:
                plt.axvline(x=event_time, color=color, linestyle=style)

    plt.ylim(0, 1500)
    plt.xlabel('Time (ms)')
    plt.ylabel('Throughput (MB/s)')
    plt.legend(loc='upper right')
    plt.grid(True)

    # Save the plot as a PNG file
    plt.savefig(graph_name)
    plt.show()

# Main function to extract and plot throughput
def main():
    parser = argparse.ArgumentParser(description='Process throughput logs and plot results.')
    parser.add_argument('--operation_count', type=int, default=500, help='Number of operations to perform')
    parser.add_argument('--db', type=str, default='ozonedb', help='Database name')
    parser.add_argument('--key_size', type=str, default='1KB', help='record size')
    parser.add_argument('--workload_name', type=str, default='a', help='ycsb workload a-f')
    args = parser.parse_args()

    db = args.db
    operation_count = args.operation_count
    key_size = args.key_size
    workload_name = args.workload_name

    insert_event_path = os.environ.get("OZONEDB_HOME") + f"/bench/results/ycsb/{db}-{key_size}-workload{workload_name}-{operation_count}-insert.result"
    readwrite_event_path = os.environ.get("OZONEDB_HOME")+ f"/bench/results/ycsb/{db}-{key_size}-workload{workload_name}-{operation_count}-readwrite.result"
    throughput_path = os.environ.get("OZONEDB_HOME")+ f"/bench/results/ycsb/{db}-{key_size}-workload{workload_name}-{operation_count}-client.log"
    result_graph_name = os.environ.get("OZONEDB_HOME") + f"/bench/graphs/ycsb/{db}-{key_size}-workload{workload_name}-{operation_count}.pdf"
    
    extract_event(insert_event_path)
    extract_event(readwrite_event_path)
    extract_throughput(throughput_path)
    plot_throughput(result_graph_name)

if __name__ == "__main__":
    main()
