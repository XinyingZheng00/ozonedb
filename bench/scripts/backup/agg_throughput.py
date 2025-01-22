import re
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates
from collections import defaultdict

# Dictionaries to store throughput data per process
process_throughputs = defaultdict(list)  # {process_id: [(timestamp, throughput)]}
aggregated_throughput = defaultdict(float)
rocksdb_times = []
rocksdb_throughputs = []

def extract_rockdb_throughput(file_path):
    with open(file_path, 'r') as file:
        data = file.read()

    for line in data.strip().split("\n"):
        if "throughput" in line:
            parts = line.split(" at ")
            time_str = int(parts[1])
            try:
                throughput = float(parts[0].split(": ")[1].replace(" MB/s", ""))
                rocksdb_times.append(time_str)
                rocksdb_throughputs.append(throughput)
            except:
                print(line)
                exit()
        

# Function to extract throughput values and events from the text
def extract_throughput(file_path):
    with open(file_path, 'r') as file:
        data = file.read()

    for line in data.strip().split("\n"):
        if "throughput" in line:
            parts = line.split(" at ")
            time_str = int(parts[1])
            try:
                process_id, throughput = parts[0].split(": ")[0], float(parts[0].split(": ")[1].replace(" MB/s", ""))
                timestamp = time_str
                process_throughputs[process_id].append((timestamp, throughput))
                aggregated_throughput[timestamp] += throughput
            except:
                print(line)
                exit()



# Function to plot throughput data for each process and aggregated throughput
def plot_throughput(graph_name):
    plt.figure(figsize=(100, 10))

    # Plotting each process's throughput
    for process_id, values in process_throughputs.items():
        times, throughputs = zip(*sorted(values, key=lambda x: x[0]))  # Sort by timestamp
        plt.plot(times, throughputs, marker='o', linestyle='-', label=f'Process {process_id}', markersize=2)

    # Plot aggregated throughput
    throughputs_sum=[]
    
    for process_id, values in process_throughputs.items():
        times, throughputs = zip(*sorted(values, key=lambda x: x[0]))  # Sort by timestamp
        if len(throughputs_sum) == 0:
            throughputs_sum = throughputs
        else:
            throughputs_sum = [sum(x) for x in zip(throughputs_sum, throughputs)]
        

    plt.plot(times, throughputs_sum, marker='o', linestyle='-', color='green', markersize=1, label='Sum of Processes')
    
    plt.plot(rocksdb_times, rocksdb_throughputs, marker='o', linestyle='-', color='b', markersize=1)

    plt.title('Throughput Per Process and Aggregated Throughput', fontdict={'fontsize': 30})

    start_time = 0
    end_time = 2e9
    plt.xlim([start_time, end_time])  # Zoom into the specified time range
    plt.xlabel('Time', fontdict={'fontsize': 30})
    plt.ylabel('Throughput (MB/s)', fontdict={'fontsize': 30})
    plt.legend(loc='upper right', fontsize=30)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)

    plt.grid(True)

    # Save the plot as a PNG file
    plt.savefig(graph_name)
    plt.show()


# Main function to extract and plot throughput
def main():
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--operation_count', type=int, default=10000, help='Number of operations')
    parser.add_argument('--num_clients', type=str, default='2', help='specify how many clients')
    args = parser.parse_args()
    operation_count = args.operation_count
    num_clients = args.num_clients

    log_file = f"../results/write/multiple_client/{num_clients}_{operation_count}_ozonedb_throughput.txt"
    graph_name = f"../graphs/write/multiple_client/{num_clients}_{operation_count}_ozonedb_throughput_agg.pdf"
    extract_throughput(log_file)
    rocksdb_file_path = f"../results/write/{operation_count}_rocksdb_throughput.txt"
    extract_rockdb_throughput(rocksdb_file_path)
    plot_throughput(graph_name)



if __name__ == "__main__":
    main()
