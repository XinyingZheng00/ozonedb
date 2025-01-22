import re
import matplotlib.pyplot as plt


# Function to parse log lines and extract elapsed time and throughput
def parse_log(file_path):
    elapsed_time = []
    throughput = []
#2024-12-12 16:19:25:813 10 sec: 19999 operations; 1999.7 current ops/sec; est completion in 8 minutes [INSERT: Count=20002, Max=298495, Min=3404, Avg=15098.67, 90=27711, 99=49407, 99.9=78975, 99.99=124543] 
    # Regular expression to match the desired log lines
    log_pattern = re.compile(r".* (\d+) sec: .* current ops/sec;.*")

    with open(file_path, 'r') as file:
        for line in file:
            match = log_pattern.match(line)
            if match:
                # Extract elapsed time and throughput
                time_sec = int(match.group(1))
                ops_per_sec = float(re.search(r"(\d+\.?\d*) current ops/sec;", line).group(1))
                elapsed_time.append(time_sec)
                throughput.append(ops_per_sec / 1024)

    return elapsed_time, throughput

# Function to plot throughput over elapsed time
def plot_throughput(elapsed_time, throughput, lable):

    plt.plot(elapsed_time, throughput, marker='o', label=lable)
    plt.title('Throughput Over Time')
    plt.xlabel('Elapsed Time (seconds)')
    plt.ylabel('Throughput (MB/sec)')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()
    

# Main script
if __name__ == "__main__":
    # Path to the log file
    gap = [0,500]
    plt.figure(figsize=(10, 6))
    for i in gap: 
        log_file_path = f"/home/xinying/Desktop/ozonedb/bench/results/azure/ozonedb-1KB-workloada-1000000-insert_{i}.result"  # Replace with your log file path
        elapsed_time, throughput = parse_log(log_file_path)
        # Plot the throughput over elapsed time
        if elapsed_time and throughput:
            plot_throughput(elapsed_time, throughput, "ozonedb-"+str(i))
        else:
            print("No valid data found in the log file.")
    
    log_file_path = f"/home/xinying/Desktop/ozonedb/bench/results/azure/azuresql-1KB-workloada-1000000-insert.result"  # Replace with your log file path
    elapsed_time, throughput = parse_log(log_file_path)
    # Plot the throughput over elapsed time
    if elapsed_time and throughput:
        plot_throughput(elapsed_time, throughput, "azuresql")
    else:
        print("No valid data found in the log file.")
    
    log_file_path = f"/home/xinying/Desktop/ozonedb/bench/results/azure/postgresql-1KB-workloada-1000000-insert.result"  # Replace with your log file path
    elapsed_time, throughput = parse_log(log_file_path)
    # Plot the throughput over elapsed time
    if elapsed_time and throughput:
        plot_throughput(elapsed_time, throughput, "postgresql")
    else:
        print("No valid data found in the log file.")
        
    # rate = ["10gb", "1gb", "500mb", "100mb", "10mb"]
    # for i in rate: 
    #     log_file_path = f"/home/xinying/Desktop/ozonedb/bench/results/azure/local_{i}.result" 
    #     elapsed_time, throughput = parse_log(log_file_path)
    #     # Plot the throughput over elapsed time
    #     if elapsed_time and throughput:
    #         plot_throughput(elapsed_time, throughput, "ozonedb-"+str(i))
    #     else:
    #         print("No valid data found in the log file.")
    
    
    # plt.axhline(y=45, color='r', linestyle='--', label='Target Throughput (ops/sec)')
    plt.savefig('plot/throughput.png')
    
# todo:
# cassendra, soft/hard acknowledge. early ack, final ack.
# nfs offer atomic append? => search, worst case
# log-structured file system