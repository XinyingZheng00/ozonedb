import glob
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
import re
import os
from datetime import datetime
from itertools import cycle

started = False
start_time = {}

elapsed_time = defaultdict(list)
throughput = defaultdict(list)
ozonedb_home = os.environ.get("OZONEDB_HOME")

# db: event: []
event_metrics = defaultdict(lambda: defaultdict(list))

# Define reusable style cycles
colors = cycle([ 'green', 'red', 'orange', 'purple', 'black', 'brown', 'pink', 'gray'])

colors = cycle([
    'green',  # light green
    '#ff9999',  # light red (pinkish)
    'orange',  # orange
    '#dab6ff',  # light purple
    '#a9a9a9',  # dark gray (lighter than black)
    '#deb887',  # burlywood (lighter brown)
    '#d3d3d3'   # light gray
])
linestyles = cycle(['--', '-.', ':'])

# Cache styles for each event type
event_styles = defaultdict(lambda: (next(colors), next(linestyles)))

def extract_numeric_suffix(db_name):
    """Extracts the numeric part from database names like 'ozonedb_t1' -> 1, 'ozonedb_t16' -> 16"""
    match = re.search(r'(\d+)$', db_name)  # Find the numeric suffix at the end
    return int(match.group(1)) if match else float('inf')  # Return as int for correct sorting


def extract_data(file_path, db_name):
    global started, start_time
#2025-04-07 19:33:41:013 0 sec: 0 operations; est completion in 0 second 
#2024-12-12 16:19:25:813 10 sec: 19999 operations; 1999.7 current ops/sec; est completion in 8 minutes [INSERT: Count=20002, Max=298495, Min=3404, Avg=15098.67, 90=27711, 99=49407, 99.9=78975, 99.99=124543] 
#2025-04-07 19:34:22:829 - Flush started: FlushJobInfo.....
    start_time_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3}) 0 sec: .*")
    log_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3}) (\d+) sec: .*; (\d+\.?\d*) current ops/sec;.*")
    event_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3}) - (.*): .*') 

    with open(file_path, 'r') as file:
        for line in file:
            if "Starting test." in line:
                if started:
                    break
                started = True
            match = start_time_pattern.match(line)
            if match:
                start_time[db_name] = match.group(1)
            
            match = log_pattern.match(line)
            if match:
                # Extract elapsed time and throughput            
                timestamp = match.group(1)
                ops_per_sec = float(match.group(3))
                elapsed_time[db_name].append(timestamp)
                throughput[db_name].append(ops_per_sec / 1024)

            match = event_pattern.search(line)
            if match:
                timestamp = match.group(1)
                event = match.group(2)
                # if "Compaction" in event:
                #     continue
                event_metrics[db_name][event].append(timestamp)

fmt = '%Y-%m-%d %H:%M:%S:%f'
def plot_throughput(result_graph_name, add_event):
    global elapsed_time, throughput, start_time
    plt.figure(figsize=(30, 10))
    elapsed_time = sorted(
        elapsed_time.items(),
        key=lambda x: extract_numeric_suffix(x[0])
    )  # Sort by numeric suffix in db_name
    for db_name, times in elapsed_time:
        times = list(map(lambda x: (datetime.strptime(x, fmt) - datetime.strptime(start_time[db_name], fmt)).total_seconds(), times))
        plt.plot(times, throughput[db_name], marker='o', markersize=2,  linewidth=2, label=db_name,linestyle='-')
    
    
    if add_event == 1:
        for db_name, events in event_metrics.items():
            # Merge events list with the prefix name (e.g., "compaction started" and "compaction completed")
            merged_events = defaultdict(list)
            for event, timestamps in events.items():
                prefix = event.rsplit(' ', 1)[0]  # Extract the prefix (e.g., "compaction")
                if prefix not in merged_events:
                    merged_events[prefix] = []
                if "started" in event or "completed" in event:
                    merged_events[prefix].append(timestamps)

            for prefix, event_timestamps in merged_events.items():
                if len(event_timestamps) == 2:  # Ensure both "started" and "completed" exist
                    merged_timestamps = [
                        (datetime.strptime(start, fmt) + (datetime.strptime(end, fmt) - datetime.strptime(start, fmt)) / 2).strftime(fmt)
                        for start, end in zip(event_timestamps[0], event_timestamps[1])
                    ]
                    merged_events[prefix] = merged_timestamps
                else:
                    merged_events[prefix] = event_timestamps[0]  # Use the existing timestamps if only one exists

            for event, timestamps in merged_events.items():
                label = f"{db_name} - {event}"
                color, linestyle = event_styles[label]
                timestamps = list(map(lambda x: (datetime.strptime(x, fmt) - datetime.strptime(start_time[db_name], fmt)).total_seconds(), timestamps))
                plt.axvline(x=timestamps[0], color=color, linestyle=linestyle, label=f"{db_name} - {event}")
                for timestamp in timestamps[1:]:
                    plt.axvline(x=timestamp, color=color, linestyle=linestyle)
            
            # for event, timestamps in events.items():
            #     lable = f"{db_name} - {event}"
            #     color, linestyle = event_styles[lable]
            #     timestamps = list(map(lambda x: (datetime.strptime(x, fmt) - datetime.strptime(start_time[db_name], fmt)).total_seconds(), timestamps))
            #     plt.axvline(x=timestamps[0], color=color, linestyle=linestyle, label=f"{db_name} - {event}")
            #     for timestamp in timestamps[1:]:
            #         plt.axvline(x=timestamp, color=color, linestyle=linestyle)
        
        
    plt.title('Throughput Over Time', fontsize=16, fontweight='bold')
    plt.xlabel('Elapsed Time (seconds)', fontsize=14)
    plt.ylabel('Throughput (MB/sec)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(title="Database", fontsize=12)
    plt.tight_layout()
    plt.savefig(result_graph_name, dpi=300, bbox_inches='tight')
    plt.show()
            


def main():
    global started
    if len(sys.argv) < 3:
        print("Usage: python3 latency.py local/cloud result-path regex 1/0 to indicate add event or not")
        sys.exit(1)

    result_files = []
    result_path = os.path.join(ozonedb_home, "bench", "results/" + sys.argv[1] + "/")
    for pattern in sys.argv[3:]:
        result_files.extend(glob.glob(os.path.join(result_path, pattern)))
    add_event = int(sys.argv[2])
    if not result_files:
        print("No result files found.")
        sys.exit(1)
    
    key_size = os.path.basename(result_files[0]).split('-')[0]
    op_cnt = os.path.basename(result_files[0]).split('-')[1]
    workload = os.path.basename(result_files[0]).split('-')[-2]
    for file in result_files:
        if file.endswith(".result"):
            print(file)
            started = False
            db_name = os.path.basename(file).split('-')[-1].split('.')[0]
            extract_data(file, db_name)
    result_graph_name = os.path.join(result_path, f"{key_size}-{op_cnt}-{workload}-throughput_over_time.pdf")
    plot_throughput(result_graph_name, add_event)


if __name__ == "__main__":
    main()
