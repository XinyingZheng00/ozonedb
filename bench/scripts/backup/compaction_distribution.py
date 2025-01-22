import re
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np

# Step 1: Load the log data
import argparse
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--operation_count', type=int, default=10000, help='Number of operations')
parser.add_argument('--num_clients', type=str, default='2', help='specify how many clients')
args = parser.parse_args()
operation_count = args.operation_count
num_clients = args.num_clients
# log_file = f"../results/write/multiple_client/{num_clients}_{operation_count}_ozonedb_throughput.txt"
# graph_name = f"../graphs/write/multiple_client/{num_clients}_{operation_count}_ozonedb_throughput_distribution.pdf"
log_file = f"/users/Xinying/ozonedb/build/result"
graph_name = f"/users/Xinying/ozonedb/build/result.pdf"


# Initialize a dictionary to store process ID and its compaction counts
log_compaction_dict = defaultdict(int)
sst_compaction_dict = defaultdict(dict)
op_each_process = defaultdict(int)
total_compaction_dict = defaultdict(int)

# Regex to capture the process ID at the start of a line and the word "Compaction"
#5733 [0x7f775f562640] INFO bench null - 1450135:Log Compaction Completed at 3925395794 time: 150
#6047 [0x7f775f562640] INFO bench null - 1450137:Level 1 SST Compaction Completed at 4241483760 time: 170
process_log_compaction_regex = re.compile(r"^(\d+) \[.*\] INFO .* - (\d+):Log Compaction Completed at (\d+) time: (\d+)")
process_sst_compaction_regex = re.compile(r"^(\d+) \[.*\] INFO .* - (\d+):Level (\d+) SST Compaction Completed at (\d+) time: (\d+)")

process_num_op_regex = re.compile(r"^(\d+) \[.*\] INFO .* - (\d+): Op Count: (\d+)")

total_compaction = 0

# Step 2: Process the log file
with open(log_file, 'r') as file:
    for line in file:
        match = process_num_op_regex.match(line)
        if match:
            process_id = match.group(2)
            num_op = int(match.group(3))
            op_each_process[process_id] = num_op
        match = process_log_compaction_regex.match(line)
        if match:
            process_id = match.group(2)  # Extract the process ID
            log_compaction_dict[process_id] += 1 # Extract the compaction time
            total_compaction_dict[process_id] += 1
            total_compaction += 1
        match = process_sst_compaction_regex.match(line)
        if match:
            process_id = match.group(2)
            level = match.group(3)
            if process_id not in sst_compaction_dict:
                sst_compaction_dict[process_id] = defaultdict(int)
            sst_compaction_dict[process_id][level] += 1
            total_compaction_dict[process_id] += 1
            total_compaction += 1

sorted_dict = dict(sorted(total_compaction_dict.items(), key=lambda item: item[1]))
print(sorted_dict.values())
print(total_compaction)
processes = list(sorted_dict.keys())
log_compactions = []
sst_levels = defaultdict(list)

# Find all SST levels in the data
all_sst_levels = set()
for process_data in sst_compaction_dict.values():
    all_sst_levels.update(process_data.keys())
all_sst_levels = sorted(all_sst_levels, key=int)  # Sort by level, e.g., 1, 2, 3...

print(all_sst_levels)
# Initialize lists for all levels
for level in all_sst_levels:
    sst_levels[level] = []

# Aggregate data
for process in processes:
    # Log compactions
    log = log_compaction_dict[process]
    log_percentage = log / total_compaction * 100
    log_compactions.append(log_percentage)
    
    # SST compactions for each level
    for level in all_sst_levels:
        sst_compaction = sst_compaction_dict[process].get(level, 0)
        sst_percentage = sst_compaction / total_compaction * 100
        sst_levels[level].append(sst_percentage)

# Plotting the data
fig, ax = plt.subplots(figsize=(15, 6))

# Bar positions
ind = np.arange(len(processes))

# Plot stacked bars
bottoms = np.zeros(len(processes))  # To track where the next bar stack should start

# Plot log compactions
ax.bar(ind, log_compactions, label='Log Compactions', color='blue')
bottoms += log_compactions

# Plot SST level compactions
colors = ['orange', 'green', 'red', 'purple']  # Define colors for different SST levels
for i, level in enumerate(all_sst_levels):
    ax.bar(ind, sst_levels[level], bottom=bottoms, label=f'SST Level {level} Compactions', color=colors[i % len(colors)])
    bottoms += sst_levels[level]


# Labels and title
ax.set_xlabel('Processes')
ax.set_ylabel('Percentage of Total Compactions')
ax.set_title('Percentage Distribution of Compactions by Process (Log and SST Levels)')
ax.set_xticks(ind)
ax.set_xticklabels(processes)
ax.legend()

# Show the plot
plt.show()
plt.savefig(graph_name)















# Step 3: Calculate the percentage for each process
# processes = list(compaction_time.keys())
# percentages = [(count / total_compaction) * 100 for count in compaction_time.values()]
# fair_attr = [(y/x) for (x,y) in compaction_time.items()]
# Step 4: Calculate average and standard deviation
# print(fair_attr)
# averages = np.mean(fair_attr)
# std_devs = np.std(fair_attr)
# cv = std_devs/averages
# averages = np.mean(percentages)
# std_devs = np.std(percentages)

# print (f"Average: {averages}")
# print (f"Standard Deviation: {std_devs}")
# print (f"CV: {cv}")

# Step 5: Plot the results
# fig, ax1 = plt.subplots(figsize=(10, 6))

# Bar plot for percentages on the left y-axis
# bars1 = ax1.bar(processes, percentages, color='skyblue', label='Percentage of Compactions', align='center')

# Labels for left y-axis
# ax1.set_xlabel('Number of Operations for each Process')
# ax1.set_ylabel('Percentage of Compactions (%)')
# ax1.tick_params(axis='y')

# Create a second y-axis for fairness attribute
# ax2 = ax1.twinx()
# bars2 = ax2.bar(processes, fair_attr, color='red', label='Fairness Attributed', align='edge')

# Labels for right y-axis
# ax2.set_ylabel('Fairness Attribute')
# ax2.tick_params(axis='y')

#ax.errorbar(processes, percentages, yerr=std_devs, fmt='o', color='red', label='Standard Deviation', capsize=5)
# Add mean lines for each process
# ax.axhline(averages, color='green', linestyle='--', label='Average')

# Combine legends
# lines1, labels1 = ax1.get_legend_handles_labels()
# lines2, labels2 = ax2.get_legend_handles_labels()
# ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
# ax1.legend(lines1, labels1, loc='upper left')

# Title
# ax1.set_title('Percentage of Compactions and Fairness Attribute by Process ID')

# Step 6: Save and show the plot
# plt.savefig(graph_name)
# plt.show()