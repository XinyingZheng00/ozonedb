import re
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--path', type=str, default='', help='Path to the result files')
args = parser.parse_args()
result_file = args.path
graph_name = result_file.split(".")[0] + "-task_distribution.pdf"
started = False

compaction_dict = defaultdict(int)
total_compaction = 0
"""
140506407355968:Compacting datalog/17 datalog/18 into:sstable1/73b0ade8-639a-46fa-95dc-d36373663dbf202504021229491743618679582400763.sst
140506407355968:Compacting sstable1/73b0ade8-639a-46fa-95dc-d36373663dbf202504021229491743618659640887233.sst sstable1/73b0ade8-639a-46fa-95dc-d36373663dbf202504021229491743618669473471570.sst into:sstable2/73b0ade8-639a-46fa-95dc-d36373663dbf202504021229491743618692111516068.sst
"""
process_compaction_regex = re.compile(r"^(\d+):Compacting (.*) into:(.*)")

with open(result_file, 'r') as file:
    for line in file:
        if "Starting test." in line and started:
                if started:
                    break
                started = True
        match = process_compaction_regex.match(line)
        if match:
            thread_id = match.group(1)
            compaction_dict[thread_id] += 1
            total_compaction += 1

sorted_thread_id = dict(sorted(compaction_dict.items(), key=lambda item: item[1]))
threads = list(sorted_thread_id.keys())
percentages = [(count / total_compaction) * 100 for count in sorted_thread_id.values()]

fig, ax = plt.subplots(figsize=(15, 6))
ind = np.arange(len(threads))
ax.bar(ind, percentages, color='skyblue', alpha=0.7)
ax.set_xlabel('Client Threads', fontsize=14)
ax.set_ylabel('Percentage of Total Compactions (%)', fontsize=14)
ax.set_title('Percentage Distribution of Compactions by Client Threads', fontsize=16, fontweight='bold')
ax.set_xticks(ind)
ax.set_xticklabels(threads, ha='center')
ax.grid(axis='y', linestyle='--', alpha=0.7)

plt.tight_layout()
plt.savefig(graph_name, dpi=300, bbox_inches='tight')
plt.show()