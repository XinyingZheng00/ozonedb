import re
from collections import Counter
import numpy as np
import matplotlib.pyplot as plt

log_file_path = "ozonedb-1KB-workloadc-30000000-insert-1.result"
with open(log_file_path, 'r') as file:
    log_lines = file.readlines()

thread_id_pattern = re.compile(r"^(\d+):Compacting .*")
thread_ids = [match.group(1) for line in log_lines if (match := thread_id_pattern.match(line))]
thread_count = Counter(thread_ids)
total_lines = len(thread_ids)
thread_percentage = {thread_id: (count / total_lines) for thread_id, count in thread_count.items()}
sorted_thread_percentage = sorted(thread_percentage.items(), key=lambda x: x[1])

percentages = [percentage for _, percentage in sorted_thread_percentage]
std_dev = np.std(percentages)
print(f"Standard Deviation of thread percentages: {std_dev}")

plt.figure(figsize=(10, 6))
plt.bar(range(64), percentages, )
plt.title("Task Distribution [SD = "+ str(std_dev) +"]")
plt.xlabel("Thread ID")
plt.ylabel("Percentage")
plt.savefig('task_distribution-1.png')
