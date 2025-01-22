import matplotlib.pyplot as plt
import numpy as np

# Data
systems = ['Cosmos', 'JDBC', 'Ozonedb']
runtime = [322974, 134941, 218350]  # RunTime in ms
throughput = [309.6224463888734, 741.0646134236445, 457.9803068468056]  # Throughput in ops/sec
avg_latency = [12850.23513, 5353.22969, 8719.45981]  # Latency in us

# Create figure and axis
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot RunTime
ax1.bar(systems, runtime, color='b', alpha=0.6, label='RunTime (ms)', width=0.4, align='center')

# Create a second y-axis for Throughput and Latency
ax2 = ax1.twinx()

# Plot Throughput
ax2.plot(systems, throughput, color='g', marker='o', label='Throughput (ops/sec)', linestyle='-', linewidth=2)
# Plot Average Latency
ax2.plot(systems, avg_latency, color='r', marker='s', label='Average Latency (us)', linestyle='--', linewidth=2)

# Set labels and title
ax1.set_xlabel('System')
ax1.set_ylabel('RunTime (ms)', color='b')
ax2.set_ylabel('Throughput (ops/sec) / Latency (us)', color='g')
ax1.set_title('System Comparison: RunTime, Throughput, and Latency')

# Legends
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

# Show plot
plt.tight_layout()
plt.show()
plt.savefig('system_comparison.png')
