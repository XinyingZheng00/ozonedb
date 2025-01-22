import matplotlib.pyplot as plt
import numpy as np

# Data
threads = [4, 8, 16, 32, 64]  # Thread numbers (x-axis)
batch_sizes = [50, 100, 500]  # Batch sizes (y-axis)

throughput = [
    [28792,37341,	34520	,25371	,15023],
    [33898	,48586	,46339	,32110,	17075,],
    [56265,	80269	,78566	,49394,27638 ]
]

for i in range(len(throughput)):
    for j in range(len(throughput[i])):
        throughput[i][j] /= 1024  # Convert to MB/s

# Plotting
plt.figure(figsize=(10, 6))

# Plot each thread count
for i, thread in enumerate(threads):
    plt.plot(batch_sizes, [row[i] for row in throughput], label=f"{thread} threads", marker='o')
# add a line for the ideal throughput
plt.plot(batch_sizes, [55 for batch_size in batch_sizes], label='Ideal', linestyle='--')

# Labels and title
plt.title('Throughput vs Batch Size for Different Thread Counts')
plt.xlabel('Batch Size')
plt.ylabel('Throughput (MB/S)')
plt.xscale('log')  # Use log scale for batch size (optional, for better visualization)
plt.legend(title='Thread Count')
plt.grid(True)

# Show plot
plt.show()

plt.savefig('network_o.png')