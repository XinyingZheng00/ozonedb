import matplotlib.pyplot as plt
import numpy as np

# Data
threads = [4, 8, 16, 32, 64]  # Thread numbers (x-axis)
batch_sizes = [100, 500, 1000, 3000, 5000]  # Batch sizes (y-axis)
throughput = [
    [13621, 14369, 17924, 22153, 23530],  # Throughput for batch size 100
    [13996, 16163, 23274, 20678, 26012],  # Throughput for batch size 500
    [15253, 16796, 23213, 24356, 21934],  # Throughput for batch size 1000
    [14845, 17241, 22797, 23492, 23694],  # Throughput for batch size 3000
    [15953, 15743, 19674, 23486, 19766],  # Throughput for batch size 5000
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
plt.plot(batch_sizes, [27.63 for batch_size in batch_sizes], label='Ideal', linestyle='--')

# Labels and title
plt.title('Throughput vs Batch Size for Different Thread Counts')
plt.xlabel('Batch Size')
plt.ylabel('Throughput (MB/S)')
plt.xscale('log')  # Use log scale for batch size (optional, for better visualization)
plt.legend(title='Thread Count')
plt.grid(True)

# Show plot
plt.show()

plt.savefig('network.png')