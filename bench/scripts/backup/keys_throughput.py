import matplotlib.pyplot as plt

# Data from the input
data = {
    'ozonedb_put': {
        100: 354.469,
        500: 264.556,
        2500: 227.822,
        12500: 235.118
    },
    'rocksdb_put': {
        100: 213.427,
        500: 195.467,
        2500: 189.995,
        12500: 183.787
    },
    'rockdb_put_no_sync': {
        100: 938.852,
        500: 366.495,
        2500: 323.945,
        12500: 317.294
    }
}

# Prepare data for plotting
operation_counts = []
ozonedb_throughput = []
rocksdb_throughput = []
rocksdb_no_sync_throughput = []

for count in sorted(data['ozonedb_put'].keys()):
    operation_counts.append(count)
    ozonedb_throughput.append(data['ozonedb_put'][count])

for count in sorted(data['rocksdb_put'].keys()):
    if count not in operation_counts:  # Avoid duplicate x values
        operation_counts.append(count)
    rocksdb_throughput.append(data['rocksdb_put'][count])
for count in sorted(data['rockdb_put_no_sync'].keys()):
    if count not in operation_counts:  # Avoid duplicate x values
        operation_counts.append(count)
    rocksdb_no_sync_throughput.append(data['rockdb_put_no_sync'][count])

# Create the line plot
plt.figure(figsize=(10, 6))
plt.plot(operation_counts, ozonedb_throughput, marker='o', label='ozonedb_put', color='blue')
plt.plot(operation_counts, rocksdb_throughput, marker='o', label='rocksdb_put', color='orange')
plt.plot(operation_counts, rocksdb_no_sync_throughput, marker='o', label='rockdb_put_no_sync', color='green')

# Adding labels and title
plt.title('Throughput vs Operation Count')
plt.xlabel('Operation Count')
plt.ylabel('Throughput (ops/s)')
plt.xticks(operation_counts)  # Set x-ticks to be operation counts
plt.legend()
plt.grid()

# Show the plot
plt.show()
plt.savefig('throughput_vs_operation_count.png')
