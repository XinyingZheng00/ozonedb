import matplotlib.pyplot as plt

# Data: RocksDB and OzoneDB (sum of client throughputs)
rocksdb_throughput_MB = 184.43  # RocksDB throughput in MB/s
rocksdb_throughput_ops = 184.427  # RocksDB throughput in ops/s

# OzoneDB throughputs for different client counts (MB/s and ops/s)
ozonedb_clients = [1, 2, 4, 8, 16]  # Number of clients

# Throughput in MB/s for each client set
ozonedb_throughputs_MB = [
    237.159,             # 1 client
    122.137 + 122.763,   # 2 clients
    59.3479 + 59.6039 + 59.6953 + 59.8807,  # 4 clients
    38.6195 + 38.4734 + 38.4585 + 38.4294 + 38.3823 + 38.3803 + 38.3753 + 38.3643,  # 8 clients
    21.0312 + 20.9128 + 20.7579 + 20.7594 + 20.7411 + 20.7001 + 20.6861 + 20.6866 +
    20.6759 + 20.6605 + 20.6553 + 20.6516 + 20.6498 + 20.6464 + 20.6454 + 20.6415  # 16 clients
]

# Throughput in ops/s for each client set
ozonedb_throughputs_ops = [
    237.156,             # 1 client
    122.135 + 122.761,   # 2 clients
    59.347 + 59.603 + 59.6944 + 59.8798,  # 4 clients
    38.6189 + 38.4728 + 38.4579 + 38.4288 + 38.3817 + 38.3797 + 38.3747 + 38.3637,  # 8 clients
    21.0309 + 20.9125 + 20.7576 + 20.759 + 20.7408 + 20.6998 + 20.6857 + 20.6863 +
    20.6756 + 20.6602 + 20.655 + 20.6513 + 20.6495 + 20.6461 + 20.6451 + 20.6412  # 16 clients
]

# Plot throughput (MB/s)
# plt.figure(figsize=(10, 6))
# plt.plot(ozonedb_clients, ozonedb_throughputs_MB, label="OzoneDB Throughput (MB/s)", marker='o')
# plt.axhline(y=rocksdb_throughput_MB, color='r', linestyle='--', label="RocksDB Throughput (MB/s)")

# # Plot throughput (ops/s) on the same graph
plt.plot(ozonedb_clients, ozonedb_throughputs_ops, label="OzoneDB Throughput (ops/s)", marker='o')
plt.axhline(y=rocksdb_throughput_ops, color='g', linestyle='--', label="RocksDB Throughput (ops/s)")

# Add labels and title
plt.xlabel("Number of Clients")
plt.ylabel("Throughput")
plt.title("Throughput Comparison: RocksDB vs OzoneDB (MB/s and ops/s)")

# Show legends
plt.legend()

# Display the graph
plt.grid(True)
plt.show()
plt.savefig("../bench/throughput_comparison_op.png")
