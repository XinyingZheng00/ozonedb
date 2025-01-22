import matplotlib.pyplot as plt
import numpy as np

# Data for systems
time_sec = np.array([10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 218])

# Throughput values (current ops/sec)
jdbc_throughput = np.array([660.13, 684.9, 702.8, 753.2, 790.9, 803.5, 746.7, 777.9, 738.8, 742.6, 773.3, 775.9, 700.3, 706.48,0,0,0,0,0,0,0,0])
ozonedb_throughput = np.array([452.9, 471.8, 466.2, 481.2, 446.3, 469.2, 417.1, 437.8, 432.2, 429.9, 472.7, 501.4, 390.8, 422.3, 474.1, 488.6, 470.6, 475.6, 445, 469.4, 500.8, 460])
cosmos_throughput = np.array([257.4, 279.2, 284.4, 305.5, 321.8, 329.5, 331.1, 331.7, 329.8, 329.8, 329.1, 327.5, 328.5, 326.6, 312.7, 264.7, 265.5, 315.2, 327.1, 326.5, 329.7, 326.4])

# Plotting the data
plt.figure(figsize=(10, 6))

plt.plot(time_sec, jdbc_throughput, label='JDBC', marker='o')
plt.plot(time_sec, ozonedb_throughput, label='Ozonedb', marker='x')
plt.plot(time_sec, cosmos_throughput, label='Cosmos', marker='s')

plt.title('Throughput over Time for Different Systems')
plt.xlabel('Time (seconds)')
plt.ylabel('Throughput (ops/sec)')
plt.legend()
plt.grid(True)
plt.tight_layout()

# Show the plot
plt.show()
plt.savefig('throughput_comparison.png')
