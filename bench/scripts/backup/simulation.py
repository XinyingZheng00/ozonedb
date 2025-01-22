import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import poisson

# Parameters
num_clients = 8  # Number of clients (based on your example)
total_tasks = 176  # Total number of compaction tasks
mu = 10  # Service rate (tasks per second, adjust as needed)
simulation_time = 100  # Total time to simulate in seconds
task_arrival_rate = total_tasks / simulation_time  # Arrival rate (lambda)

# Generate task arrival times using Poisson distribution
task_arrivals = np.random.poisson(task_arrival_rate, total_tasks)

# Simulate each client processing tasks
client_tasks = np.zeros(num_clients)
for task in task_arrivals:
    client_id = np.random.randint(0, num_clients)  # Randomly assign task to a client
    client_tasks[client_id] += 1

# Normalize task counts to percentage of total tasks
client_task_percentages = (client_tasks / total_tasks) * 100
client_task_percentages.sort()

# Empirical data (from your bar chart)
empirical_data = [16, 15, 13, 10, 15, 13, 9, 8]  # Replace with actual values from plot
empirical_data.sort()

# Plot the comparison
clients = range(1, num_clients + 1)
width = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(clients, client_task_percentages, width, label='Simulated')
rects2 = ax.bar([c + width for c in clients], empirical_data, width, label='Empirical')

ax.set_xlabel('Clients')
ax.set_ylabel('Percentage of Total Tasks')
ax.set_title('Comparison of Simulated and Empirical Task Distribution')
ax.legend()

plt.savefig("simulation_vs_empirical.png")
plt.show()
