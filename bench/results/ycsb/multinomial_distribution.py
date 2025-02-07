import numpy as np
from numpy.random import multinomial
import matplotlib.pyplot as plt

ps = [1/64] * 64
xs = multinomial(915,  pvals=ps, size=20)
normalized_xs = xs / 915
print(normalized_xs)
std_devs = np.std(normalized_xs, axis=1)  # axis=1 means calculating std across each row (each experiment)
print("Standard deviations for the first 10 experiments:")
print(std_devs[:20])

plt.figure(figsize=(10, 6))
for i, x in enumerate(normalized_xs[0:1]):
    x = np.sort(x)
    plt.bar(range(64), x, label=f'Experiment {i+1}')
plt.xlabel('Category')
plt.ylabel('Normalized Count')
plt.title('Normalized Multinomial Distribution Across 20 Experiments')
plt.legend(loc='upper right')
plt.show()
plt.savefig('multinomial_distribution.png')