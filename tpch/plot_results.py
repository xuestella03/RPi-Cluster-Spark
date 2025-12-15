import pandas as pd
import matplotlib.pyplot as plt

csv_filename = "tpch/results/dietpi-eclipse-j9.csv"

df = pd.read_csv(csv_filename)

# The .mean() method calculates the average for each column automatically
averages = df.mean(numeric_only=True) # numeric_only=True ensures only numerical columns are averaged

print(f"Averages for columns in '{csv_filename}':")
print(averages)

# TODO: Save the averages to a summary file 
# where each row has the averages for the config 

plt.figure(figsize=(6, 4))
plt.bar(averages.index.astype(str), averages.values)

plt.xlabel("Column")
plt.ylabel("Average Value")
plt.title("Average per Column")
plt.tight_layout()

plt.savefig("tpch/graphs/dietpi-eclipse-j9.png", dpi=300)

plt.close()