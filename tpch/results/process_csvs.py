import csv
import os
from statistics import mean

# Get all CSV files except averages.csv

csv_files = ['/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/' + f for f in os.listdir('/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results') if f.endswith('.csv') and f != 'averages.csv']
print(csv_files)

# Dictionary to store results
results = {}

# Process each CSV file
for filename in csv_files:

    # full_filename = '/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/' + filename
    # Extract configuration name (remove .csv extension)
    config_name = filename.replace('.csv', '')
    config_name = config_name.replace('/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/', '')
    
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        
        # Skip header row (query numbers)
        next(reader)
        
        # Read all data rows
        data_rows = list(reader)
        
        # Convert to floats and transpose (columns become rows)
        columns = []
        num_cols = len(data_rows[0])
        
        for col_idx in range(num_cols):
            column_values = [float(row[col_idx]) for row in data_rows]
            columns.append(column_values)
        
        # Calculate average for each column (query)
        averages = [mean(col) for col in columns]
        results[config_name] = averages

# Write results to averages.csv
with open('/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/averages.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    
    # Write header (query numbers from first file)
    with open(csv_files[0], 'r') as first_file:
        first_reader = csv.reader(first_file)
        header = next(first_reader)
        writer.writerow(['configuration'] + header)
    
    # Write each configuration's averages
    for config_name in sorted(results.keys()):
        writer.writerow([config_name] + results[config_name])

print("Averages calculated and saved to averages.csv")
print("\nResults:")
for config_name in sorted(results.keys()):
    print(f"{config_name}: {results[config_name]}")