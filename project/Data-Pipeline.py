import pandas as pd
import sqlite3
import os

# Load the datasets
file_path_1 = '/mnt/data/stadt-neuss-herbstpflanzung-2023 (3).csv'
file_path_2 = '/mnt/data/Clean 2023.csv'

df1 = pd.read_csv(file_path_1)
df2 = pd.read_csv(file_path_2)

# Display the first few rows of each dataset
print("Dataset 1:")
print(df1.head())

print("\nDataset 2:")
print(df2.head())

# Display dataset info
print("\nDataset 2 Info:")
print(df2.info())

print("\nDataset 1 Info:")
print(df1.info())

# Check the number of unique values in df2
print("\nUnique values in df2:")
print(df2.nunique())

# Data Transformation for df2
df2['population'] = df2['Total Population - Male'] + df2['Total Population - Female']
df2.drop(columns=['Total Population - Male', 'Total Population - Female'], inplace=True)
df2['emission_per_person'] = df2['total_emission'] / df2['population']

# Handle missing values: Filling with mean for numerical columns
df2.fillna(df2.mean(), inplace=True)

# Ensure data types are appropriate
df2['Year'] = df2['Year'].astype(int)
df2['population'] = df2['population'].astype(int)
df2['total_emission'] = df2['total_emission'].astype(float)
df2['emission_per_person'] = df2['emission_per_person'].astype(float)

# Select relevant columns for df2
relevant_columns = [
    'Area', 'Year', 'Rural population', 'Urban population', 'population', 'total_emission', 'emission_per_person', 'Average Temperature °C',
    'Crop Residues', 'Rice Cultivation', 'Manure Management', 'Manure left on Pasture', 'Manure applied to Soils'
]
df2 = df2[relevant_columns]

# Display the first few rows of the transformed df2
print("\nTransformed df2:")
print(df2.head())

# Function to check unique columns
def check_unique_columns(df):
    unique_counts = df.nunique()
    total_rows = df.shape[0]
    unique_columns = unique_counts[unique_counts == total_rows].index.tolist()
    return unique_columns

# Check for unique columns in df1
unique_columns_df1 = check_unique_columns(df1)
print("\nUnique columns in df1:", unique_columns_df1)

# Check for unique columns in df2
unique_columns_df2 = check_unique_columns(df2)
print("\nUnique columns in df2:", unique_columns_df2)

# Create a SQLite database connection
conn = sqlite3.connect('/mnt/data/my_database.db')

# Store the transformed datasets into the database
df1.to_sql('dataset1', conn, if_exists='replace', index=False)
df2.to_sql('dataset2', conn, if_exists='replace', index=False)

# Close the database connection
conn.close()
