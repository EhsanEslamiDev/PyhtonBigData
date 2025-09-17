"""
Pandas Basics for Big Data Processing
From Krish Naik's Big Data Course

Essential Pandas operations for data manipulation and analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 1. DataFrame Creation and Basic Operations
def create_dataframes():
    """
    Demonstrate various ways to create DataFrames
    """
    print("=== DataFrame Creation ===")
    
    # From dictionary
    data_dict = {
        'Name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'Age': [25, 30, 35, 28, 32],
        'City': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'],
        'Salary': [75000, 85000, 95000, 72000, 88000]
    }
    df = pd.DataFrame(data_dict)
    print("DataFrame from dictionary:")
    print(df)
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"Data types:\n{df.dtypes}")
    
    return df

# 2. Data Inspection and Information
def inspect_data(df):
    """
    Demonstrate data inspection methods
    """
    print("\n=== Data Inspection ===")
    
    # Basic info
    print("First 3 rows:")
    print(df.head(3))
    
    print("\nLast 2 rows:")
    print(df.tail(2))
    
    print("\nDataFrame info:")
    df.info()
    
    print("\nDescriptive statistics:")
    print(df.describe())
    
    print("\nUnique values in City column:")
    print(df['City'].unique())
    
    print("\nValue counts for City:")
    print(df['City'].value_counts())

# 3. Data Selection and Filtering
def data_selection(df):
    """
    Demonstrate data selection and filtering techniques
    """
    print("\n=== Data Selection and Filtering ===")
    
    # Column selection
    print("Single column (Name):")
    print(df['Name'])
    
    print("\nMultiple columns:")
    print(df[['Name', 'Salary']])
    
    # Row selection
    print("\nRow selection by index:")
    print(df.iloc[1:3])  # By position
    print(df.loc[1:2])   # By label
    
    # Conditional filtering
    print("\nFiltering by condition (Salary > 80000):")
    high_salary = df[df['Salary'] > 80000]
    print(high_salary)
    
    print("\nMultiple conditions:")
    young_high_earners = df[(df['Age'] < 30) & (df['Salary'] > 70000)]
    print(young_high_earners)
    
    # String operations
    print("\nFiltering by string contains:")
    chicago_people = df[df['City'].str.contains('Chicago')]
    print(chicago_people)

# 4. Data Modification
def data_modification(df):
    """
    Demonstrate data modification operations
    """
    print("\n=== Data Modification ===")
    
    # Create a copy to avoid modifying original
    df_copy = df.copy()
    
    # Add new column
    df_copy['Salary_Bonus'] = df_copy['Salary'] * 1.1
    print("Added bonus column:")
    print(df_copy[['Name', 'Salary', 'Salary_Bonus']])
    
    # Modify existing column
    df_copy['Age_Category'] = df_copy['Age'].apply(
        lambda x: 'Young' if x < 30 else 'Experienced'
    )
    print("\nAdded age category:")
    print(df_copy[['Name', 'Age', 'Age_Category']])
    
    # Update values
    df_copy.loc[df_copy['City'] == 'NYC', 'City'] = 'New York'
    print("\nUpdated NYC to New York:")
    print(df_copy)
    
    return df_copy

# 5. Handling Missing Data
def handle_missing_data():
    """
    Demonstrate missing data handling
    """
    print("\n=== Handling Missing Data ===")
    
    # Create DataFrame with missing values
    data_with_nulls = {
        'A': [1, 2, np.nan, 4, 5],
        'B': [np.nan, 2, 3, 4, np.nan],
        'C': [1, 2, 3, 4, 5],
        'D': ['a', 'b', None, 'd', 'e']
    }
    df_nulls = pd.DataFrame(data_with_nulls)
    print("DataFrame with missing values:")
    print(df_nulls)
    
    # Check for missing values
    print("\nMissing value check:")
    print(df_nulls.isnull().sum())
    
    # Drop missing values
    print("\nDrop rows with any null:")
    print(df_nulls.dropna())
    
    print("\nDrop columns with any null:")
    print(df_nulls.dropna(axis=1))
    
    # Fill missing values
    print("\nFill nulls with forward fill:")
    print(df_nulls.fillna(method='ffill'))
    
    print("\nFill with specific values:")
    fill_values = {'A': 0, 'B': df_nulls['B'].mean(), 'D': 'missing'}
    print(df_nulls.fillna(fill_values))

# 6. Sorting and Ranking
def sorting_operations(df):
    """
    Demonstrate sorting and ranking operations
    """
    print("\n=== Sorting and Ranking ===")
    
    # Sort by single column
    print("Sort by salary (ascending):")
    print(df.sort_values('Salary'))
    
    print("\nSort by salary (descending):")
    print(df.sort_values('Salary', ascending=False))
    
    # Sort by multiple columns
    print("\nSort by Age, then Salary:")
    print(df.sort_values(['Age', 'Salary']))
    
    # Ranking
    df_with_rank = df.copy()
    df_with_rank['Salary_Rank'] = df['Salary'].rank(ascending=False)
    print("\nSalary ranking:")
    print(df_with_rank[['Name', 'Salary', 'Salary_Rank']])

# 7. File I/O Operations
def file_operations(df):
    """
    Demonstrate file input/output operations
    """
    print("\n=== File I/O Operations ===")
    
    # Save to CSV
    csv_file = '/tmp/sample_data.csv'
    df.to_csv(csv_file, index=False)
    print(f"Saved DataFrame to {csv_file}")
    
    # Read from CSV
    df_from_csv = pd.read_csv(csv_file)
    print("Loaded DataFrame from CSV:")
    print(df_from_csv)
    
    # Save to JSON
    json_file = '/tmp/sample_data.json'
    df.to_json(json_file, orient='records', indent=2)
    print(f"Saved DataFrame to {json_file}")
    
    # Read from JSON
    df_from_json = pd.read_json(json_file)
    print("Loaded DataFrame from JSON:")
    print(df_from_json)

# 8. Performance Tips for Large Data
def performance_tips():
    """
    Demonstrate performance optimization techniques
    """
    print("\n=== Performance Tips ===")
    
    # Create larger dataset for demonstration
    large_data = {
        'ID': range(100000),
        'Value': np.random.randn(100000),
        'Category': np.random.choice(['A', 'B', 'C'], 100000)
    }
    df_large = pd.DataFrame(large_data)
    
    print(f"Large DataFrame shape: {df_large.shape}")
    print(f"Memory usage: {df_large.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Optimize data types
    df_optimized = df_large.copy()
    df_optimized['Category'] = df_optimized['Category'].astype('category')
    df_optimized['ID'] = df_optimized['ID'].astype('int32')
    
    print(f"Optimized memory usage: {df_optimized.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Chunked processing
    chunk_size = 10000
    print(f"\nProcessing in chunks of {chunk_size}:")
    
    chunk_results = []
    for chunk in pd.read_csv('/tmp/sample_data.csv', chunksize=chunk_size):
        # Process each chunk
        chunk_mean = chunk['Salary'].mean()
        chunk_results.append(chunk_mean)
    
    if chunk_results:
        print(f"Average salary from chunked processing: {np.mean(chunk_results):.2f}")

if __name__ == "__main__":
    print("=== Pandas Basics for Big Data ===")
    
    # Create sample DataFrame
    df = create_dataframes()
    
    # Demonstrate various operations
    inspect_data(df)
    data_selection(df)
    df_modified = data_modification(df)
    handle_missing_data()
    sorting_operations(df)
    file_operations(df)
    performance_tips()