# Exercises for Python Big Data Course
*From Krish Naik's Big Data Course*

## Exercise Set 1: Python Fundamentals

### Exercise 1.1: Memory-Efficient Data Processing
Create a function that processes a list of 1 million numbers and calculates:
- Sum of all even numbers
- Average of all odd numbers
- Count of numbers divisible by 3

**Requirements:**
- Use generators for memory efficiency
- Process in chunks of 10,000
- Handle any potential errors

### Exercise 1.2: Dictionary Operations
Given a list of sales transactions, create a function that:
- Calculates total revenue by product category
- Finds the top 5 best-selling products
- Identifies the month with highest sales

### Exercise 1.3: File Processing
Write a program that:
- Reads a large CSV file line by line
- Filters data based on multiple conditions
- Writes results to a new file
- Reports processing statistics

## Exercise Set 2: NumPy Operations

### Exercise 2.1: Array Manipulations
Create a 3D NumPy array (100x50x20) and perform:
- Reshape to 2D array
- Calculate statistics along different axes
- Find indices of maximum values
- Normalize the data

### Exercise 2.2: Performance Optimization
Compare the performance of:
- Pure Python list operations
- NumPy array operations
- Memory usage differences
Create visualizations showing the results.

### Exercise 2.3: Missing Data Handling
Given an array with missing values (NaN):
- Identify missing data patterns
- Implement different imputation strategies
- Compare the impact on statistical measures

## Exercise Set 3: Pandas Data Processing

### Exercise 3.1: Data Cleaning
Clean a messy dataset containing:
- Missing values
- Duplicate records
- Inconsistent data formats
- Outliers

### Exercise 3.2: Time Series Analysis
Work with time series data to:
- Resample data at different frequencies
- Calculate moving averages
- Identify trends and seasonality
- Handle missing timestamps

### Exercise 3.3: Advanced Groupby Operations
Perform complex aggregations:
- Multiple grouping levels
- Custom aggregation functions
- Window functions
- Pivot tables and cross-tabulations

## Exercise Set 4: Apache Spark

### Exercise 4.1: RDD Operations
Create and manipulate RDDs to:
- Read large text files
- Apply transformations and actions
- Optimize partition strategies
- Handle skewed data

### Exercise 4.2: DataFrame Processing
Process large datasets using Spark DataFrames:
- Join multiple large datasets
- Perform complex aggregations
- Optimize query performance
- Cache strategic DataFrames

### Exercise 4.3: Spark SQL
Write SQL queries for:
- Window functions
- Common Table Expressions (CTEs)
- Complex joins and subqueries
- Performance tuning

## Project-Based Exercises

### Project 1: E-commerce Analytics
Analyze e-commerce data to:
- Customer segmentation
- Product recommendation system
- Sales forecasting
- Churn prediction

### Project 2: Log Analysis
Process web server logs to:
- Identify traffic patterns
- Detect anomalies
- Performance monitoring
- Real-time dashboards

### Project 3: Financial Data Processing
Work with financial data for:
- Risk analysis
- Portfolio optimization
- Fraud detection
- Regulatory reporting

## Datasets
Sample datasets are available in the `/data` directory:
- `sales_data.csv` - E-commerce sales transactions
- `user_logs.txt` - Web application logs
- `stock_prices.json` - Financial market data
- `customer_data.parquet` - Customer information

## Solutions
Solutions to all exercises are provided in the `/solutions` directory.

## Evaluation Criteria
Each exercise will be evaluated on:
- Code correctness and efficiency
- Memory optimization
- Error handling
- Code documentation
- Performance analysis

## Additional Resources
- [NumPy Documentation](https://numpy.org/doc/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Krish Naik YouTube Channel](https://www.youtube.com/user/krishnaik06)