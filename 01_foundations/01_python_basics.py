"""
Python Basics for Big Data
From Krish Naik's Big Data Course

This module covers fundamental Python concepts essential for big data processing.
"""

# 1. Working with Large Lists and Generators
def process_large_dataset(data_size=1000000):
    """
    Demonstrate memory-efficient processing of large datasets
    """
    # Generator for memory efficiency
    def data_generator(size):
        for i in range(size):
            yield i ** 2
    
    # Process data in chunks
    chunk_size = 10000
    total_sum = 0
    
    for i, value in enumerate(data_generator(data_size)):
        total_sum += value
        
        # Process in chunks to avoid memory overflow
        if (i + 1) % chunk_size == 0:
            print(f"Processed {i + 1} items, current sum: {total_sum}")
    
    return total_sum

# 2. List Comprehensions vs Generators
def compare_memory_usage():
    """
    Compare memory usage between list comprehensions and generators
    """
    import sys
    
    # List comprehension (stores all values in memory)
    list_comp = [x**2 for x in range(100000)]
    list_memory = sys.getsizeof(list_comp)
    
    # Generator (lazy evaluation)
    gen_comp = (x**2 for x in range(100000))
    gen_memory = sys.getsizeof(gen_comp)
    
    print(f"List comprehension memory: {list_memory} bytes")
    print(f"Generator memory: {gen_memory} bytes")
    print(f"Memory saved: {list_memory - gen_memory} bytes")

# 3. Dictionary Operations for Data Processing
def process_data_with_dicts():
    """
    Demonstrate efficient dictionary operations for data processing
    """
    # Sample data processing
    sales_data = [
        {"product": "laptop", "price": 1000, "quantity": 5},
        {"product": "mouse", "price": 25, "quantity": 100},
        {"product": "keyboard", "price": 75, "quantity": 50},
        {"product": "laptop", "price": 1200, "quantity": 3},
    ]
    
    # Aggregate sales by product
    product_sales = {}
    for sale in sales_data:
        product = sale["product"]
        revenue = sale["price"] * sale["quantity"]
        
        if product in product_sales:
            product_sales[product] += revenue
        else:
            product_sales[product] = revenue
    
    return product_sales

# 4. File Processing Techniques
def process_large_file(filename="sample_data.txt"):
    """
    Efficiently process large files line by line
    """
    try:
        # Process file line by line to avoid loading entire file in memory
        line_count = 0
        total_length = 0
        
        with open(filename, 'r') as file:
            for line in file:
                line_count += 1
                total_length += len(line.strip())
                
                # Process every 1000 lines
                if line_count % 1000 == 0:
                    print(f"Processed {line_count} lines")
        
        avg_length = total_length / line_count if line_count > 0 else 0
        print(f"Total lines: {line_count}, Average line length: {avg_length:.2f}")
        
    except FileNotFoundError:
        print(f"File {filename} not found. Creating sample file...")
        create_sample_file(filename)

def create_sample_file(filename="sample_data.txt"):
    """
    Create a sample file for demonstration
    """
    with open(filename, 'w') as file:
        for i in range(10000):
            file.write(f"This is line {i} with some sample data for processing.\n")
    print(f"Created sample file: {filename}")

# 5. Error Handling for Data Processing
def robust_data_processing(data_list):
    """
    Demonstrate robust error handling in data processing
    """
    results = []
    errors = []
    
    for i, item in enumerate(data_list):
        try:
            # Simulate data processing that might fail
            if isinstance(item, (int, float)):
                result = item ** 0.5  # Square root
                results.append(result)
            else:
                # Try to convert to float
                result = float(item) ** 0.5
                results.append(result)
                
        except (ValueError, TypeError) as e:
            error_msg = f"Error processing item {i}: {item} - {str(e)}"
            errors.append(error_msg)
            print(error_msg)
    
    return results, errors

if __name__ == "__main__":
    print("=== Python Basics for Big Data ===\n")
    
    # 1. Large dataset processing
    print("1. Processing large dataset:")
    result = process_large_dataset(50000)
    print(f"Final sum: {result}\n")
    
    # 2. Memory comparison
    print("2. Memory usage comparison:")
    compare_memory_usage()
    print()
    
    # 3. Dictionary operations
    print("3. Data aggregation with dictionaries:")
    sales_summary = process_data_with_dicts()
    for product, revenue in sales_summary.items():
        print(f"{product}: ${revenue}")
    print()
    
    # 4. File processing
    print("4. File processing:")
    process_large_file()
    print()
    
    # 5. Error handling
    print("5. Robust data processing:")
    test_data = [1, 4, 9, "16", "invalid", 25, None, 36]
    results, errors = robust_data_processing(test_data)
    print(f"Successfully processed: {len(results)} items")
    print(f"Errors encountered: {len(errors)} items")