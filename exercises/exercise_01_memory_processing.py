"""
Exercise 1: Memory-Efficient Data Processing
From Krish Naik's Big Data Course

Complete the functions below to process large datasets efficiently.
"""

import time
import sys
from typing import Tuple, Generator

def efficient_number_processor(data_size: int = 1000000) -> Tuple[int, float, int]:
    """
    Process a large list of numbers efficiently using generators.
    
    Args:
        data_size: Number of integers to process
    
    Returns:
        Tuple containing:
        - Sum of all even numbers
        - Average of all odd numbers
        - Count of numbers divisible by 3
    
    Requirements:
    - Use generators for memory efficiency
    - Process in chunks of 10,000
    - Handle any potential errors
    """
    
    # TODO: Implement your solution here
    # Hint: Create a generator function for the data
    # Hint: Use chunked processing to avoid memory issues
    # Hint: Handle division by zero for average calculation
    
    pass  # Remove this and implement your solution

def chunk_processor(data_generator: Generator, chunk_size: int = 10000) -> Tuple[int, float, int]:
    """
    Process data in chunks for memory efficiency.
    
    Args:
        data_generator: Generator yielding numbers
        chunk_size: Size of each processing chunk
    
    Returns:
        Tuple of results (even_sum, odd_average, divisible_by_3_count)
    """
    
    # TODO: Implement chunked processing logic
    # Initialize counters and accumulators
    
    pass  # Remove this and implement your solution

def data_generator(size: int) -> Generator[int, None, None]:
    """
    Generate numbers for processing.
    
    Args:
        size: Number of integers to generate
    
    Yields:
        Integer values
    """
    
    # TODO: Implement generator that yields numbers 1 to size
    # This should be memory efficient
    
    pass  # Remove this and implement your solution

def memory_usage_comparison():
    """
    Compare memory usage of different approaches.
    """
    
    size = 100000
    
    # Method 1: List comprehension (stores all in memory)
    # TODO: Create list and measure memory usage
    
    # Method 2: Generator (lazy evaluation)
    # TODO: Create generator and measure memory usage
    
    # Print comparison results
    pass

# Test your implementation
if __name__ == "__main__":
    print("=== Exercise 1: Memory-Efficient Data Processing ===")
    
    # Test with smaller dataset first
    print("Testing with 50,000 numbers:")
    start_time = time.time()
    
    try:
        even_sum, odd_avg, div_by_3 = efficient_number_processor(50000)
        end_time = time.time()
        
        print(f"Sum of even numbers: {even_sum}")
        print(f"Average of odd numbers: {odd_avg:.2f}")
        print(f"Count divisible by 3: {div_by_3}")
        print(f"Processing time: {end_time - start_time:.4f} seconds")
        
    except Exception as e:
        print(f"Error: {e}")
    
    # Memory usage comparison
    print("\n=== Memory Usage Comparison ===")
    memory_usage_comparison()
    
    # Expected output (approximate):
    # Sum of even numbers: 624975000
    # Average of odd numbers: 25000.00
    # Count divisible by 3: 16666
    print("\nExpected results for 50,000 numbers:")
    print("Sum of even numbers: ~624,975,000")
    print("Average of odd numbers: ~25,000")
    print("Count divisible by 3: ~16,666")