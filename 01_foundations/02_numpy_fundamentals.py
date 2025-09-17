"""
NumPy Fundamentals for Big Data
From Krish Naik's Big Data Course

Essential NumPy operations for efficient numerical computing in big data scenarios.
"""

import numpy as np
import time

# 1. Array Creation and Basic Operations
def numpy_basics():
    """
    Demonstrate basic NumPy array operations
    """
    print("=== NumPy Array Creation ===")
    
    # Creating arrays
    arr1 = np.array([1, 2, 3, 4, 5])
    arr2 = np.arange(0, 10, 2)  # Start, stop, step
    arr3 = np.linspace(0, 1, 11)  # Start, stop, num_points
    arr4 = np.zeros((3, 4))
    arr5 = np.ones((2, 3))
    arr6 = np.random.random((3, 3))
    
    print(f"Array from list: {arr1}")
    print(f"Array with arange: {arr2}")
    print(f"Array with linspace: {arr3}")
    print(f"Zeros array shape: {arr4.shape}")
    print(f"Ones array shape: {arr5.shape}")
    print(f"Random array:\n{arr6}")

# 2. Array Indexing and Slicing
def array_indexing():
    """
    Demonstrate advanced indexing and slicing
    """
    print("\n=== Array Indexing and Slicing ===")
    
    # 2D array
    arr = np.random.randint(0, 100, (5, 6))
    print(f"Original array:\n{arr}")
    
    # Basic slicing
    print(f"First row: {arr[0]}")
    print(f"First column: {arr[:, 0]}")
    print(f"Subarray [1:3, 2:5]:\n{arr[1:3, 2:5]}")
    
    # Boolean indexing
    mask = arr > 50
    print(f"Elements > 50: {arr[mask]}")
    
    # Fancy indexing
    indices = [0, 2, 4]
    print(f"Rows 0, 2, 4:\n{arr[indices]}")

# 3. Mathematical Operations
def mathematical_operations():
    """
    Demonstrate vectorized mathematical operations
    """
    print("\n=== Mathematical Operations ===")
    
    arr1 = np.random.randint(1, 10, (3, 4))
    arr2 = np.random.randint(1, 10, (3, 4))
    
    print(f"Array 1:\n{arr1}")
    print(f"Array 2:\n{arr2}")
    
    # Element-wise operations
    print(f"Addition:\n{arr1 + arr2}")
    print(f"Multiplication:\n{arr1 * arr2}")
    print(f"Power:\n{arr1 ** 2}")
    
    # Aggregate functions
    print(f"Sum of arr1: {np.sum(arr1)}")
    print(f"Mean of arr1: {np.mean(arr1)}")
    print(f"Standard deviation: {np.std(arr1)}")
    print(f"Max value: {np.max(arr1)}")
    print(f"Min value: {np.min(arr1)}")

# 4. Performance Comparison: NumPy vs Pure Python
def performance_comparison():
    """
    Compare NumPy performance with pure Python
    """
    print("\n=== Performance Comparison ===")
    
    size = 1000000
    
    # Pure Python
    python_list = list(range(size))
    start_time = time.time()
    python_result = sum([x**2 for x in python_list])
    python_time = time.time() - start_time
    
    # NumPy
    numpy_array = np.arange(size)
    start_time = time.time()
    numpy_result = np.sum(numpy_array**2)
    numpy_time = time.time() - start_time
    
    print(f"Pure Python time: {python_time:.4f} seconds")
    print(f"NumPy time: {numpy_time:.4f} seconds")
    print(f"NumPy is {python_time/numpy_time:.1f}x faster")
    print(f"Results match: {python_result == numpy_result}")

# 5. Array Reshaping and Manipulation
def array_manipulation():
    """
    Demonstrate array reshaping and manipulation
    """
    print("\n=== Array Manipulation ===")
    
    # Create array
    arr = np.arange(24)
    print(f"Original array: {arr}")
    
    # Reshape
    reshaped = arr.reshape(4, 6)
    print(f"Reshaped (4x6):\n{reshaped}")
    
    # Transpose
    transposed = reshaped.T
    print(f"Transposed:\n{transposed}")
    
    # Flatten
    flattened = reshaped.flatten()
    print(f"Flattened: {flattened}")
    
    # Stack arrays
    arr1 = np.ones((2, 3))
    arr2 = np.zeros((2, 3))
    
    vstacked = np.vstack([arr1, arr2])
    hstacked = np.hstack([arr1, arr2])
    
    print(f"Vertical stack:\n{vstacked}")
    print(f"Horizontal stack:\n{hstacked}")

# 6. Working with Missing Data
def handle_missing_data():
    """
    Demonstrate handling missing data with NumPy
    """
    print("\n=== Handling Missing Data ===")
    
    # Create array with NaN values
    arr = np.array([1.0, 2.0, np.nan, 4.0, 5.0, np.nan, 7.0])
    print(f"Array with NaN: {arr}")
    
    # Check for NaN
    nan_mask = np.isnan(arr)
    print(f"NaN mask: {nan_mask}")
    
    # Remove NaN values
    clean_arr = arr[~nan_mask]
    print(f"Clean array: {clean_arr}")
    
    # Replace NaN with mean
    mean_value = np.nanmean(arr)
    arr_filled = np.where(np.isnan(arr), mean_value, arr)
    print(f"Array with NaN replaced by mean: {arr_filled}")

# 7. Broadcasting
def broadcasting_examples():
    """
    Demonstrate NumPy broadcasting
    """
    print("\n=== Broadcasting Examples ===")
    
    # Broadcasting with different shapes
    arr1 = np.array([[1, 2, 3],
                     [4, 5, 6]])
    arr2 = np.array([10, 20, 30])
    
    print(f"Array 1 shape: {arr1.shape}")
    print(f"Array 2 shape: {arr2.shape}")
    
    result = arr1 + arr2  # Broadcasting
    print(f"Broadcasted addition:\n{result}")
    
    # Broadcasting with scalar
    scalar_result = arr1 * 2
    print(f"Scalar multiplication:\n{scalar_result}")

# 8. Memory Optimization Techniques
def memory_optimization():
    """
    Demonstrate memory optimization techniques
    """
    print("\n=== Memory Optimization ===")
    
    # Data type optimization
    large_array = np.random.randint(0, 255, 1000000)
    
    # Default dtype (int64 or int32)
    default_memory = large_array.nbytes
    print(f"Default dtype memory: {default_memory} bytes")
    
    # Optimized dtype (uint8 for values 0-255)
    optimized_array = large_array.astype(np.uint8)
    optimized_memory = optimized_array.nbytes
    print(f"Optimized dtype memory: {optimized_memory} bytes")
    print(f"Memory saved: {default_memory - optimized_memory} bytes")
    
    # Memory layout (C vs Fortran order)
    c_order = np.random.random((1000, 1000))  # C order (row-major)
    f_order = np.asfortranarray(c_order)      # Fortran order (column-major)
    
    print(f"C order flags: {c_order.flags}")
    print(f"Fortran order flags: {f_order.flags}")

if __name__ == "__main__":
    print("=== NumPy Fundamentals for Big Data ===")
    
    numpy_basics()
    array_indexing()
    mathematical_operations()
    performance_comparison()
    array_manipulation()
    handle_missing_data()
    broadcasting_examples()
    memory_optimization()