"""
Sample Data Generator for Sales Transactions
From Krish Naik's Big Data Course

Generates realistic e-commerce sales data for practice.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_sales_data(num_records: int = 10000, output_file: str = 'sales_data.csv'):
    """
    Generate sample sales transaction data.
    
    Args:
        num_records: Number of records to generate
        output_file: Output CSV file name
    """
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Product categories and names
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
    
    products = {
        'Electronics': ['Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Camera', 'Smart Watch'],
        'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes', 'Hat'],
        'Books': ['Fiction Novel', 'Technical Manual', 'Cookbook', 'Biography', 'Textbook', 'Comic'],
        'Home & Garden': ['Chair', 'Table', 'Lamp', 'Plant', 'Vase', 'Mirror'],
        'Sports': ['Basketball', 'Tennis Racket', 'Running Shoes', 'Yoga Mat', 'Bicycle', 'Helmet'],
        'Toys': ['Action Figure', 'Board Game', 'Puzzle', 'Doll', 'Building Blocks', 'RC Car']
    }
    
    # Generate data
    data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(num_records):
        # Random timestamp within the year
        random_days = random.randint(0, 365)
        transaction_date = start_date + timedelta(days=random_days)
        
        # Random category and product
        category = random.choice(categories)
        product = random.choice(products[category])
        
        # Price based on category
        price_ranges = {
            'Electronics': (50, 2000),
            'Clothing': (10, 200),
            'Books': (5, 50),
            'Home & Garden': (20, 500),
            'Sports': (15, 300),
            'Toys': (5, 100)
        }
        
        min_price, max_price = price_ranges[category]
        price = round(random.uniform(min_price, max_price), 2)
        
        # Random quantity (weighted towards lower quantities)
        quantity = np.random.choice([1, 2, 3, 4, 5], p=[0.5, 0.25, 0.15, 0.07, 0.03])
        
        # Customer information
        customer_id = f"CUST_{random.randint(1000, 9999)}"
        
        # Sales person
        sales_person = random.choice(['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eva Brown'])
        
        # Store location
        store = random.choice(['Store_A', 'Store_B', 'Store_C', 'Store_D', 'Store_E'])
        
        # Payment method
        payment_method = random.choice(['Credit Card', 'Debit Card', 'Cash', 'PayPal', 'Bank Transfer'])
        
        # Discount (0-20%)
        discount_percent = random.choice([0, 5, 10, 15, 20])
        discounted_price = price * (1 - discount_percent / 100)
        total_amount = discounted_price * quantity
        
        record = {
            'transaction_id': f"TXN_{i+1:06d}",
            'date': transaction_date.strftime('%Y-%m-%d'),
            'time': transaction_date.strftime('%H:%M:%S'),
            'customer_id': customer_id,
            'product_name': product,
            'category': category,
            'quantity': quantity,
            'unit_price': price,
            'discount_percent': discount_percent,
            'total_amount': round(total_amount, 2),
            'payment_method': payment_method,
            'sales_person': sales_person,
            'store_location': store
        }
        
        data.append(record)
    
    # Create DataFrame and save
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    
    print(f"Generated {num_records} sales records")
    print(f"Saved to: {output_file}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Total revenue: ${df['total_amount'].sum():,.2f}")
    print(f"Average transaction: ${df['total_amount'].mean():.2f}")
    
    return df

def generate_sample_datasets():
    """
    Generate all sample datasets for the course.
    """
    print("=== Generating Sample Datasets ===")
    
    # Generate sales data
    print("\n1. Generating sales data...")
    sales_df = generate_sales_data(10000, 'sales_data.csv')
    
    # Display sample
    print("\nSample sales data:")
    print(sales_df.head())
    
    print("\nCategory distribution:")
    print(sales_df['category'].value_counts())

if __name__ == "__main__":
    generate_sample_datasets()