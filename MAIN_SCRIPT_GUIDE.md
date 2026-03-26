# main.py - Data Generation Script Guide

## Overview

The `main.py` script is the primary entry point for generating large-scale synthetic e-commerce data. It orchestrates the entire pipeline:

1. **Generates** synthetic customers, products, and orders
2. **Saves** each dataset as Parquet files (compressed, efficient format)
3. **Reports** generation time, file sizes, and performance metrics
4. **Handles** errors gracefully with comprehensive exception handling

## Features

✅ **Parquet File Format**
- Efficient columnar storage format
- Native support in Spark, Pandas, and other tools
- Snappy compression (configurable)
- ~50-70% smaller than CSV files

✅ **Performance Reporting**
- Generation time tracking
- File size calculation and formatting
- Timing breakdown (generation vs. saving)
- Data generation speed metrics

✅ **Comprehensive Error Handling**
- KeyboardInterrupt handling
- FileNotFoundError handling
- PermissionError handling
- MemoryError handling
- ValueError handling
- Generic exception handling with logging

✅ **Logging Throughout**
- All operations logged with timestamps
- Error stack traces captured
- Info messages for progress tracking
- Configurable log levels

✅ **Production-Ready Code**
- Full type hints on all functions
- Comprehensive docstrings
- Modular function design
- Proper exit codes

## Usage

### Basic Usage (Default Parameters)

```bash
python main.py
```

**Default Configuration:**
- Customers: 100,000
- Products: 10,000
- Orders: 1,000,000
- Seed: 42 (reproducible)
- Output: `data/raw/`

### Custom Parameters

Modify the bottom of `main.py`:

```python
if __name__ == "__main__":
    success = main(
        num_customers=1_000_000,      # 1 million customers
        num_products=100_000,          # 100K products
        num_orders=10_000_000,         # 10 million orders
        seed=42                        # Reproducible seed
    )
```

### Scale Options

#### Quick Test (5-10 seconds)
```python
success = main(
    num_customers=1_000,
    num_products=100,
    num_orders=10_000,
    seed=42
)
```

#### Small Dataset (~1 minute)
```python
success = main(
    num_customers=10_000,
    num_products=1_000,
    num_orders=100_000,
    seed=42
)
```

#### Medium Dataset (~3 minutes)
```python
success = main(
    num_customers=100_000,
    num_products=10_000,
    num_orders=1_000_000,
    seed=42
)
```

#### Large Dataset (~30 minutes)
```python
success = main(
    num_customers=1_000_000,
    num_products=100_000,
    num_orders=10_000_000,
    seed=42
)
```

### Non-Reproducible Data

Remove or set seed to None:

```python
success = main(
    num_customers=100_000,
    num_products=10_000,
    num_orders=1_000_000,
    seed=None  # Different data each run
)
```

## Output Format

### Console Output

```
================================================================================
                 SYNTHETIC E-COMMERCE DATA GENERATION
================================================================================

Configuration:
  Customers: 100,000
  Products:  10,000
  Orders:    1,000,000
  Output:    data/raw

2024-03-26 10:15:23 - __main__ - INFO - Initializing SyntheticDataGenerator...
2024-03-26 10:15:24 - __main__ - INFO - Generating synthetic data...
Names: 100%|██████████| 100000/100000 [00:05<00:00, 19542.25it/s]
Emails: 100%|██████████| 100000/100000 [00:08<00:00, 12345.67it/s]
...

================================================================================
              SYNTHETIC DATA GENERATION SUMMARY
================================================================================

CUSTOMERS:
  Total Count: 100,000
  Average Age: 35.2
  Age Std Dev: 14.8

PRODUCTS:
  Total Count: 10,000
  Average Price: $127.45
  Average Rating: 3.1/5.0

ORDERS:
  Total Count: 1,000,000
  Average Quantity: 3.2
  Unique Customers: 89,234
  Orders per Customer: 11.2

================================================================================
                          GENERATION RESULTS
================================================================================

TIMING:
  Total Generation Time: 185.42 seconds
  Generation Speed: 0.05 hours (19.36 seconds per hour)

FILE SIZES:
  Customers            :    15.32 MB
  Products             :     2.18 MB
  Orders               :    82.45 MB
  Total                :    99.95 MB

OUTPUT LOCATIONS:
  Customers            : data/raw/customers.parquet
  Products             : data/raw/products.parquet
  Orders               : data/raw/orders.parquet

================================================================================

TIMING BREAKDOWN:
  Data Generation:      165.23s ( 89.1%)
  Parquet Saving:        20.19s ( 10.9%)
  Total Time:           185.42s
```

## File Output

### Parquet Files Created

```
data/raw/
├── customers.parquet/
│   ├── _metadata
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── ...
├── products.parquet/
│   ├── _metadata
│   ├── part-00000.parquet
│   └── ...
└── orders.parquet/
    ├── _metadata
    ├── part-00000.parquet
    ├── part-00001.parquet
    ├── part-00002.parquet
    └── ...
```

## API Reference

### `main()` Function

```python
def main(
    num_customers: int = 100_000,
    num_products: int = 10_000,
    num_orders: int = 1_000_000,
    seed: Optional[int] = 42,
) -> bool
```

**Parameters:**
- `num_customers` (int): Number of customer records (default: 100,000)
- `num_products` (int): Number of product records (default: 10,000)
- `num_orders` (int): Number of order records (default: 1,000,000)
- `seed` (Optional[int]): Random seed for reproducibility (default: 42)

**Returns:**
- `bool`: True if successful, False if error occurred

**Exit Codes:**
- `0`: Success
- `1`: Error occurred

### Helper Functions

#### `setup_logging() -> logging.Logger`
Configures logging for the script.

#### `format_file_size(size_bytes: int) -> str`
Converts bytes to human-readable format (B, KB, MB, GB, TB, PB).

#### `get_file_size(file_path: str) -> int`
Gets total file size, including subdirectories for Parquet files.

#### `save_as_parquet(df, output_path: str, logger, compression: str) -> bool`
Saves pandas DataFrame as Parquet file with compression.

#### `print_header(title: str) -> None`
Prints formatted section headers.

#### `print_results(generation_time: float, file_sizes: dict) -> None`
Prints formatted results summary.

## Error Handling

The script handles various error scenarios:

| Error Type | Scenario | Action |
|-----------|----------|--------|
| `KeyboardInterrupt` | User interrupts (Ctrl+C) | Logs warning, returns False |
| `FileNotFoundError` | Missing directory/file | Logs error, returns False |
| `PermissionError` | No write permissions | Logs error, returns False |
| `MemoryError` | Insufficient RAM | Logs error with suggestion, returns False |
| `ValueError` | Invalid parameters | Logs error, returns False |
| `Exception` | Any other error | Logs full stack trace, returns False |

### Example Error Output

```
2024-03-26 10:15:23 - __main__ - ERROR - Insufficient memory for data generation. Try reducing the number of records.
Traceback (most recent call last):
  File "main.py", line 185, in main
    customers, products, orders = generator.generate_all()
  File "src/data_generator.py", line 45, in generate_all
    ...
MemoryError: Unable to allocate 5.50 GiB for an array with shape (1000000,) and data type float64
```

## Performance Expectations

### Generation Times

| Scale | Time | Memory |
|-------|------|--------|
| 1K customers, 100 products, 10K orders | 5-10 sec | ~50 MB |
| 10K customers, 1K products, 100K orders | 30 sec | ~200 MB |
| 100K customers, 10K products, 1M orders | 3 min | ~500 MB |
| 1M customers, 100K products, 10M orders | 30 min | ~5 GB |

### File Sizes

| Dataset | Customers | Products | Orders | Total |
|---------|-----------|----------|--------|-------|
| 1M/100/10K | 200 KB | 50 KB | 300 KB | 550 KB |
| 10K/1K/100K | 1.5 MB | 500 KB | 3 MB | 5 MB |
| 100K/10K/1M | 15 MB | 2 MB | 80 MB | 97 MB |
| 1M/100K/10M | 150 MB | 20 MB | 800 MB | 970 MB |

## Integration with Spark

### Loading Parquet Files into Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analytics").getOrCreate()

# Load Parquet files
customers = spark.read.parquet("data/raw/customers.parquet")
products = spark.read.parquet("data/raw/products.parquet")
orders = spark.read.parquet("data/raw/orders.parquet")

# Perform Spark SQL queries
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

result = spark.sql("""
    SELECT customer_id, COUNT(*) as order_count
    FROM orders
    GROUP BY customer_id
    ORDER BY order_count DESC
    LIMIT 10
""")

result.show()
```

## Integration with Pandas

### Loading Parquet Files into Pandas

```python
import pandas as pd

# Load Parquet files
customers = pd.read_parquet("data/raw/customers.parquet")
products = pd.read_parquet("data/raw/products.parquet")
orders = pd.read_parquet("data/raw/orders.parquet")

# Perform pandas operations
top_customers = (
    orders.groupby("customer_id")
    .agg({"quantity": "sum"})
    .nlargest(10, "quantity")
)

print(top_customers)
```

## Troubleshooting

### Out of Memory Error

**Problem:** `MemoryError: Unable to allocate X GiB`

**Solution:** Reduce the number of records:
```python
success = main(
    num_customers=50_000,      # Reduced from 100,000
    num_products=5_000,        # Reduced from 10,000
    num_orders=500_000,        # Reduced from 1,000,000
    seed=42
)
```

### Permission Denied

**Problem:** `PermissionError: [Errno 13] Permission denied`

**Solution:** Ensure `data/raw/` directory is writable:
```bash
chmod -R 755 data/
```

### Interrupted by User

**Problem:** Script exits when Ctrl+C is pressed

**Solution:** Expected behavior - the script handles this gracefully and logs it.

### Slow Performance

**Problem:** Generation taking too long

**Solution:**
1. Reduce dataset size
2. Close other applications to free RAM
3. Check disk space availability
4. Consider using smaller seed value for faster randomization

## Extending the Script

### Custom Compression

Change compression codec in `save_as_parquet()`:

```python
# Options: snappy, gzip, brotli, lz4, zstd
save_as_parquet(customers, customers_path, logger, compression="gzip")
```

### Additional Processing

Add custom processing after generation:

```python
def main(...):
    # ... existing code ...

    # Add custom processing
    customers["age_group"] = pd.cut(
        customers["age"],
        bins=[0, 18, 25, 35, 50, 100],
        labels=["<18", "18-25", "25-35", "35-50", "50+"]
    )

    # Re-save
    save_as_parquet(customers, customers_path, logger)
```

## Environment Variables

Configure via `src/config.py`:

```python
# Log level
LOG_LEVEL = logging.INFO  # or DEBUG, WARNING, ERROR

# Output directory
RAW_DATA_DIR = Path("data/raw")
```

## License

MIT License - See LICENSE file for details
