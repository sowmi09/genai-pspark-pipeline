# Quick Start Guide

## Run the Data Generation Script

### Basic Run (Default Parameters)
```bash
python main.py
```

This generates:
- 100,000 customers
- 10,000 products
- 1,000,000 orders
- Saves as Parquet files in `data/raw/`
- Takes ~3 minutes

### Outputs
```
data/raw/
├── customers.parquet (15 MB)
├── products.parquet (2 MB)
└── orders.parquet (82 MB)
```

## Expected Output

```
================================================================================
                 SYNTHETIC E-COMMERCE DATA GENERATION
================================================================================

Configuration:
  Customers: 100,000
  Products:  10,000
  Orders:    1,000,000
  Output:    data/raw

[Progress bars for generation...]

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

FILE SIZES:
  Customers            :    15.32 MB
  Products             :     2.18 MB
  Orders               :    82.45 MB
  Total                :    99.95 MB

TIMING BREAKDOWN:
  Data Generation:      165.23s ( 89.1%)
  Parquet Saving:        20.19s ( 10.9%)
  Total Time:           185.42s
```

## Load Data in Python

### Pandas
```python
import pandas as pd

customers = pd.read_parquet("data/raw/customers.parquet")
products = pd.read_parquet("data/raw/products.parquet")
orders = pd.read_parquet("data/raw/orders.parquet")

print(customers.head())
print(orders.shape)
```

### PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analytics").getOrCreate()

customers = spark.read.parquet("data/raw/customers.parquet")
products = spark.read.parquet("data/raw/products.parquet")
orders = spark.read.parquet("data/raw/orders.parquet")

orders.createOrReplaceTempView("orders")
spark.sql("SELECT COUNT(*) FROM orders").show()
```

## Custom Parameters

Edit the bottom of `main.py`:

```python
if __name__ == "__main__":
    # Change these values
    success = main(
        num_customers=1_000_000,      # 1 million
        num_products=100_000,          # 100K
        num_orders=10_000_000,         # 10 million
        seed=42
    )
```

Then run:
```bash
python main.py
```

## Quick Tests

### Test Run (10 seconds)
```python
success = main(
    num_customers=1_000,
    num_products=100,
    num_orders=10_000,
    seed=42
)
```

### Small Run (30 seconds)
```python
success = main(
    num_customers=10_000,
    num_products=1_000,
    num_orders=100_000,
    seed=42
)
```

## Error Handling

The script automatically handles:
- ✅ File permission errors
- ✅ Out of memory errors
- ✅ Missing directories (creates them)
- ✅ User interruptions (Ctrl+C)
- ✅ Invalid parameters

Returns:
- `0` on success
- `1` on failure

## File Structure After Running

```
genai-pspark-pipeline/
├── main.py                          ← Run this
├── src/
│   ├── config.py
│   ├── data_generator.py
│   └── spark_analytics.py
└── data/
    └── raw/
        ├── customers.parquet/       ← Generated
        ├── products.parquet/        ← Generated
        └── orders.parquet/          ← Generated
```

## Performance Tips

- **Smaller datasets**: Use fewer records for testing
- **Large datasets**: Run with `nohup` or `screen` for background execution
- **Monitor memory**: Use `htop` or Task Manager
- **Check disk space**: Ensure `data/` has at least 200MB free

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Out of memory | Reduce `num_orders` by half |
| Permission denied | Check `data/raw/` permissions |
| Slow performance | Close other applications |
| Script hangs | Check if tqdm progress is showing |

## Next Steps

1. ✅ Run `python main.py`
2. ✅ Load data with pandas/Spark
3. ✅ Run analytics with `src/spark_analytics.py`
4. ✅ Explore with Jupyter notebooks

## Documentation

- **Full Guide**: [MAIN_SCRIPT_GUIDE.md](MAIN_SCRIPT_GUIDE.md)
- **Data Generator**: [SYNTHETIC_DATA_GUIDE.md](SYNTHETIC_DATA_GUIDE.md)
- **API Docs**: [README.md](README.md)
- **Example Usage**: [example_usage.py](example_usage.py)
