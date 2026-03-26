# SyntheticDataGenerator - Complete Guide

## Overview

The `SyntheticDataGenerator` class creates large-scale, realistic synthetic e-commerce data with advanced statistical distributions and proper relationships between entities.

## Key Features

### 1. **Advanced Statistical Distributions**

#### Customer Data
- **Age Distribution**: Normal distribution (μ=35, σ=15, clipped to [18, 75])
  - Realistic age spread across customer base
  - Mean centered around typical online shopper age

- **Registration Dates**: Uniform distribution over last 3 years
  - Realistic customer acquisition timeline
  - Supports historical analysis

#### Product Data
- **Price Ranges**: Category-specific ranges
  - Electronics: $50–$500
  - Clothing: $10–$150
  - Home: $20–$400
  - Sports: $15–$300
  - Books: $5–$50

- **Ratings**: Uniform distribution [1.0, 5.0]
  - Rounded to 1 decimal place
  - Realistic product quality variations

- **Stock**: Uniform distribution [10, 10,000]
  - Realistic inventory levels
  - Supports supply chain analysis

#### Order Data - **Pareto Distribution (80/20 Rule)**
- 20% of customers generate ~80% of orders
- Reflects real-world e-commerce behavior
- Top customers are "power users"
- Tail distribution for order frequency analysis

### 2. **Data Generation at Scale**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_customers` | 100,000 | Customer records |
| `num_products` | 10,000 | Product records |
| `num_orders` | 1,000,000 | Order records |
| `seed` | None | Optional reproducibility seed |

### 3. **Progress Tracking**

All generation steps use **tqdm progress bars**:
- Names generation
- Email generation
- City generation
- Country generation
- Registration dates
- Price calculation
- Order date generation

```
Generating 100,000 customer records...
Names: 100%|██████████| 100000/100000 [00:05<00:00, 19542.25it/s]
Emails: 100%|██████████| 100000/100000 [00:08<00:00, 12345.67it/s]
...
```

### 4. **Comprehensive Logging**

All operations logged with:
- Timestamp
- Log level (INFO, ERROR, WARNING)
- Operation details
- Record counts

Example log output:
```
2024-03-26 10:15:23 - root - INFO - SyntheticDataGenerator initialized: customers=100,000, products=10,000, orders=1,000,000
2024-03-26 10:15:24 - root - INFO - Generating 100,000 customer records...
2024-03-26 10:16:12 - root - INFO - Generated 100,000 customers successfully
```

### 5. **Pandas DataFrames**

All data returned as pandas DataFrames for easy manipulation:

#### Customer DataFrame
```
customer_id   name           email                  age  city           country              registration_date
CUST000001    John Smith     john@example.com       34   New York       United States       2023-01-15
CUST000002    Jane Doe       jane@example.com       42   Los Angeles    United States       2023-02-20
...
```

#### Product DataFrame
```
product_id   name              category      price   stock  rating
PROD00001    Premium Widget    Electronics   199.99  2543   4.2
PROD00002    Cotton Shirt      Clothing      45.99   8923   3.8
...
```

#### Order DataFrame
```
order_id      customer_id  product_id  quantity  order_date              unit_price
ORD0000001    CUST000042   PROD00234   2        2024-01-15 14:32:00    199.99
ORD0000002    CUST000001   PROD00567   1        2024-01-16 09:15:00    45.99
...
```

### 6. **Type Hints and Docstrings**

- Full type hints on all methods
- Comprehensive docstrings for every method
- Clear parameter descriptions
- Return type specifications

## Usage Examples

### Basic Usage

```python
from src.data_generator import SyntheticDataGenerator

# Create generator with defaults (100K customers, 10K products, 1M orders)
generator = SyntheticDataGenerator(seed=42)

# Generate all data
customers, products, orders = generator.generate_all()

# Save to CSV
generator.save_to_csv(output_dir="data/raw")
```

### Small-Scale Testing

```python
# For quick testing/development
generator = SyntheticDataGenerator(
    num_customers=1_000,
    num_products=100,
    num_orders=10_000,
    seed=42
)

customers, products, orders = generator.generate_all()
```

### Large-Scale Production

```python
# For full production dataset
generator = SyntheticDataGenerator(
    num_customers=1_000_000,  # 1 million customers
    num_products=100_000,      # 100K products
    num_orders=10_000_000,     # 10 million orders
    seed=42
)

customers, products, orders = generator.generate_all()
generator.save_to_csv()
```

### Accessing Data

```python
# Get DataFrames
customers_df = generator.customers_df
products_df = generator.products_df
orders_df = generator.orders_df

# Perform analysis
avg_customer_age = customers_df["age"].mean()
total_products = len(products_df)
total_revenue = (orders_df["quantity"] * orders_df["unit_price"]).sum()

# Use with pandas operations
top_customers = (
    orders_df.groupby("customer_id")
    .agg({"quantity": "sum"})
    .nlargest(10, "quantity")
)
```

### Generate Statistics

```python
# Get statistics dictionary
stats = generator.get_statistics()

print(stats)
# Output:
# {
#   'customers': {
#     'count': 100000,
#     'avg_age': 35.2,
#     'age_std': 14.8
#   },
#   'products': {
#     'count': 10000,
#     'avg_price': 127.45,
#     'avg_rating': 3.1
#   },
#   'orders': {
#     'count': 1000000,
#     'avg_quantity': 3.2,
#     'unique_customers': 89234,
#     'orders_per_customer': 11.2
#   }
# }

# Print formatted statistics
generator.print_statistics()
```

## API Reference

### `SyntheticDataGenerator` Class

#### Constructor
```python
def __init__(
    self,
    num_customers: int = 100_000,
    num_products: int = 10_000,
    num_orders: int = 1_000_000,
    seed: Optional[int] = None,
) -> None
```

#### Methods

##### `generate_customers() -> pd.DataFrame`
Generates customer data with normal distribution for age.

##### `generate_products() -> pd.DataFrame`
Generates product data with category-specific pricing.

##### `generate_orders() -> pd.DataFrame`
Generates orders with Pareto distribution (80/20 rule).

##### `generate_all() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]`
Generates all datasets. Returns tuple of (customers, products, orders).

##### `save_to_csv(output_dir: str = "data/raw") -> None`
Saves generated DataFrames to CSV files.

##### `get_statistics() -> dict`
Returns summary statistics as dictionary.

##### `print_statistics() -> None`
Prints formatted summary statistics to console.

## Data Schema

### Customers Table
| Column | Type | Range | Description |
|--------|------|-------|-------------|
| customer_id | str | CUST000001-CUST100000 | Unique identifier |
| name | str | - | Full name from Faker |
| email | str | - | Email address |
| age | int | 18-75 | Age (normal distribution) |
| city | str | - | City from Faker |
| country | str | - | Country from Faker |
| registration_date | str | Last 3 years | YYYY-MM-DD format |

### Products Table
| Column | Type | Range | Description |
|--------|------|-------|-------------|
| product_id | str | PROD00001-PROD10000 | Unique identifier |
| name | str | - | Product name |
| category | str | {Electronics, Clothing, Home, Sports, Books} | Product category |
| price | float | 5.0-500.0 | Category-specific range |
| stock | int | 10-10000 | Inventory quantity |
| rating | float | 1.0-5.0 | Product rating |

### Orders Table
| Column | Type | Range | Description |
|--------|------|-------|-------------|
| order_id | str | ORD0000001-ORD1000000 | Unique identifier |
| customer_id | str | CUST* | Foreign key to customers |
| product_id | str | PROD* | Foreign key to products |
| quantity | int | 1-10 | Units ordered |
| order_date | str | Last 2 years | YYYY-MM-DD HH:MM:SS |
| unit_price | float | - | Price from product table |

## Performance Notes

### Generation Times (Approximate)

| Scale | Customers | Products | Orders | Time |
|-------|-----------|----------|--------|------|
| Test | 1,000 | 100 | 10,000 | ~5 sec |
| Small | 10,000 | 1,000 | 100,000 | ~30 sec |
| Medium | 100,000 | 10,000 | 1,000,000 | ~3 min |
| Large | 1,000,000 | 100,000 | 10,000,000 | ~30 min |

### Memory Usage

- Each customer record: ~200 bytes
- Each product record: ~150 bytes
- Each order record: ~100 bytes

For 100K customers, 10K products, 1M orders: ~150-200 MB

## Pareto Distribution Verification

The Pareto parameter (a=1.16) ensures approximately 80/20 distribution:
- Top 20% of customers make ~80% of orders
- Validates realistic e-commerce behavior
- Useful for analyzing customer value distribution

```python
# Verify Pareto distribution
orders_by_customer = orders_df.groupby("customer_id").size()
total_orders = len(orders_df)
top_20_pct = int(len(customers_df) * 0.2)
top_20_pct_orders = orders_by_customer.nlargest(top_20_pct).sum()
percentage = (top_20_pct_orders / total_orders) * 100

print(f"Top 20% of customers: {percentage:.1f}% of orders")
# Output: Top 20% of customers: ~78-82% of orders
```

## Integration with Spark

```python
from src.data_generator import SyntheticDataGenerator
from pyspark.sql import SparkSession

# Generate data
generator = SyntheticDataGenerator()
customers, products, orders = generator.generate_all()

# Create Spark session
spark = SparkSession.builder.appName("Analytics").getOrCreate()

# Convert to Spark DataFrames
customers_spark = spark.createDataFrame(customers)
products_spark = spark.createDataFrame(products)
orders_spark = spark.createDataFrame(orders)

# Perform Spark analysis
top_customers = (
    orders_spark.groupBy("customer_id")
    .agg({"quantity": "sum"})
    .orderBy("quantity", ascending=False)
    .limit(10)
)
```

## Requirements

```
pyspark==3.5.0
faker==22.6.0
pandas==2.2.0
numpy==1.26.3
tqdm==4.66.1
python-dotenv==1.0.0
logging-loki==0.3.2
```

Install with:
```bash
pip install -r requirements.txt
```

## License

MIT License - See LICENSE file for details
