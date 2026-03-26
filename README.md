# E-Commerce Data Pipeline

A comprehensive data pipeline project that generates synthetic e-commerce data and performs business analytics using Apache PySpark.

## Project Overview

This project demonstrates a complete ETL (Extract, Transform, Load) workflow for e-commerce data:

- **Data Generation**: Creates realistic fake customer, product, and order data using the Faker library
- **Data Processing**: Transforms and analyzes data using Apache PySpark for scalability
- **Business Intelligence**: Derives actionable insights such as customer behavior patterns, product performance, and sales trends

## Folder Structure

```
genai-pspark-pipeline/
├── src/                          # Source code
│   ├── __init__.py              # Package initialization
│   ├── config.py                # Configuration and constants
│   ├── data_generator.py        # Fake data generation
│   └── spark_analytics.py       # PySpark analysis and insights
├── data/
│   ├── raw/                     # Generated raw CSV files
│   └── processed/               # Processed analysis results
├── tests/                       # Unit and integration tests
├── notebooks/                   # Jupyter notebooks for exploration
├── requirements.txt             # Project dependencies
├── README.md                    # This file
└── .gitignore                   # Git ignore rules
```

## Features

### Data Generator (`src/data_generator.py`)
- Generates realistic customer data (names, emails, locations, registration dates)
- Creates product catalog with diverse categories and pricing
- Produces order records with items, timestamps, and order status
- Exports data to CSV format in `data/raw/`

### Spark Analytics (`src/spark_analytics.py`)
- Loads CSV data into Spark DataFrames
- Performs aggregations and transformations
- Generates business insights:
  - Top customers by purchase value
  - Best-selling products
  - Revenue trends over time
  - Customer segmentation analysis
  - Product category performance

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd genai-pspark-pipeline
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Generate Fake Data
```python
from src.data_generator import DataGenerator

generator = DataGenerator(num_customers=1000, num_products=100, num_orders=5000)
generator.generate_all()
print("Data generation complete!")
```

### Run Analytics
```python
from src.spark_analytics import SparkAnalytics

analytics = SparkAnalytics()
analytics.load_data()
analytics.analyze_top_customers()
analytics.analyze_top_products()
analytics.analyze_revenue_by_category()
analytics.generate_report()
```

### Running Tests
```bash
python -m pytest tests/
```

## Configuration

Edit `src/config.py` to customize:
- Output file paths
- Number of records to generate
- Date ranges for data
- Spark session settings

## Requirements

- Python 3.8+
- Apache Spark 3.5.0+
- Java Runtime Environment (JRE) for Spark

## Example Output

The pipeline generates CSV files in `data/raw/`:
- `customers.csv` - Customer master data
- `products.csv` - Product catalog
- `orders.csv` - Transaction records

Analysis results are saved to `data/processed/`:
- `top_customers.csv`
- `top_products.csv`
- `revenue_by_category.csv`
- `analysis_report.txt`

## Logging

The project uses Python's logging module to track operations:
- Log level can be configured in `src/config.py`
- Logs include timestamps, severity levels, and operation details
- Useful for monitoring data generation and analysis processes

## Future Enhancements

- [ ] Add streaming data support
- [ ] Implement real-time dashboards
- [ ] Add machine learning models for customer segmentation
- [ ] Deploy to cloud platforms (AWS, GCP, Azure)
- [ ] Add data validation and quality checks
- [ ] Implement incremental updates

## License

MIT License - See LICENSE file for details
