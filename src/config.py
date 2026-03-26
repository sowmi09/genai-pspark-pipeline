"""
Configuration settings for the e-commerce data pipeline.

This module contains all configurable parameters for data generation,
processing, and analysis, as well as logging settings.
"""

import logging
from pathlib import Path
from typing import Final

# Project Paths
PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
RAW_DATA_DIR: Final[Path] = DATA_DIR / "raw"
PROCESSED_DATA_DIR: Final[Path] = DATA_DIR / "processed"
NOTEBOOKS_DIR: Final[Path] = PROJECT_ROOT / "notebooks"
TESTS_DIR: Final[Path] = PROJECT_ROOT / "tests"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Data File Paths
CUSTOMERS_CSV: Final[Path] = RAW_DATA_DIR / "customers.csv"
PRODUCTS_CSV: Final[Path] = RAW_DATA_DIR / "products.csv"
ORDERS_CSV: Final[Path] = RAW_DATA_DIR / "orders.csv"

# Data Generation Settings
NUM_CUSTOMERS: Final[int] = 1000
NUM_PRODUCTS: Final[int] = 100
NUM_ORDERS: Final[int] = 5000

# Product Categories
PRODUCT_CATEGORIES: Final[list[str]] = [
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports & Outdoors",
    "Toys & Games",
    "Beauty & Personal Care",
    "Food & Beverage",
]

# Price Ranges (in USD)
PRICE_RANGES: Final[dict[str, tuple[float, float]]] = {
    "Electronics": (50.0, 1500.0),
    "Clothing": (15.0, 150.0),
    "Books": (10.0, 50.0),
    "Home & Garden": (20.0, 500.0),
    "Sports & Outdoors": (25.0, 800.0),
    "Toys & Games": (10.0, 200.0),
    "Beauty & Personal Care": (5.0, 150.0),
    "Food & Beverage": (1.0, 50.0),
}

# Order Status Options
ORDER_STATUSES: Final[list[str]] = [
    "pending",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
]

# Logging Configuration
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = (
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

# Spark Configuration
SPARK_APP_NAME: Final[str] = "ECommerce-DataPipeline"
SPARK_MASTER: Final[str] = "local[*]"
SPARK_LOG_LEVEL: Final[str] = "ERROR"

# Output Analysis Files
TOP_CUSTOMERS_OUTPUT: Final[Path] = PROCESSED_DATA_DIR / "top_customers.csv"
TOP_PRODUCTS_OUTPUT: Final[Path] = PROCESSED_DATA_DIR / "top_products.csv"
REVENUE_BY_CATEGORY_OUTPUT: Final[Path] = (
    PROCESSED_DATA_DIR / "revenue_by_category.csv"
)
ANALYSIS_REPORT_OUTPUT: Final[Path] = (
    PROCESSED_DATA_DIR / "analysis_report.txt"
)
