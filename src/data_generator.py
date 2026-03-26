"""
Advanced synthetic data generation module for e-commerce analytics.

This module provides the SyntheticDataGenerator class that creates realistic
e-commerce data with proper statistical distributions using Faker, NumPy, pandas,
and tqdm for efficient data generation with progress tracking.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

from src.config import LOG_FORMAT, DATE_FORMAT, LOG_LEVEL


class SyntheticDataGenerator:
    """Generate large-scale synthetic e-commerce data with realistic distributions.

    This class creates realistic fake data for customers, products, and orders
    using advanced statistical distributions:
    - Pareto distribution for customer order frequency (20% customers = 80% orders)
    - Normal distribution for customer age (mean=35, std=15)
    - Realistic product categories and ratings

    Attributes:
        num_customers (int): Number of customer records to generate.
        num_products (int): Number of product records to generate.
        num_orders (int): Number of order records to generate.
        seed (Optional[int]): Random seed for reproducibility.
        customers_df (pd.DataFrame): Generated customer data.
        products_df (pd.DataFrame): Generated product data.
        orders_df (pd.DataFrame): Generated order data.
    """

    # Product categories with descriptions
    PRODUCT_CATEGORIES: list[str] = [
        "Electronics",
        "Clothing",
        "Home",
        "Sports",
        "Books",
    ]

    # Price ranges per category (min, max)
    PRICE_RANGES: dict[str, Tuple[float, float]] = {
        "Electronics": (50.0, 500.0),
        "Clothing": (10.0, 150.0),
        "Home": (20.0, 400.0),
        "Sports": (15.0, 300.0),
        "Books": (5.0, 50.0),
    }

    def __init__(
        self,
        num_customers: int = 100_000,
        num_products: int = 10_000,
        num_orders: int = 1_000_000,
        seed: Optional[int] = None,
    ) -> None:
        """Initialize the SyntheticDataGenerator.

        Args:
            num_customers: Number of customer records (default: 100,000).
            num_products: Number of product records (default: 10,000).
            num_orders: Number of order records (default: 1,000,000).
            seed: Optional seed for reproducible random data.
        """
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_orders = num_orders

        # Set seeds for reproducibility
        if seed is not None:
            np.random.seed(seed)
            Faker.seed(seed)

        self.faker = Faker()
        self._setup_logging()

        # Initialize dataframes
        self.customers_df: Optional[pd.DataFrame] = None
        self.products_df: Optional[pd.DataFrame] = None
        self.orders_df: Optional[pd.DataFrame] = None

        self.logger.info(
            f"SyntheticDataGenerator initialized: customers={num_customers:,}, "
            f"products={num_products:,}, orders={num_orders:,}"
        )

    def _setup_logging(self) -> None:
        """Configure logging for the data generator."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)

        handler = logging.StreamHandler()
        handler.setLevel(LOG_LEVEL)

        formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
        handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def generate_customers(self) -> pd.DataFrame:
        """Generate customer data with realistic distributions.

        Creates customer records with:
        - customer_id: Unique identifier (CUST000001, CUST000002, ...)
        - name: Full name from Faker
        - email: Email address from Faker
        - age: Normal distribution (mean=35, std=15, range=18-75)
        - city: City from Faker
        - country: Country from Faker
        - registration_date: Random date within last 3 years

        Returns:
            pd.DataFrame: Customer data with 7 columns.
        """
        self.logger.info(
            f"Generating {self.num_customers:,} customer records..."
        )

        customer_ids = [f"CUST{i:06d}" for i in range(1, self.num_customers + 1)]
        names = [self.faker.name() for _ in tqdm(range(self.num_customers), desc="Names")]
        emails = [self.faker.email() for _ in tqdm(range(self.num_customers), desc="Emails")]

        # Age: Normal distribution, mean=35, std=15, clipped to [18, 75]
        ages = np.clip(
            np.random.normal(loc=35, scale=15, size=self.num_customers),
            18,
            75,
        ).astype(int)

        cities = [
            self.faker.city() for _ in tqdm(range(self.num_customers), desc="Cities")
        ]
        countries = [
            self.faker.country() for _ in tqdm(range(self.num_customers), desc="Countries")
        ]

        # Registration dates: Random within last 3 years
        base_date = datetime.now() - timedelta(days=1095)
        registration_dates = [
            (
                base_date
                + timedelta(days=int(np.random.uniform(0, 1095)))
            ).strftime("%Y-%m-%d")
            for _ in tqdm(range(self.num_customers), desc="Registration Dates")
        ]

        self.customers_df = pd.DataFrame(
            {
                "customer_id": customer_ids,
                "name": names,
                "email": emails,
                "age": ages,
                "city": cities,
                "country": countries,
                "registration_date": registration_dates,
            }
        )

        self.logger.info(
            f"Generated {len(self.customers_df):,} customers successfully"
        )
        return self.customers_df

    def generate_products(self) -> pd.DataFrame:
        """Generate product data with realistic attributes.

        Creates product records with:
        - product_id: Unique identifier (PROD00001, PROD00002, ...)
        - name: Product name from Faker
        - category: One of {Electronics, Clothing, Home, Sports, Books}
        - price: Category-specific range, rounded to 2 decimals
        - stock: Random stock quantity (10-10000)
        - rating: Uniform distribution (1.0-5.0, rounded to 1 decimal)

        Returns:
            pd.DataFrame: Product data with 6 columns.
        """
        self.logger.info(
            f"Generating {self.num_products:,} product records..."
        )

        product_ids = [
            f"PROD{i:05d}" for i in range(1, self.num_products + 1)
        ]

        names = [
            f"{self.faker.word().title()} {self.faker.word().title()}"
            for _ in tqdm(range(self.num_products), desc="Product Names")
        ]

        categories = np.random.choice(
            self.PRODUCT_CATEGORIES, size=self.num_products
        )

        # Price: Category-specific ranges
        prices = [
            round(
                np.random.uniform(
                    self.PRICE_RANGES[category][0],
                    self.PRICE_RANGES[category][1],
                ),
                2,
            )
            for category in tqdm(categories, desc="Prices")
        ]

        # Stock: Uniform distribution
        stock = np.random.randint(10, 10001, size=self.num_products)

        # Rating: Uniform distribution [1.0, 5.0], rounded to 1 decimal
        rating = np.round(
            np.random.uniform(1.0, 5.0, size=self.num_products), 1
        )

        self.products_df = pd.DataFrame(
            {
                "product_id": product_ids,
                "name": names,
                "category": categories,
                "price": prices,
                "stock": stock,
                "rating": rating,
            }
        )

        self.logger.info(
            f"Generated {len(self.products_df):,} products successfully"
        )
        return self.products_df

    def generate_orders(self) -> pd.DataFrame:
        """Generate order data with Pareto distribution (80/20 rule).

        Creates order records with:
        - order_id: Unique identifier (ORD0000001, ORD0000002, ...)
        - customer_id: Random from generated customers (Pareto distribution)
        - product_id: Random from generated products
        - quantity: Uniform distribution (1-10)
        - order_date: Random date within last 2 years
        - unit_price: Price from corresponding product

        The Pareto distribution ensures 20% of customers make ~80% of orders,
        reflecting realistic e-commerce behavior.

        Returns:
            pd.DataFrame: Order data with 6 columns.
        """
        if self.customers_df is None or self.products_df is None:
            raise ValueError(
                "Must generate customers and products before orders"
            )

        self.logger.info(
            f"Generating {self.num_orders:,} order records with Pareto distribution..."
        )

        order_ids = [
            f"ORD{i:07d}" for i in range(1, self.num_orders + 1)
        ]

        # Pareto distribution for customer selection (80/20 rule)
        # 20% of customers will make ~80% of orders
        pareto_values = np.random.pareto(
            a=1.16, size=self.num_orders
        )  # a=1.16 gives approximately 80/20
        pareto_normalized = pareto_values / pareto_values.max()
        customer_indices = (
            (pareto_normalized * (self.num_customers - 1)).astype(int)
        )
        customer_ids = self.customers_df["customer_id"].iloc[customer_indices].values

        # Random product selection (uniform)
        product_indices = np.random.randint(0, self.num_products, size=self.num_orders)
        product_ids = self.products_df["product_id"].iloc[product_indices].values
        product_prices = self.products_df["price"].iloc[product_indices].values

        # Quantity: Uniform distribution [1, 10]
        quantities = np.random.randint(1, 11, size=self.num_orders)

        # Order dates: Random within last 2 years
        base_date = datetime.now() - timedelta(days=730)
        order_dates = [
            (base_date + timedelta(days=int(np.random.uniform(0, 730)))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            for _ in tqdm(range(self.num_orders), desc="Order Dates")
        ]

        self.orders_df = pd.DataFrame(
            {
                "order_id": order_ids,
                "customer_id": customer_ids,
                "product_id": product_ids,
                "quantity": quantities,
                "order_date": order_dates,
                "unit_price": product_prices,
            }
        )

        self.logger.info(
            f"Generated {len(self.orders_df):,} orders successfully"
        )
        return self.orders_df

    def generate_all(
        self,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Generate all datasets (customers, products, and orders).

        This is the main entry point for generating the complete dataset.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple of
                (customers_df, products_df, orders_df).
        """
        self.logger.info(
            "Starting complete synthetic data generation process..."
        )
        try:
            customers = self.generate_customers()
            products = self.generate_products()
            orders = self.generate_orders()

            self.logger.info(
                "Data generation completed successfully! "
                f"Customers: {len(customers):,}, "
                f"Products: {len(products):,}, "
                f"Orders: {len(orders):,}"
            )

            return customers, products, orders

        except Exception as e:
            self.logger.error(
                f"Data generation failed: {e}", exc_info=True
            )
            raise

    def save_to_csv(self, output_dir: str = "data/raw") -> None:
        """Save generated dataframes to CSV files.

        Args:
            output_dir: Directory to save CSV files (default: "data/raw").

        Raises:
            ValueError: If dataframes haven't been generated yet.
        """
        if (
            self.customers_df is None
            or self.products_df is None
            or self.orders_df is None
        ):
            raise ValueError(
                "Must call generate_all() before saving to CSV"
            )

        self.logger.info(f"Saving data to {output_dir}/...")

        try:
            self.customers_df.to_csv(
                f"{output_dir}/customers.csv", index=False
            )
            self.products_df.to_csv(
                f"{output_dir}/products.csv", index=False
            )
            self.orders_df.to_csv(
                f"{output_dir}/orders.csv", index=False
            )

            self.logger.info(
                f"Successfully saved all data to {output_dir}/"
            )
        except IOError as e:
            self.logger.error(f"Failed to save data to CSV: {e}")
            raise

    def get_statistics(self) -> dict:
        """Generate summary statistics for the generated data.

        Returns:
            dict: Statistics including customer age distribution,
                product price distribution, order metrics.
        """
        if (
            self.customers_df is None
            or self.products_df is None
            or self.orders_df is None
        ):
            raise ValueError(
                "Must call generate_all() before getting statistics"
            )

        stats = {
            "customers": {
                "count": len(self.customers_df),
                "avg_age": self.customers_df["age"].mean(),
                "age_std": self.customers_df["age"].std(),
            },
            "products": {
                "count": len(self.products_df),
                "avg_price": self.products_df["price"].mean(),
                "avg_rating": self.products_df["rating"].mean(),
            },
            "orders": {
                "count": len(self.orders_df),
                "avg_quantity": self.orders_df["quantity"].mean(),
                "unique_customers": self.orders_df["customer_id"].nunique(),
                "orders_per_customer": len(self.orders_df)
                / self.orders_df["customer_id"].nunique(),
            },
        }

        return stats

    def print_statistics(self) -> None:
        """Print summary statistics for generated data."""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("SYNTHETIC DATA GENERATION SUMMARY")
        print("=" * 70)

        print("\nCUSTOMERS:")
        print(f"  Total Count: {stats['customers']['count']:,}")
        print(f"  Average Age: {stats['customers']['avg_age']:.1f}")
        print(f"  Age Std Dev: {stats['customers']['age_std']:.1f}")

        print("\nPRODUCTS:")
        print(f"  Total Count: {stats['products']['count']:,}")
        print(f"  Average Price: ${stats['products']['avg_price']:.2f}")
        print(f"  Average Rating: {stats['products']['avg_rating']:.2f}/5.0")

        print("\nORDERS:")
        print(f"  Total Count: {stats['orders']['count']:,}")
        print(f"  Average Quantity: {stats['orders']['avg_quantity']:.2f}")
        print(f"  Unique Customers: {stats['orders']['unique_customers']:,}")
        print(
            f"  Orders per Customer: {stats['orders']['orders_per_customer']:.2f}"
        )

        print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    # Example usage
    generator = SyntheticDataGenerator(
        num_customers=100_000,
        num_products=10_000,
        num_orders=1_000_000,
        seed=42,
    )

    customers, products, orders = generator.generate_all()
    generator.print_statistics()
    generator.save_to_csv()
