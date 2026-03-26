"""
Spark analytics module for analyzing e-commerce data.

This module provides the SparkAnalytics class for performing distributed
data analysis using Apache PySpark to generate business insights.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    desc,
    year,
    month,
)

from src.config import (
    CUSTOMERS_CSV,
    ORDERS_CSV,
    PRODUCTS_CSV,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_LOG_LEVEL,
    TOP_CUSTOMERS_OUTPUT,
    TOP_PRODUCTS_OUTPUT,
    REVENUE_BY_CATEGORY_OUTPUT,
    ANALYSIS_REPORT_OUTPUT,
    LOG_FORMAT,
    DATE_FORMAT,
    LOG_LEVEL,
)


class SparkAnalytics:
    """Perform analytics on e-commerce data using Apache Spark.

    This class provides methods to load, transform, and analyze
    customer, product, and order data to generate business insights.

    Attributes:
        spark (SparkSession): The Spark session for data processing.
    """

    def __init__(self) -> None:
        """Initialize the SparkAnalytics engine.

        Creates a Spark session configured for local execution
        with appropriate log levels.
        """
        self._setup_logging()
        self._init_spark_session()
        self.logger.info("SparkAnalytics initialized")

    def _setup_logging(self) -> None:
        """Configure logging for the analytics engine."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)

        handler = logging.StreamHandler()
        handler.setLevel(LOG_LEVEL)

        formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
        handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def _init_spark_session(self) -> None:
        """Initialize and configure the Spark session."""
        self.logger.info("Initializing Spark session...")

        self.spark = (
            SparkSession.builder.appName(SPARK_APP_NAME)
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
        self.logger.info(
            f"Spark session created: {SPARK_APP_NAME} @ {SPARK_MASTER}"
        )

    def load_data(self) -> None:
        """Load CSV data into Spark DataFrames.

        Reads customers, products, and orders CSV files and stores
        them as instance variables for analysis.
        """
        self.logger.info("Loading data from CSV files...")

        try:
            self.customers_df = self.spark.read.csv(
                str(CUSTOMERS_CSV), header=True, inferSchema=True
            )
            self.logger.info(
                f"Loaded {self.customers_df.count()} customer records"
            )

            self.products_df = self.spark.read.csv(
                str(PRODUCTS_CSV), header=True, inferSchema=True
            )
            self.logger.info(
                f"Loaded {self.products_df.count()} product records"
            )

            self.orders_df = self.spark.read.csv(
                str(ORDERS_CSV), header=True, inferSchema=True
            )
            self.logger.info(
                f"Loaded {self.orders_df.count()} order records"
            )

        except Exception as e:
            self.logger.error(f"Failed to load data: {e}", exc_info=True)
            raise

    def analyze_top_customers(
        self, limit: int = 20
    ) -> Optional[DataFrame]:
        """Identify top customers by total purchase value.

        Aggregates order data to find the highest-value customers,
        ranked by total spending.

        Args:
            limit: Number of top customers to return (default: 20).

        Returns:
            DataFrame with top customers and their metrics.
        """
        self.logger.info(f"Analyzing top {limit} customers...")

        try:
            top_customers = (
                self.orders_df.groupBy("customer_id")
                .agg(
                    count("order_id").alias("order_count"),
                    sum("total_amount").alias("total_spent"),
                    avg("total_amount").alias("avg_order_value"),
                )
                .join(self.customers_df, "customer_id", "left")
                .select(
                    "customer_id",
                    "name",
                    "country",
                    "order_count",
                    "total_spent",
                    "avg_order_value",
                )
                .orderBy(desc("total_spent"))
                .limit(limit)
            )

            top_customers.write.mode("overwrite").csv(
                str(TOP_CUSTOMERS_OUTPUT), header=True
            )
            self.logger.info(
                f"Top customers analysis saved to {TOP_CUSTOMERS_OUTPUT}"
            )

            return top_customers

        except Exception as e:
            self.logger.error(
                f"Failed to analyze top customers: {e}", exc_info=True
            )
            raise

    def analyze_top_products(
        self, limit: int = 20
    ) -> Optional[DataFrame]:
        """Identify top products by revenue and quantity sold.

        Aggregates order data to find the best-performing products
        ranked by total revenue.

        Args:
            limit: Number of top products to return (default: 20).

        Returns:
            DataFrame with top products and their metrics.
        """
        self.logger.info(f"Analyzing top {limit} products...")

        try:
            top_products = (
                self.orders_df.groupBy("product_id")
                .agg(
                    sum("quantity").alias("units_sold"),
                    sum("total_amount").alias("total_revenue"),
                    avg("unit_price").alias("avg_price"),
                    count("order_id").alias("order_count"),
                )
                .join(self.products_df, "product_id", "left")
                .select(
                    "product_id",
                    "product_name",
                    "category",
                    "units_sold",
                    "total_revenue",
                    "avg_price",
                    "order_count",
                )
                .orderBy(desc("total_revenue"))
                .limit(limit)
            )

            top_products.write.mode("overwrite").csv(
                str(TOP_PRODUCTS_OUTPUT), header=True
            )
            self.logger.info(
                f"Top products analysis saved to {TOP_PRODUCTS_OUTPUT}"
            )

            return top_products

        except Exception as e:
            self.logger.error(
                f"Failed to analyze top products: {e}", exc_info=True
            )
            raise

    def analyze_revenue_by_category(self) -> Optional[DataFrame]:
        """Analyze revenue and sales metrics by product category.

        Groups products and orders by category to identify
        category performance metrics.

        Returns:
            DataFrame with category-level metrics.
        """
        self.logger.info("Analyzing revenue by category...")

        try:
            revenue_by_category = (
                self.orders_df.join(self.products_df, "product_id", "left")
                .groupBy("category")
                .agg(
                    sum("quantity").alias("units_sold"),
                    sum("total_amount").alias("total_revenue"),
                    count("order_id").alias("order_count"),
                    avg("total_amount").alias("avg_order_value"),
                )
                .orderBy(desc("total_revenue"))
            )

            revenue_by_category.write.mode("overwrite").csv(
                str(REVENUE_BY_CATEGORY_OUTPUT), header=True
            )
            self.logger.info(
                f"Revenue by category saved to {REVENUE_BY_CATEGORY_OUTPUT}"
            )

            return revenue_by_category

        except Exception as e:
            self.logger.error(
                f"Failed to analyze revenue by category: {e}", exc_info=True
            )
            raise

    def analyze_customer_segmentation(self) -> Optional[DataFrame]:
        """Segment customers based on purchase behavior.

        Creates customer segments based on order frequency,
        average order value, and total spending.

        Returns:
            DataFrame with customer segmentation analysis.
        """
        self.logger.info("Performing customer segmentation analysis...")

        try:
            customer_metrics = (
                self.orders_df.groupBy("customer_id")
                .agg(
                    count("order_id").alias("purchase_frequency"),
                    avg("total_amount").alias("avg_order_value"),
                    sum("total_amount").alias("lifetime_value"),
                )
                .join(self.customers_df, "customer_id", "left")
            )

            # Simple segmentation logic
            from pyspark.sql.functions import when, case

            segmented = customer_metrics.withColumn(
                "segment",
                case(
                    (col("lifetime_value") > 5000, "Premium"),
                    (col("lifetime_value") > 1000, "Gold"),
                    (col("lifetime_value") > 100, "Silver"),
                ).otherwise("Bronze"),
            )

            self.logger.info("Customer segmentation completed")
            return segmented

        except Exception as e:
            self.logger.error(
                f"Failed to perform customer segmentation: {e}",
                exc_info=True,
            )
            raise

    def generate_report(self) -> None:
        """Generate a comprehensive analysis report.

        Creates a text report summarizing all key metrics
        and insights from the analysis.
        """
        self.logger.info("Generating comprehensive analysis report...")

        try:
            report_lines = [
                "=" * 80,
                "E-COMMERCE DATA PIPELINE - ANALYSIS REPORT",
                "=" * 80,
                "",
            ]

            # Data Overview
            report_lines.extend([
                "DATA OVERVIEW",
                "-" * 80,
                f"Total Customers: {self.customers_df.count():,}",
                f"Total Products: {self.products_df.count():,}",
                f"Total Orders: {self.orders_df.count():,}",
                "",
            ])

            # Revenue Metrics
            total_revenue = self.orders_df.agg(
                sum("total_amount")
            ).collect()[0][0]
            report_lines.extend([
                "REVENUE METRICS",
                "-" * 80,
                f"Total Revenue: ${total_revenue:,.2f}",
                f"Average Order Value: ${self.orders_df.agg(avg('total_amount')).collect()[0][0]:,.2f}",
                "",
            ])

            # Top Categories
            top_categories = self.analyze_revenue_by_category()
            report_lines.extend([
                "TOP 5 PRODUCT CATEGORIES",
                "-" * 80,
            ])
            for row in top_categories.limit(5).collect():
                report_lines.append(
                    f"{row['category']}: ${row['total_revenue']:,.2f} "
                    f"({row['units_sold']} units)"
                )
            report_lines.append("")

            # Top Products
            top_products = self.analyze_top_products(limit=5)
            report_lines.extend([
                "TOP 5 PRODUCTS",
                "-" * 80,
            ])
            for row in top_products.collect():
                report_lines.append(
                    f"{row['product_name']}: ${row['total_revenue']:,.2f} "
                    f"({row['units_sold']} units sold)"
                )
            report_lines.append("")

            # Top Customers
            top_customers = self.analyze_top_customers(limit=5)
            report_lines.extend([
                "TOP 5 CUSTOMERS",
                "-" * 80,
            ])
            for row in top_customers.collect():
                report_lines.append(
                    f"{row['name']} ({row['customer_id']}): "
                    f"${row['total_spent']:,.2f} "
                    f"({row['order_count']} orders)"
                )
            report_lines.append("")

            report_lines.extend([
                "=" * 80,
                "Report generated successfully",
                "=" * 80,
            ])

            # Write report to file
            with open(ANALYSIS_REPORT_OUTPUT, "w", encoding="utf-8") as f:
                f.write("\n".join(report_lines))

            self.logger.info(
                f"Report saved to {ANALYSIS_REPORT_OUTPUT}"
            )

            # Also print to console
            for line in report_lines:
                print(line)

        except Exception as e:
            self.logger.error(
                f"Failed to generate report: {e}", exc_info=True
            )
            raise

    def stop(self) -> None:
        """Stop the Spark session and clean up resources."""
        self.logger.info("Stopping Spark session...")
        self.spark.stop()
        self.logger.info("Spark session stopped")


if __name__ == "__main__":
    analytics = SparkAnalytics()
    analytics.load_data()
    analytics.analyze_top_customers()
    analytics.analyze_top_products()
    analytics.analyze_revenue_by_category()
    analytics.generate_report()
    analytics.stop()
