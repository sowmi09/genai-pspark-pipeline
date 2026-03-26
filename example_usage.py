"""
Example usage of the SyntheticDataGenerator class.

This script demonstrates how to generate large-scale synthetic e-commerce data
with realistic distributions and save it to CSV files.
"""

from src.data_generator import SyntheticDataGenerator


def main() -> None:
    """Generate synthetic e-commerce data and display statistics."""

    # Create generator with default large-scale parameters
    # (100K customers, 10K products, 1M orders)
    print("Initializing SyntheticDataGenerator...")
    generator = SyntheticDataGenerator(
        num_customers=100_000,
        num_products=10_000,
        num_orders=1_000_000,
        seed=42,  # For reproducibility
    )

    # Generate all data (returns pandas DataFrames)
    print("\nGenerating synthetic data...")
    customers_df, products_df, orders_df = generator.generate_all()

    # Display statistics
    generator.print_statistics()

    # Save to CSV files
    print("Saving data to CSV files...")
    generator.save_to_csv(output_dir="data/raw")

    # Access dataframes directly for further analysis
    print("\nFirst few customer records:")
    print(customers_df.head())

    print("\nFirst few product records:")
    print(products_df.head())

    print("\nFirst few order records:")
    print(orders_df.head())

    # Example: Analyze customer order distribution (Pareto principle)
    orders_by_customer = orders_df.groupby("customer_id").size()
    print("\n" + "=" * 70)
    print("PARETO ANALYSIS (20/80 Rule Verification)")
    print("=" * 70)
    total_orders = len(orders_df)
    top_20_percent_count = int(len(customers_df) * 0.2)
    top_20_percent_orders = orders_by_customer.nlargest(
        top_20_percent_count
    ).sum()
    percentage = (top_20_percent_orders / total_orders) * 100

    print(
        f"Top 20% of customers ({top_20_percent_count:,}) made: "
        f"{percentage:.1f}% of {total_orders:,} total orders"
    )
    print("=" * 70)


if __name__ == "__main__":
    main()
