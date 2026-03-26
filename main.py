"""
Main entry point for synthetic e-commerce data generation and processing.

This script orchestrates the complete data generation pipeline:
1. Generates synthetic customers, products, and orders
2. Saves them as Parquet files for efficient storage
3. Reports generation time and file sizes
4. Includes comprehensive error handling
"""

import logging
import os
import time
from pathlib import Path
from typing import Optional, Tuple

from src.config import LOG_FORMAT, DATE_FORMAT, LOG_LEVEL, RAW_DATA_DIR
from src.data_generator import SyntheticDataGenerator


def setup_logging() -> logging.Logger:
    """Configure logging for the main script.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)

    handler = logging.StreamHandler()
    handler.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


def format_file_size(size_bytes: int) -> str:
    """Convert bytes to human-readable format.

    Args:
        size_bytes: Size in bytes.

    Returns:
        str: Human-readable size string.
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0

    return f"{size_bytes:.2f} PB"


def get_file_size(file_path: str) -> int:
    """Get size of a file in bytes.

    Args:
        file_path: Path to the file.

    Returns:
        int: File size in bytes, or 0 if file doesn't exist.
    """
    try:
        if os.path.isfile(file_path):
            return os.path.getsize(file_path)
        elif os.path.isdir(file_path):
            # For directories (Parquet files are directories), sum all files
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(file_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    total_size += os.path.getsize(filepath)
            return total_size
    except OSError:
        pass
    return 0


def save_as_parquet(
    df,
    output_path: str,
    logger: logging.Logger,
    compression: str = "snappy",
) -> bool:
    """Save pandas DataFrame as Parquet file.

    Args:
        df: Pandas DataFrame to save.
        output_path: Output file path (without .parquet extension).
        logger: Logger instance.
        compression: Compression codec (snappy, gzip, brotli, lz4, zstd).

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        logger.info(f"Saving {len(df):,} records to {output_path}...")
        df.to_parquet(
            output_path,
            compression=compression,
            index=False,
            engine="pyarrow",
        )
        logger.info(f"Successfully saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save to {output_path}: {e}", exc_info=True)
        return False


def print_header(title: str) -> None:
    """Print formatted section header.

    Args:
        title: Section title to display.
    """
    width = 80
    print("\n" + "=" * width)
    print(f"{title.center(width)}")
    print("=" * width)


def print_results(
    generation_time: float,
    file_sizes: dict[str, Tuple[str, int]],
) -> None:
    """Print formatted results summary.

    Args:
        generation_time: Total generation time in seconds.
        file_sizes: Dictionary mapping file names to (path, size_bytes).
    """
    print_header("GENERATION RESULTS")

    print("\nTIMING:")
    print(f"  Total Generation Time: {generation_time:.2f} seconds")
    print(
        f"  Generation Speed: {generation_time / 3600:.2f} hours "
        f"({3600 / generation_time:.0f} seconds per hour)"
    )

    print("\nFILE SIZES:")
    total_size = 0
    for name, (path, size) in file_sizes.items():
        formatted_size = format_file_size(size)
        print(f"  {name:20s}: {formatted_size:>15s}")
        total_size += size

    print(f"  {'Total':20s}: {format_file_size(total_size):>15s}")

    print("\nOUTPUT LOCATIONS:")
    for name, (path, _) in file_sizes.items():
        print(f"  {name:20s}: {path}")

    print("\n" + "=" * 80 + "\n")


def main(
    num_customers: int = 100_000,
    num_products: int = 10_000,
    num_orders: int = 1_000_000,
    seed: Optional[int] = 42,
) -> bool:
    """Main function to orchestrate data generation and saving.

    Args:
        num_customers: Number of customer records to generate.
        num_products: Number of product records to generate.
        num_orders: Number of order records to generate.
        seed: Random seed for reproducibility (default: 42).

    Returns:
        bool: True if successful, False otherwise.
    """
    logger = setup_logging()

    print_header("SYNTHETIC E-COMMERCE DATA GENERATION")
    print(
        f"\nConfiguration:\n"
        f"  Customers: {num_customers:,}\n"
        f"  Products:  {num_products:,}\n"
        f"  Orders:    {num_orders:,}\n"
        f"  Output:    {RAW_DATA_DIR}"
    )

    try:
        # Create output directory if it doesn't exist
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ready: {RAW_DATA_DIR}")

        # Initialize generator
        logger.info("Initializing SyntheticDataGenerator...")
        generator = SyntheticDataGenerator(
            num_customers=num_customers,
            num_products=num_products,
            num_orders=num_orders,
            seed=seed,
        )

        # Measure generation time
        start_time = time.time()

        # Generate all data
        logger.info("Generating synthetic data...")
        customers, products, orders = generator.generate_all()
        generation_time = time.time() - start_time

        # Print generation statistics
        generator.print_statistics()

        # Save as Parquet files
        logger.info("Saving data as Parquet files...")
        save_start = time.time()

        customers_path = str(RAW_DATA_DIR / "customers.parquet")
        products_path = str(RAW_DATA_DIR / "products.parquet")
        orders_path = str(RAW_DATA_DIR / "orders.parquet")

        success_customers = save_as_parquet(
            customers, customers_path, logger
        )
        success_products = save_as_parquet(products, products_path, logger)
        success_orders = save_as_parquet(orders, orders_path, logger)

        save_time = time.time() - save_start

        if not (success_customers and success_products and success_orders):
            logger.error("Failed to save one or more Parquet files")
            return False

        # Get file sizes
        customers_size = get_file_size(customers_path)
        products_size = get_file_size(products_path)
        orders_size = get_file_size(orders_path)

        file_sizes = {
            "Customers": (customers_path, customers_size),
            "Products": (products_path, products_size),
            "Orders": (orders_path, orders_size),
        }

        total_time = generation_time + save_time

        # Print results
        print_results(total_time, file_sizes)

        # Print timing breakdown
        print("TIMING BREAKDOWN:")
        print(f"  Data Generation:  {generation_time:7.2f}s "
              f"({generation_time/total_time*100:5.1f}%)")
        print(f"  Parquet Saving:   {save_time:7.2f}s "
              f"({save_time/total_time*100:5.1f}%)")
        print(f"  Total Time:       {total_time:7.2f}s")

        logger.info(
            "Data generation and saving completed successfully! "
            f"Total time: {total_time:.2f}s"
        )

        return True

    except KeyboardInterrupt:
        logger.warning("Data generation interrupted by user")
        return False

    except FileNotFoundError as e:
        logger.error(f"File or directory not found: {e}", exc_info=True)
        return False

    except PermissionError as e:
        logger.error(
            f"Permission denied when accessing file/directory: {e}",
            exc_info=True,
        )
        return False

    except MemoryError:
        logger.error(
            "Insufficient memory for data generation. "
            "Try reducing the number of records.",
            exc_info=True,
        )
        return False

    except ValueError as e:
        logger.error(f"Invalid parameter value: {e}", exc_info=True)
        return False

    except Exception as e:
        logger.error(
            f"Unexpected error during data generation: {e}", exc_info=True
        )
        return False


if __name__ == "__main__":
    # Run with default parameters
    success = main()

    # Exit with appropriate code
    exit(0 if success else 1)

    # Alternative: Run with custom parameters
    # success = main(
    #     num_customers=1_000_000,
    #     num_products=100_000,
    #     num_orders=10_000_000,
    #     seed=42
    # )
