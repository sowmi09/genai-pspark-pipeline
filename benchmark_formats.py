"""
File format benchmarking script with hardware metrics.

This script benchmarks various file formats (CSV, XLSX, Parquet, ORC, Feather)
with detailed metrics including:
- File size in MB
- Write time in seconds
- Read time in seconds
- Peak memory usage (MB)
- CPU time (seconds)
- Estimated energy consumption (Wh)
- Percentage savings vs CSV baseline

Author: Data Pipeline Team
"""

import logging
import os
import time
import tracemalloc
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional
import warnings

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=DeprecationWarning)


class BenchmarkMetrics:
    """Store and calculate benchmark metrics for a file format."""

    def __init__(self, format_name: str) -> None:
        """Initialize metrics container.

        Args:
            format_name: Name of the file format (CSV, Parquet, etc.)
        """
        self.format_name = format_name
        self.file_size_mb: float = 0.0
        self.write_time_sec: float = 0.0
        self.read_time_sec: float = 0.0
        self.peak_memory_mb: float = 0.0
        self.cpu_time_sec: float = 0.0
        self.energy_consumption_wh: float = 0.0
        self.write_throughput_mbs: float = 0.0
        self.read_throughput_mbs: float = 0.0

    def calculate_energy(self, cpu_tdp_watts: float = 65.0) -> None:
        """Calculate estimated energy consumption.

        Uses formula: Energy (Wh) = CPU_time * TDP / 3600

        Args:
            cpu_tdp_watts: CPU Thermal Design Power in watts (default: 65W)
        """
        self.energy_consumption_wh = (self.cpu_time_sec * cpu_tdp_watts) / 3600

    def calculate_throughput(self) -> None:
        """Calculate read/write throughput in MB/s."""
        if self.write_time_sec > 0:
            self.write_throughput_mbs = self.file_size_mb / self.write_time_sec
        if self.read_time_sec > 0:
            self.read_throughput_mbs = self.file_size_mb / self.read_time_sec


class FileFormatBenchmark:
    """Comprehensive benchmarking tool for file formats."""

    # CPU Thermal Design Power (watts)
    CPU_TDP_WATTS = 65.0

    def __init__(
        self,
        num_rows: int = 500_000,
        output_dir: str = "benchmark_output",
        seed: int = 42,
    ) -> None:
        """Initialize the benchmark.

        Args:
            num_rows: Number of rows in the DataFrame.
            output_dir: Directory to save benchmark files.
            seed: Random seed for reproducibility.
        """
        self.num_rows = num_rows
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.seed = seed

        np.random.seed(seed)
        self.faker = Faker()
        Faker.seed(seed)

        self._setup_logging()
        self.logger.info(
            f"FileFormatBenchmark initialized: rows={num_rows:,}, "
            f"output_dir={output_dir}"
        )

        self.df: Optional[pd.DataFrame] = None
        self.metrics: Dict[str, BenchmarkMetrics] = {}

    def _setup_logging(self) -> None:
        """Configure logging for the benchmark."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def create_dataframe(self) -> pd.DataFrame:
        """Create a sample DataFrame with 500K rows.

        Columns: id, name, email, amount, date, category

        Returns:
            pd.DataFrame: Generated test data
        """
        self.logger.info(f"Creating DataFrame with {self.num_rows:,} rows...")

        data = {
            "id": range(1, self.num_rows + 1),
            "name": [
                self.faker.name()
                for _ in tqdm(range(self.num_rows), desc="Names", disable=True)
            ],
            "email": [
                self.faker.email()
                for _ in tqdm(
                    range(self.num_rows), desc="Emails", disable=True
                )
            ],
            "amount": np.random.uniform(10, 10000, self.num_rows),
            "date": pd.date_range("2023-01-01", periods=self.num_rows, freq="1min"),
            "category": np.random.choice(
                ["Electronics", "Clothing", "Home", "Sports", "Books"],
                self.num_rows,
            ),
        }

        self.df = pd.DataFrame(data)
        self.logger.info(
            f"DataFrame created: shape={self.df.shape}, "
            f"memory={self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        )

        return self.df

    def _get_file_size_mb(self, file_path: str) -> float:
        """Get file size in MB.

        Args:
            file_path: Path to the file.

        Returns:
            float: File size in MB.
        """
        if os.path.isdir(file_path):
            # For multi-part formats like ORC
            total_size = 0
            for root, dirs, files in os.walk(file_path):
                for file in files:
                    total_size += os.path.getsize(os.path.join(root, file))
            return total_size / (1024 * 1024)
        elif os.path.isfile(file_path):
            return os.path.getsize(file_path) / (1024 * 1024)
        return 0.0

    def _benchmark_format(
        self,
        format_name: str,
        write_func,
        read_func,
        file_path: str,
    ) -> BenchmarkMetrics:
        """Benchmark a single file format.

        Args:
            format_name: Name of the format.
            write_func: Function to write DataFrame.
            read_func: Function to read DataFrame.
            file_path: Path to save file.

        Returns:
            BenchmarkMetrics: Benchmark results.
        """
        self.logger.info(f"Benchmarking {format_name}...")

        metrics = BenchmarkMetrics(format_name)

        # Write benchmark
        tracemalloc.start()
        cpu_start = time.process_time()
        wall_start = time.perf_counter()

        try:
            write_func(self.df, file_path)
        except Exception as e:
            self.logger.error(f"Failed to write {format_name}: {e}")
            return metrics

        wall_write = time.perf_counter() - wall_start
        cpu_write = time.process_time() - cpu_start
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        metrics.write_time_sec = wall_write
        metrics.peak_memory_mb = peak / (1024 * 1024)
        metrics.file_size_mb = self._get_file_size_mb(file_path)

        # Read benchmark
        tracemalloc.start()
        cpu_start = time.process_time()
        wall_start = time.perf_counter()

        try:
            df_read = read_func(file_path)
        except Exception as e:
            self.logger.error(f"Failed to read {format_name}: {e}")
            tracemalloc.stop()
            return metrics

        wall_read = time.perf_counter() - wall_start
        cpu_read = time.process_time() - cpu_start
        current, peak_read = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        metrics.read_time_sec = wall_read
        metrics.peak_memory_mb = max(
            metrics.peak_memory_mb, peak_read / (1024 * 1024)
        )

        # Total CPU time (read + write)
        metrics.cpu_time_sec = cpu_write + cpu_read

        # Calculate energy consumption
        metrics.calculate_energy(self.CPU_TDP_WATTS)

        # Calculate throughput
        metrics.calculate_throughput()

        self.logger.info(
            f"{format_name}: size={metrics.file_size_mb:.2f}MB, "
            f"write={metrics.write_time_sec:.3f}s, "
            f"read={metrics.read_time_sec:.3f}s"
        )

        return metrics

    def benchmark_csv(self) -> None:
        """Benchmark CSV format."""

        def write_csv(df, path):
            df.to_csv(path, index=False)

        def read_csv(path):
            return pd.read_csv(path)

        file_path = str(self.output_dir / "data.csv")
        self.metrics["CSV"] = self._benchmark_format(
            "CSV", write_csv, read_csv, file_path
        )

    def benchmark_xlsx(self) -> None:
        """Benchmark XLSX format."""

        def write_xlsx(df, path):
            df.to_excel(path, index=False, engine="openpyxl")

        def read_xlsx(path):
            return pd.read_excel(path, engine="openpyxl")

        file_path = str(self.output_dir / "data.xlsx")
        self.metrics["XLSX"] = self._benchmark_format(
            "XLSX", write_xlsx, read_xlsx, file_path
        )

    def benchmark_parquet(self) -> None:
        """Benchmark Parquet format (PyArrow)."""

        def write_parquet(df, path):
            df.to_parquet(path, engine="pyarrow", compression="snappy")

        def read_parquet(path):
            return pd.read_parquet(path, engine="pyarrow")

        file_path = str(self.output_dir / "data.parquet")
        self.metrics["Parquet"] = self._benchmark_format(
            "Parquet", write_parquet, read_parquet, file_path
        )

    def benchmark_fastparquet(self) -> None:
        """Benchmark Parquet format (FastParquet)."""

        def write_fastparquet(df, path):
            df.to_parquet(path, engine="fastparquet", compression="snappy")

        def read_fastparquet(path):
            return pd.read_parquet(path, engine="fastparquet")

        file_path = str(self.output_dir / "data_fastparquet.parquet")
        self.metrics["FastParquet"] = self._benchmark_format(
            "FastParquet", write_fastparquet, read_fastparquet, file_path
        )

    def benchmark_feather(self) -> None:
        """Benchmark Feather format."""

        def write_feather(df, path):
            df.to_feather(path)

        def read_feather(path):
            return pd.read_feather(path)

        file_path = str(self.output_dir / "data.feather")
        self.metrics["Feather"] = self._benchmark_format(
            "Feather", write_feather, read_feather, file_path
        )

    def benchmark_orc(self) -> None:
        """Benchmark ORC format."""

        def write_orc(df, path):
            df.to_orc(path, engine="pyarrow")

        def read_orc(path):
            return pd.read_orc(path, engine="pyarrow")

        file_path = str(self.output_dir / "data.orc")
        self.metrics["ORC"] = self._benchmark_format(
            "ORC", write_orc, read_orc, file_path
        )

    def run_all_benchmarks(self) -> None:
        """Run all format benchmarks."""
        self.logger.info("Starting all benchmarks...")

        self.benchmark_csv()
        self.benchmark_xlsx()
        self.benchmark_parquet()
        self.benchmark_feather()
        self.benchmark_orc()

        try:
            self.benchmark_fastparquet()
        except Exception as e:
            self.logger.warning(f"FastParquet benchmark skipped: {e}")

        self.logger.info("All benchmarks completed!")

    def print_results_table(self) -> None:
        """Print a formatted comparison table."""
        if not self.metrics:
            self.logger.error("No metrics available. Run benchmarks first.")
            return

        # Get CSV baseline for comparison
        csv_metrics = self.metrics.get("CSV")
        if not csv_metrics:
            self.logger.error("CSV metrics not found for baseline comparison")
            return

        print("\n" + "=" * 150)
        print(
            "FILE FORMAT BENCHMARK RESULTS - 500,000 ROWS"
            .center(150)
        )
        print("=" * 150)
        print(
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(f"Rows: {self.num_rows:,} | DataFrame Memory: "
              f"{self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        print("=" * 150)

        # Header
        print(
            f"{'Format':<15} {'Size (MB)':<12} {'Write (s)':<12} {'Read (s)':<12} "
            f"{'Memory (MB)':<12} {'CPU (s)':<10} {'Energy (Wh)':<12} {'vs CSV':<10}"
        )
        print("-" * 150)

        # Data rows
        for format_name, metrics in sorted(
            self.metrics.items(),
            key=lambda x: x[1].file_size_mb,
        ):
            size_savings = (
                (1 - metrics.file_size_mb / csv_metrics.file_size_mb) * 100
            )
            time_savings = (
                (1 - (metrics.write_time_sec + metrics.read_time_sec) /
                 (csv_metrics.write_time_sec + csv_metrics.read_time_sec)) * 100
            )

            size_pct = (
                f"-{size_savings:.1f}%"
                if size_savings > 0
                else f"+{abs(size_savings):.1f}%"
            )
            time_pct = (
                f"-{time_savings:.1f}%"
                if time_savings > 0
                else f"+{abs(time_savings):.1f}%"
            )

            print(
                f"{format_name:<15} {metrics.file_size_mb:<12.2f} "
                f"{metrics.write_time_sec:<12.3f} {metrics.read_time_sec:<12.3f} "
                f"{metrics.peak_memory_mb:<12.2f} {metrics.cpu_time_sec:<10.3f} "
                f"{metrics.energy_consumption_wh:<12.4f} {size_pct:>9}"
            )

        print("=" * 150)

    def print_detailed_analysis(self) -> None:
        """Print detailed analysis and recommendations."""
        if not self.metrics:
            return

        print("\n" + "=" * 100)
        print("DETAILED ANALYSIS & RECOMMENDATIONS".center(100))
        print("=" * 100)

        # File size analysis
        print("\n📊 FILE SIZE ANALYSIS")
        print("-" * 100)
        sorted_by_size = sorted(
            self.metrics.items(), key=lambda x: x[1].file_size_mb
        )
        for i, (fmt, metrics) in enumerate(sorted_by_size, 1):
            print(
                f"{i}. {fmt:<15}: {metrics.file_size_mb:>10.2f} MB "
                f"(Compression: {(1 - metrics.file_size_mb / self.metrics['CSV'].file_size_mb) * 100:>6.1f}%)"
            )

        # Speed analysis (combined read + write)
        print("\n⚡ SPEED ANALYSIS (Total Read + Write Time)")
        print("-" * 100)
        sorted_by_speed = sorted(
            self.metrics.items(),
            key=lambda x: x[1].write_time_sec + x[1].read_time_sec,
        )
        for i, (fmt, metrics) in enumerate(sorted_by_speed, 1):
            total_time = metrics.write_time_sec + metrics.read_time_sec
            throughput = (metrics.file_size_mb * 2) / total_time  # Read + Write
            print(
                f"{i}. {fmt:<15}: {total_time:>7.3f}s "
                f"(Throughput: {throughput:>7.2f} MB/s)"
            )

        # Memory analysis
        print("\n💾 PEAK MEMORY USAGE")
        print("-" * 100)
        sorted_by_memory = sorted(
            self.metrics.items(), key=lambda x: x[1].peak_memory_mb
        )
        for i, (fmt, metrics) in enumerate(sorted_by_memory, 1):
            print(f"{i}. {fmt:<15}: {metrics.peak_memory_mb:>10.2f} MB")

        # Energy analysis
        print("\n⚡ ESTIMATED ENERGY CONSUMPTION (65W CPU TDP)")
        print("-" * 100)
        sorted_by_energy = sorted(
            self.metrics.items(), key=lambda x: x[1].energy_consumption_wh
        )
        for i, (fmt, metrics) in enumerate(sorted_by_energy, 1):
            kwh = metrics.energy_consumption_wh / 1000
            cost_usd = kwh * 0.12  # Assuming $0.12 per kWh
            print(
                f"{i}. {fmt:<15}: {metrics.energy_consumption_wh:>10.4f} Wh "
                f"({kwh:.6f} kWh, Est. ${cost_usd:.4f})"
            )

        # Recommendations
        print("\n🎯 RECOMMENDATIONS")
        print("-" * 100)

        best_size = min(
            self.metrics.items(), key=lambda x: x[1].file_size_mb
        )
        best_speed = min(
            self.metrics.items(),
            key=lambda x: x[1].write_time_sec + x[1].read_time_sec,
        )
        best_memory = min(
            self.metrics.items(), key=lambda x: x[1].peak_memory_mb
        )
        best_energy = min(
            self.metrics.items(), key=lambda x: x[1].energy_consumption_wh
        )

        print(
            f"🏆 Best Storage:    {best_size[0]:<15} "
            f"({best_size[1].file_size_mb:.2f} MB)"
        )
        print(
            f"🚀 Fastest I/O:     {best_speed[0]:<15} "
            f"({best_speed[1].write_time_sec + best_speed[1].read_time_sec:.3f}s)"
        )
        print(
            f"💾 Lowest Memory:   {best_memory[0]:<15} "
            f"({best_memory[1].peak_memory_mb:.2f} MB)"
        )
        print(
            f"⚡ Most Efficient:  {best_energy[0]:<15} "
            f"({best_energy[1].energy_consumption_wh:.4f} Wh)"
        )

        print("\n" + "=" * 100 + "\n")

    def export_results_csv(self, filename: str = "benchmark_results.csv") -> None:
        """Export results to CSV file.

        Args:
            filename: Output CSV filename.
        """
        results = []
        for fmt, metrics in self.metrics.items():
            results.append(
                {
                    "Format": fmt,
                    "File Size (MB)": round(metrics.file_size_mb, 2),
                    "Write Time (s)": round(metrics.write_time_sec, 4),
                    "Read Time (s)": round(metrics.read_time_sec, 4),
                    "Peak Memory (MB)": round(metrics.peak_memory_mb, 2),
                    "CPU Time (s)": round(metrics.cpu_time_sec, 4),
                    "Energy (Wh)": round(metrics.energy_consumption_wh, 6),
                    "Write Throughput (MB/s)": round(
                        metrics.write_throughput_mbs, 2
                    ),
                    "Read Throughput (MB/s)": round(
                        metrics.read_throughput_mbs, 2
                    ),
                }
            )

        results_df = pd.DataFrame(results)
        output_path = self.output_dir / filename
        results_df.to_csv(output_path, index=False)
        self.logger.info(f"Results exported to {output_path}")


def main() -> None:
    """Main function to run the benchmark."""
    print("\n" + "=" * 100)
    print("FILE FORMAT BENCHMARKING SUITE".center(100))
    print("=" * 100 + "\n")

    # Create benchmark instance
    benchmark = FileFormatBenchmark(
        num_rows=500_000, output_dir="benchmark_output", seed=42
    )

    # Create test data
    benchmark.create_dataframe()

    # Run benchmarks
    print("\nRunning benchmarks (this may take a few minutes)...\n")
    benchmark.run_all_benchmarks()

    # Print results
    benchmark.print_results_table()
    benchmark.print_detailed_analysis()

    # Export results
    benchmark.export_results_csv()

    print("\n✅ Benchmark completed!")
    print(f"Files saved to: {benchmark.output_dir}")
    print(f"Results exported to: {benchmark.output_dir / 'benchmark_results.csv'}")


if __name__ == "__main__":
    main()
