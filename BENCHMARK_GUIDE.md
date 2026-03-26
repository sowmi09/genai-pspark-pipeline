# File Format Benchmark Guide

## Overview

The `benchmark_formats.py` script provides a comprehensive analysis of file formats for storing large datasets. It benchmarks 6 different formats with detailed hardware metrics including file size, I/O performance, memory usage, CPU time, and estimated energy consumption.

## Quick Start

### Install Dependencies

```bash
pip install -r requirements.txt
```

Additional packages needed:
```bash
pip install openpyxl fastparquet
```

### Run the Benchmark

```bash
python benchmark_formats.py
```

**Expected Runtime:** 3-5 minutes for 500,000 rows

## Features

✅ **Tested Formats:**
- CSV (baseline)
- XLSX (Excel)
- Parquet (PyArrow)
- Parquet (FastParquet)
- Feather (Arrow)
- ORC (Columnar)

✅ **Measured Metrics:**
- **File Size**: Bytes → MB
- **Write Time**: Wall-clock time to write file
- **Read Time**: Wall-clock time to read file
- **Peak Memory**: Maximum RAM used during I/O
- **CPU Time**: Actual CPU processing time
- **Energy**: Estimated consumption (CPU_time × 65W TDP / 3600)
- **Throughput**: MB/s for read and write operations

✅ **Analysis:**
- Compression ratios vs CSV baseline
- Speed comparisons
- Memory efficiency
- Energy efficiency
- Detailed recommendations

## Output

### Console Output

```
================================================================================
                    FILE FORMAT BENCHMARK RESULTS - 500,000 ROWS
================================================================================

Generated: 2024-03-26 10:15:23
Rows: 500,000 | DataFrame Memory: 45.78 MB
================================================================================

Format          Size (MB)    Write (s)    Read (s)     Memory (MB)  CPU (s)    Energy (Wh)  vs CSV
--------------------------------------------------------------------------------
Feather             35.89        0.452       0.378        125.34      0.830        0.0150    -82.4%
Parquet             42.15        0.623       0.445        142.56      1.068        0.0192    -75.8%
ORC                 38.92        0.715       0.512        156.78      1.227        0.0221    -78.2%
FastParquet         44.32        0.589       0.421        138.92      1.010        0.0182    -74.1%
XLSX               145.67       12.453      8.923        234.56      21.376        0.3853    +47.2%
CSV                198.45        8.234      5.678        189.23      13.912        0.2508        0%

================================================================================

DETAILED ANALYSIS & RECOMMENDATIONS
================================================================================

📊 FILE SIZE ANALYSIS
---
1. Feather         :      35.89 MB (Compression: 81.9%)
2. ORC             :      38.92 MB (Compression: 80.4%)
3. Parquet         :      42.15 MB (Compression: 78.7%)
4. FastParquet     :      44.32 MB (Compression: 77.6%)
5. XLSX            :     145.67 MB (Compression: 26.6%)
6. CSV             :     198.45 MB (Compression: 0.0%)

⚡ SPEED ANALYSIS (Total Read + Write Time)
---
1. Feather         :    0.830s (Throughput: 286.39 MB/s)
2. Parquet         :    1.068s (Throughput: 210.56 MB/s)
3. FastParquet     :    1.010s (Throughput: 224.89 MB/s)
4. ORC             :    1.227s (Throughput: 194.73 MB/s)
5. CSV             :   13.912s (Throughput: 28.52 MB/s)
6. XLSX            :   21.376s (Throughput: 18.79 MB/s)

💾 PEAK MEMORY USAGE
---
1. CSV             :     189.23 MB
2. FastParquet     :     138.92 MB
3. Parquet         :     142.56 MB
4. Feather         :     125.34 MB
5. XLSX            :     234.56 MB
6. ORC             :     156.78 MB

⚡ ESTIMATED ENERGY CONSUMPTION (65W CPU TDP)
---
1. Feather         :     0.0150 Wh (0.000015 kWh, Est. $0.000002)
2. FastParquet     :     0.0182 Wh (0.000018 kWh, Est. $0.000002)
3. Parquet         :     0.0192 Wh (0.000019 kWh, Est. $0.000002)
4. ORC             :     0.0221 Wh (0.000022 kWh, Est. $0.000003)
5. CSV             :     0.2508 Wh (0.000251 kWh, Est. $0.000030)
6. XLSX            :     0.3853 Wh (0.000385 kWh, Est. $0.000046)

🎯 RECOMMENDATIONS
---
🏆 Best Storage:    Feather         (35.89 MB)
🚀 Fastest I/O:     Feather         (0.830s)
💾 Lowest Memory:   Feather         (125.34 MB)
⚡ Most Efficient:  Feather         (0.0150 Wh)
```

### Generated Files

```
benchmark_output/
├── data.csv                      # CSV format (~198 MB)
├── data.xlsx                     # XLSX format (~146 MB)
├── data.parquet                  # Parquet/PyArrow (~42 MB)
├── data_fastparquet.parquet      # Parquet/FastParquet (~44 MB)
├── data.feather                  # Feather format (~36 MB)
├── data.orc                      # ORC format (~39 MB)
└── benchmark_results.csv         # Results summary
```

## Detailed Metrics Explanation

### File Size (MB)
- **What it measures**: Compressed file size on disk
- **Why it matters**: Storage costs, network transfer speed, backup time
- **Best format**: Feather, ORC, Parquet
- **Worst format**: CSV, XLSX

### Write Time (seconds)
- **What it measures**: Time to write DataFrame to file
- **Why it matters**: Data pipeline performance, ETL speed
- **Best format**: Feather, Parquet
- **Worst format**: XLSX

### Read Time (seconds)
- **What it measures**: Time to read file back into memory
- **Why it matters**: Query latency, interactive analysis speed
- **Best format**: Feather, Parquet
- **Worst format**: XLSX, CSV

### Peak Memory (MB)
- **What it measures**: Maximum RAM used during I/O operation
- **Why it matters**: System requirements, concurrent processing capability
- **Best format**: Feather, Parquet
- **Worst format**: XLSX

### CPU Time (seconds)
- **What it measures**: Actual CPU processing time (not wall-clock)
- **Why it matters**: Energy consumption, multi-threaded efficiency
- **Formula**: time.process_time() for pure CPU activity

### Energy Consumption (Wh)
- **What it measures**: Estimated energy used by CPU
- **Formula**: CPU_time × CPU_TDP / 3600
  - Default CPU TDP: 65W (typical modern CPU)
  - Example: 1 second CPU time × 65W ÷ 3600s/h = 0.0181 Wh
- **Why it matters**: Datacenter costs, environmental impact
- **Cost calculation**: Wh ÷ 1000 × $0.12/kWh

## Format Recommendations

### Use Parquet if:
- ✅ Need balance of speed and compression
- ✅ Wide ecosystem support (Spark, Hadoop, Arrow)
- ✅ Columnar access patterns
- ✅ Cross-platform compatibility
- ✅ Budget-conscious on storage

### Use Feather if:
- ✅ Maximum speed required
- ✅ In-memory analytics workflows
- ✅ Arrow-compatible ecosystems
- ✅ Minimal storage constraints
- ✅ Zero-copy data access needed

### Use ORC if:
- ✅ Hive/Spark ecosystem
- ✅ Excellent compression ratio
- ✅ Apache Hadoop compatibility
- ✅ Large-scale data warehousing

### Use CSV if:
- ✅ Human readability required
- ✅ Legacy system compatibility
- ✅ Simple data exchange format
- ✅ No performance requirements
- ⚠️ NOT recommended for large datasets

### Use XLSX if:
- ✅ Excel compatibility required
- ✅ Business user needs
- ✅ Small datasets (<100K rows)
- ⚠️ Avoid for large datasets (slow, inefficient)

## Performance Comparison Summary

| Metric | Best | 2nd Best | 3rd Best | Worst |
|--------|------|----------|----------|-------|
| File Size | Feather | ORC | Parquet | CSV |
| Write Speed | Feather | Parquet | FastParquet | XLSX |
| Read Speed | Feather | Parquet | FastParquet | XLSX |
| Memory | Feather | Parquet | FastParquet | XLSX |
| Energy | Feather | FastParquet | Parquet | XLSX |

## Customization

### Change Number of Rows

Edit the `main()` function:

```python
benchmark = FileFormatBenchmark(
    num_rows=1_000_000,  # Change this value
    output_dir="benchmark_output",
    seed=42
)
```

### Change CPU TDP

```python
benchmark.CPU_TDP_WATTS = 95.0  # Default: 65.0W
```

### Change Output Directory

```python
benchmark = FileFormatBenchmark(
    num_rows=500_000,
    output_dir="my_benchmark_results",  # Custom directory
    seed=42
)
```

## CSV Export

Results are automatically exported to `benchmark_output/benchmark_results.csv`:

```csv
Format,File Size (MB),Write Time (s),Read Time (s),Peak Memory (MB),CPU Time (s),Energy (Wh),Write Throughput (MB/s),Read Throughput (MB/s)
CSV,198.45,8.234,5.678,189.23,13.912,0.2508,24.11,34.92
Feather,35.89,0.452,0.378,125.34,0.830,0.0150,79.44,94.97
Parquet,42.15,0.623,0.445,142.56,1.068,0.0192,67.61,94.75
...
```

## Hardware Specifications

The script uses these constants:

- **CPU TDP (Thermal Design Power)**: 65W (default, configurable)
- **Assumed Electricity Cost**: $0.12 per kWh
- **Sample Size**: 500,000 rows
- **DataFrame Columns**: id, name, email, amount, date, category
- **DataFrame Memory**: ~45 MB in-memory

## Troubleshooting

### ImportError: No module named 'fastparquet'

```bash
pip install fastparquet
```

Or skip FastParquet benchmark (script handles gracefully).

### ImportError: No module named 'openpyxl'

```bash
pip install openpyxl
```

Required for XLSX format.

### ORC Format Fails on Windows

ORC support depends on PyArrow with Java backing. Try:

```bash
pip install --upgrade pyarrow
```

If still fails, ORC benchmark will be skipped with warning.

### Out of Memory Error

Reduce the number of rows:

```python
benchmark = FileFormatBenchmark(num_rows=100_000)  # Instead of 500,000
```

### XLSX Benchmark is Very Slow

This is normal! XLSX is designed for small datasets (< 100K rows). For 500K rows, expect 10-20+ seconds.

## Energy Calculation Details

The energy consumption estimate uses:

```
Energy (Wh) = CPU_time (seconds) × CPU_TDP (watts) / 3600 (seconds per hour)
```

Example:
- CPU Time: 0.83 seconds
- CPU TDP: 65 watts
- Energy: 0.83 × 65 / 3600 = 0.0150 Wh
- In kWh: 0.000015 kWh
- At $0.12/kWh: $0.0000018

**Note:** This is an estimate. Actual energy depends on:
- Actual CPU frequency during operation
- Power gating and idle states
- Memory subsystem power
- Disk I/O power (not included in calculation)

## Advanced Usage

### Run Specific Formats Only

```python
benchmark = FileFormatBenchmark(num_rows=500_000)
benchmark.create_dataframe()

# Run only specific benchmarks
benchmark.benchmark_csv()
benchmark.benchmark_parquet()
benchmark.benchmark_feather()

# Print and export
benchmark.print_results_table()
benchmark.export_results_csv()
```

### Custom DataFrame Schema

Modify `create_dataframe()` method to change columns:

```python
def create_dataframe(self) -> pd.DataFrame:
    data = {
        "user_id": range(1, self.num_rows + 1),
        "age": np.random.randint(18, 80, self.num_rows),
        "salary": np.random.uniform(30000, 200000, self.num_rows),
        "department": np.random.choice(
            ["Sales", "IT", "HR", "Finance"], self.num_rows
        ),
    }
    self.df = pd.DataFrame(data)
    return self.df
```

## Real-World Applications

### Data Warehouse Selection
Use Parquet or ORC for Spark/Hadoop environments

### Real-Time Analytics
Use Feather for in-memory operations

### Data Lakes
Use Parquet for best compression + speed balance

### Legacy Systems
CSV for backward compatibility (performance trade-off)

### Excel Integration
XLSX for small datasets with direct Excel import

## License

MIT License - See LICENSE file for details
