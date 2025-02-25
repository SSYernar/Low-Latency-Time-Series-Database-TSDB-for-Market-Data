# High-Performance Time Series Database for Market Data

This project is a custom time series database optimized for storing and analyzing financial market data. It provides efficient data storage, fast querying capabilities, and solid concurrency support.

## Features

- **Columnar Storage**: Data fields are stored in separate memory-mapped files for optimal access patterns
- **B+ Tree Indexing**: Efficient time-based queries using in-memory B+ Tree
- **Append-Only Design**: Optimized for high-throughput write operations
- **Lock-Free Operations**: Support for concurrent reads/writes without blocking
- **Memory-Efficiency**: Zero-copy data access via memory mapping
- **Batched Operations**: Support for batch insertion and queries for better performance

## Building the Project

To build the project, you need a C++20 compatible compiler.

```bash
make
```

## Usage

### Insert Data

```bash
./tsdb_cli insert AAPL 1625097600 148.56 1000000
```

### Query Data by Time Range

```bash
./tsdb_cli query AAPL 1625097600 1625184000
```

### Get Last N Ticks

```bash
./tsdb_cli last AAPL 10
```

### Run Benchmarks

```bash
./tsdb_cli benchmark AAPL 100000
```

### Import from CSV

```bash
./tsdb_cli import AAPL market_data.csv
```

## Performance

This implementation is optimized for:

- High-throughput append operations
- Low-latency range queries
- Efficient memory usage

## Technical Details

### Storage Structure

Data is stored in a directory structure:

```
tsdb_data/
  AAPL/
    timestamps.bin  # Memory-mapped file for timestamps
    prices.bin      # Memory-mapped file for prices
    volumes.bin     # Memory-mapped file for volumes
  MSFT/
    ...
```

### Optimizations

1. **Memory-Mapped Files**: Zero-copy data access using mmap for minimal overhead
2. **Chunked File Allocation**: Files are preallocated in chunks to minimize resizing costs
3. **B+ Tree Indexing**: Cache-optimized B+ Tree for faster range queries
4. **Lock-Free Design**: Using atomic operations and reader-writer locks for concurrency
5. **Background Processing**: Asynchronous write operations to improve throughput

## Project History

This project evolved from a basic columnar store to a full-featured TSDB with optimizations for market data:

1. **Phase 1**: Basic storage engine with append-only binary format
2. **Phase 2**: Query and performance optimizations with B+ Tree and memory mapping
3. **Phase 3**: Advanced features for concurrency and batch operations