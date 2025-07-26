# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Breach Parse is a high-performance Rust tool for searching through large breach datasets. It uses parallel processing with Rayon to efficiently search through compressed files (`.gz` and `.zst` formats) and can handle billions of lines of data.

## Key Architecture

- **Command-line interface**: Uses `clap` for argument parsing with support for keyword search and email lookup
- **Parallel processing**: Uses `rayon` for multi-threaded file processing
- **Compressed file support**: Handles `.gz` (via `flate2`) and `.zst` (via `zstd`) compressed files
- **Email search optimization**: Uses directory structure based on email prefix for faster lookups
- **Progress tracking**: Uses `indicatif` for visual progress bars during processing
- **Pattern matching**: Uses `aho-corasick` for efficient multi-pattern string matching

## Directory Structure

```
src/
├── main.rs           # Main application logic
├── Config            # CLI configuration struct
├── parse_arguments   # CLI argument parsing
├── process_email     # Email-specific search with optimized path lookup
├── process_file      # Generic file processing with Aho-Corasick matching
└── main              # Entry point with orchestration logic
```

## Build Commands

```bash
# Development build
cargo build

# Production build (optimized)
cargo build --release

# Run with debug output
cargo run -- --help

# Run with specific arguments
cargo run -- -k "password" -s "123456" -o results.txt
```

## Usage Patterns

### Email Search (Optimized)
- Direct email lookup via optimized path: `./breach-parse --email user@example.com`
- Uses directory structure: `data.tmp/f/i/r/firstname@domain.com.gz`

### Keyword Search (Parallel)
- Multi-keyword search: `./breach-parse -k "keyword1" -s "keyword2" -o output.txt`
- Processes all files in parallel using Rayon thread pool

### Data Requirements
- Expects breach data in `data.tmp/` directory (configurable via `--breach_data_location`)
- Supports both `.gz` and `.zst` compressed formats
- Directory structure for email search: `data.tmp/{first_char}/{second_char}/{third_char}/`

## Testing

No dedicated test suite exists. Manual testing recommended:

```bash
# Test email search
cargo run -- --email "test@example.com"

# Test keyword search
cargo run -- -k "password" -s "admin"

# Test with custom data location
cargo run -- --breach_data_location /path/to/data -k "search_term"
```

## Dependencies

- `clap`: CLI argument parsing
- `rayon`: Parallel processing
- `indicatif`: Progress bars
- `aho-corasick`: Efficient pattern matching
- `flate2`: Gzip decompression
- `zstd`: Zstandard decompression
- `walkdir`: Directory traversal