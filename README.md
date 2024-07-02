# Breach Parse: A Parsing Tool To Quickly Search Through Breach Data Written in Rust

Breach Pars is a powerful and efficient tool designed to search through large datasets of breach data. Leveraging parallel processing, it delivers lightning-fast search results for specified keywords or emails, even when dealing with billions of lines of compressed data.

## Features

- **Keyword Search**: Search for primary and secondary keywords within breach data.
- **Email Search**: Directly search for specific email addresses.
- **Parallel Processing**: Utilizes multi-threading for faster search results.
- **Flexible Output**: Results can be printed to the console or saved to a file.
- **Support for Compressed Files**: Handles `.gz` and `.zst` compressed files seamlessly.

## Performance

Breach-Parse is designed to handle massive datasets efficiently. For example, it can search through more than 3 billion lines of compressed data (approximately 19 GB) and deliver results in about 45 seconds. 

[![asciicast](https://asciinema.org/a/ROQDk5AAw62wynxCdgkQwTXjT.svg)](https://asciinema.org/a/ROQDk5AAw62wynxCdgkQwTXjT)

## Usage

### Command Line Arguments

- `-k, --keyword`: Primary keyword to search for (required unless `--email` is provided).
- `-s, --second_keyword`: Secondary keyword to search for (optional).
- `-o, --output_file`: File to output results or 'print' to output to console (default: 'print').
- `--email`: Email to search for directly (optional).

### Examples

#### Keyword Search

```sh
./breach-parse -k "password" -s "123456" -o "results.txt"
```

This command searches for the keywords "password" and "123456" in the breach data and writes the results to `results.txt`.

#### Email Search

```sh
./breach-parse --email "example@example.com"
```

OR

```sh
./breach-parse "example@example.com"
```

This command searches for the email "example@example.com" in the breach data and prints the results to the console.

#### Print Results to Console

```sh
./breach-parse -k "firstname" -s "lastname"
```

This command searches for the keywords "password" and "123456" and prints the results to the console.

## Installation

To install Breach Parser, clone the repository and build the tool using Cargo:

```sh
git clone https://github.com/44za12/breach-parse-rs.git
cd breach-parse-rs
cargo build --release
```

## Downloading Breach Data

Download the breach data using the provided torrent file in the repository:

1. Download a torrent client if you don't already have one (e.g., qBittorrent, Transmission).
2. Open the torrent client and add the torrent file `data.tmp.torrent` located in the root of the cloned repository.
3. Once the download is complete, place the downloaded data in the cloned directory.

Once the data is downloaded, you can use the tool without specifying the data location.

## How It Works

### Email Processing

The `process_email` function builds a search path based on the email address and decompresses the relevant file to search for matches.

### File Processing

The `process_file` function reads each file, decompresses if necessary, and uses the `AhoCorasick` library to find matches for the specified keywords.

### Main Function

The main function sets up the search, initializes a progress bar using the `indicatif` crate, and performs the search in parallel using the `rayon` crate. Results are either printed to the console or written to a specified output file.

## Contributing

We welcome contributions! Please fork the repository and submit a pull request for any enhancements or bug fixes.
