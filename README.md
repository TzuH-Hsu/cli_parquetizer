# cli_parquetizer

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/TzuH-Hsu/cli_parquetizer/pyinstaller-build.yml)
[![CodeQL](https://github.com/TzuH-Hsu/cli_parquetizer/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/TzuH-Hsu/cli_parquetizer/actions/workflows/github-code-scanning/codeql)
[![Release](https://img.shields.io/github/v/release/TzuH-Hsu/cli_parquetizer)](https://github.com/TzuH-Hsu/cli_parquetizer/releases)
![GitHub Downloads (all assets, all releases)](https://img.shields.io/github/downloads/TzuH-Hsu/cli_parquetizer/total)
![GitHub top language](https://img.shields.io/github/languages/top/TzuH-Hsu/cli_parquetizer)
![GitHub License](https://img.shields.io/github/license/TzuH-Hsu/cli_parquetizer)

A CLI tool for converting various data formats to Parquet.

## Description

`Parquetizer` is a Python-based command-line utility that converts different data formats into the Parquet format.

#### The tool currently supports the following input formats:

-   CSV
-   LVM

#### The tool currently supports the following source types:

-   Local file
-   MinIO

## Usage

To get started, download the executable from the project's [releases page](https://github.com/TzuH-Hsu/cli_parquetizer/releases).

### Windows

-   Locate the downloaded `Parquetizer-windows.exe` file.
-   Double-click the file to run the program.

### Linux

Execute the following commands in the terminal:

```bash
chmod +x Parquetizer-linux      # Make the file executable
./Parquetizer                   # Run the program
```

### MacOS

-   Open Finder and go to the location where Parquetizer.app is saved.
-   Double-click on `Parquetizer-macos` to start the application.
