# ANAC Open Data Extractor (Italy)

A Python-based utility designed for economists, data scientists, and policy researchers to efficiently extract and filter public procurement data from the **ANAC (Autorità Nazionale Anticorruzione)** Open Data portal.

## Overview
The Italian ANAC portal provides massive datasets from 2007 to 2026. However, these files are often several gigabytes in size, making them difficult to handle on standard hardware. 

This tool provides a **memory-efficient workflow** that:
- **Streams** data directly from ZIP archives (minimizing disk usage).
- **Filters** records on-the-fly (e.g., by year, procedure type, or amount).
- **Cleans** financial variables (converting Italian-format currency strings into floats).
- **Validates** schema headers automatically to handle year-over-year column changes.


## Installation
Ensure you have Python 3.8+ installed. Clone this repository and install the dependencies:
```bash
# Clone the repository
git clone https://github.com/giacomogazzellone/anac_opendata_extractor.git

# Enter the directory
cd anac_opendata_extractor

# Install dependencies
pip install -r requirements.txt
