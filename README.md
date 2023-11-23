# FAA Aircraft Scraper

This script allows you to scrape aircraft information from the FAA registry based on N-NUMBER data.

## Usage

1. The script assumes that input CSV file is inside data folder with a column named "N-NUMBER" containing the aircraft registration numbers.
2. Run the scraper script with the following command:
```python3 main.py```
3. The script will start processing records, and you will see progress information in the console. The extracted information will be saved to the same file.

### Convert csv to excel

For coverting csv to xlsx run: ```python3 convert.py```