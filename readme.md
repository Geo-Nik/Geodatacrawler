# geodatacrawler

## Overview

The `geodatacrawler` project is a data crawling application designed to collect information from the Global Disaster Alert and Coordination System (GDACS) website. It utilizes Selenium for web scraping, xml.etree.ElementTree for XML data parsing, and GeoPandas to handle GeoJSON data. The collected data is then pushed into a PostGIS database.

## Getting Started

### Prerequisites

- Python 3.7 or higher
- Web driver (e.g., ChromeDriver for Google Chrome)

### Installation

Install required packages:
```bash
pip install -r requirements.txt
```


### Configuration

#### Adjust the configuration in config.yaml:

- Database connection parameters
- URLs for GDACS GeoJSON and XML data
- Web driver options
- Other common parameters

Run the main.py script:

```bash
python main.py
```

This script fetches and pushes data into the specified PostGIS database. It is designed to run continuously, fetching new data every 24 hours.

### Dependencies
- Selenium: For web scraping.
- xml.etree.ElementTree: For parsing XML data.
- Geopandas: For handling GeoJSON data.
- SQLAlchemy: For interacting with the database.
- Requests: For fetching XML data.
- GeoJSON: For creating GeoJSON features.
- BeautifulSoup4: For HTML parsing in Selenium.

### How It Works
- Web Scraping: The script uses Selenium to navigate GDACS website and scrape GeoJSON data.
- XML Data Parsing: XML data is fetched from GDACS, and xml.etree.ElementTree is used for parsing.
- Database Interaction: GeoJSON data is converted to GeoPandas DataFrame and pushed to a PostGIS database using SQLAlchemy.

### Notes
- Ensure that the web driver executable is in the system PATH.
- The script runs continuously, fetching new data every 24 hours. Adjust the interval in main.py if needed.
- Secure sensitive information such as database credentials.