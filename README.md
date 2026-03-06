# Yellow Taxi Insights 2025

## What this project does
- Reads NYC TLC Yellow Taxi trip data for the full year 2025 from Parquet files.
- Loads the dataset into Apache DataFusion.
- Computes aggregations using both the DataFrame API and DataFusion SQL.
- Prints the results in a readable table format in the terminal.

## Dataset source
NYC TLC Trip Record Data page:  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data Dictionary:  
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

## How to download data
Download the 12 monthly Yellow Taxi Parquet files for the year 2025 from the NYC TLC Trip Record Data page.

Place the files in this folder: 
D:/NYC

Example files:
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
yellow_tripdata_2025-04.parquet
yellow_tripdata_2025-05.parquet
yellow_tripdata_2025-06.parquet
yellow_tripdata_2025-07.parquet
yellow_tripdata_2025-08.parquet
yellow_tripdata_2025-09.parquet
yellow_tripdata_2025-10.parquet
yellow_tripdata_2025-11.parquet

## How to run the project
Open the project folder in terminal and run:
cargo run

## Aggregation 1: Trips and revenue by month
Groups trips by pickup month using the pickup datetime and calculates:
- Trip count
- Total revenue
- Average fare

## Aggregation 2: Tip behavior by payment type
Groups trips by payment type and calculates:
- Trip count
- Average tip amount
- Tip rate (sum of tips / sum of total amount)

## Screenshot
Terminal output screenshot:

![Terminal Output](screenshots/output.png) 