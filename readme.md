# YouTube Data Pipeline: Python to Snowflake

## Overview

This project extracts video metadata from the YouTube Data API, processes the results using Python and Pandas, saves the data to a CSV file, and loads it into Snowflake for further analysis.

The project demonstrates a basic data engineering workflow involving API extraction, data handling, file storage, and cloud data warehouse loading.

## Architecture

YouTube Data API → Python Requests → Pandas DataFrame → CSV → Snowflake

## Technologies

- Python
- YouTube Data API v3
- Pandas
- Requests
- Snowflake
- Snowflake Python Connector
- Git and GitHub

## Features

- Retrieves a channel's uploads playlist.
- Extracts video IDs using API pagination.
- Fetches video metadata in batches.
- Captures titles, publication dates, durations, views, likes, and comment counts.
- Saves extracted data to CSV.
- Loads the dataset into a Snowflake table using `write_pandas()`.

## Prerequisites

- Python
- A YouTube Data API key
- A Snowflake account with appropriate database, schema, warehouse, and table permissions

## Setup

1. Clone this repository.
2. Create and activate a Python virtual environment.
3. Install the required dependencies using `pip install -r requirements.txt`.
4. Create a `.env` file using `.env.example` as a template.
5. Add your own YouTube API and Snowflake credentials to `.env`.
6. Run the pipeline using `python main.py`.

Never commit your actual `.env` file or credentials.


