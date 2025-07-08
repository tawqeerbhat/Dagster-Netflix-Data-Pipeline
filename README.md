# Dagster-Netflix-Data-Pipeline


This project is a data pipeline built using Dagster that processes and analyzes Netflix title data. It showcases how to orchestrate data ingestion, transformation, and visualization using modern data engineering tools.


📁 Project Structure

- dagster_311_project/
  - nyc_311/
    - nyc_311/
      - assets.py – Dagster asset definitions
      - definitions.py – Dagster Definitions object
    - nyc_311_tests/
      - test_assets.py – Unit tests
    - data/
      - raw/ – Raw downloaded CSV files
      - processed/ – Cleaned, processed CSV files
  - netflix.duckdb – DuckDB database storing tables
  - README.md – Project documentation
  - setup.py, pyproject.toml, setup.cfg – Project metadata files


## 📊 What the Pipeline Does

1. Downloads the Netflix dataset (TidyTuesday - April 2021).
2. Loads the CSV into a DuckDB table.
3. Previews the top 5 rows of the dataset.
4. Cleans missing data and stores a processed CSV.
5. Visualizes the distribution of Movies vs TV Shows as a bar chart shown directly in the Dagster UI.

## 🧠 Skills Demonstrated

- Data Orchestration using Dagster
- SQL & Python-based transformations with DuckDB and Pandas
- Asset materialization & metadata visualization
- Project structure and documentation for promotion-ready work

## 🛠 Tech Stack

- Python 3.12
- Dagster
- DuckDB
- Pandas
- Matplotlib

## 📁 Source

Dataset: [TidyTuesday Netflix Titles CSV](https://github.com/rfordatascience/tidytuesday/blob/master/data/2021/2021-04-20/netflix_titles.csv)

## 🚀 Running the Project

```bash
# Set up environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Launch Dagster UI
dagster dev
