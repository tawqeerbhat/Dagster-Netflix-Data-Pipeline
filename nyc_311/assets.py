from dagster import asset, Nothing
import dagster as dg
import requests
import os
import duckdb
import pandas as pd

# Asset 1: Download the Netflix CSV file
@dg.asset
def netflix_file() -> None:
    """
    Downloads the raw Netflix titles CSV into data/raw/.
    """
    url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2021/2021-04-20/netflix_titles.csv"
    output_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "netflix_titles.csv")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    response = requests.get(url)
    with open(output_path, "wb") as f:
        f.write(response.content)

# Asset 2: Load the CSV into DuckDB
@asset(deps=["netflix_file"])
def netflix_table() -> None:
    """
    Loads the downloaded Netflix CSV into a DuckDB table.
    """
    raw_path = os.path.join("data", "raw", "netflix_titles.csv")
    conn = duckdb.connect(database="netflix.duckdb")

    conn.execute("DROP TABLE IF EXISTS netflix_titles")
    conn.execute(f"""
        CREATE TABLE netflix_titles AS
        SELECT * FROM read_csv_auto('{raw_path}')
    """)

    conn.close()

# Asset 3: Preview first 5 rows
@asset(deps=["netflix_table"])
def preview_netflix_table() -> None:
    """
    Read and preview first 5 rows from netflix.duckdb
    """
    con = duckdb.connect("netflix.duckdb")

    try:
        df = con.execute("SELECT * FROM netflix_titles LIMIT 5").fetch_df()
        print("Preview of data:\n", df)
    except Exception as e:
        print("Error reading from table:", e)
    finally:
        con.close()

# Asset 4: Clean the data and save processed version
@asset(deps=["netflix_table"])
def clean_netflix_data() -> Nothing:
    """
    Cleans Netflix dataset and writes:
    - Cleaned table to DuckDB as 'cleaned_netflix_titles'
    - Cleaned CSV file to data/processed/cleaned_netflix.csv
    """
    con = duckdb.connect("netflix.duckdb")

    try:
        # Load from DuckDB
        df = con.execute("SELECT * FROM netflix_titles").fetch_df()

        # Drop rows missing key fields
        df_cleaned = df.dropna(subset=["title", "type", "release_year"])

        # Save cleaned table to DuckDB
        con.execute("DROP TABLE IF EXISTS cleaned_netflix_titles")
        con.register("clean_df", df_cleaned)
        con.execute("CREATE TABLE cleaned_netflix_titles AS SELECT * FROM clean_df")

        # Save cleaned CSV to data/processed/
        processed_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "cleaned_netflix.csv")
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        df_cleaned.to_csv(processed_path, index=False)

        print(f"Cleaned data saved. Shape: {df_cleaned.shape}")

    except Exception as e:
        print("Error during cleaning:", e)

    finally:
        con.close()

    return None





from dagster import AssetObservation, MetadataValue
import matplotlib.pyplot as plt
import base64
from io import BytesIO

@asset(deps=["clean_netflix_data"])
def type_distribution_chart(context) -> None:
    """
    Creates a bar chart showing number of Movies vs TV Shows.
    """
    df = pd.read_csv("data/processed/cleaned_netflix.csv")

    type_counts = df["type"].value_counts()

    # Create bar chart
    fig, ax = plt.subplots()
    type_counts.plot(kind="bar", ax=ax)
    ax.set_title("Netflix Titles by Type")
    ax.set_ylabel("Count")
    ax.set_xlabel("Type")

    # Save to file
    output_path = os.path.join("data", "processed", "type_counts.png")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path)
    plt.close()

    # Save base64 for Dagster UI preview
    with open(output_path, "rb") as img_file:
        encoded = base64.b64encode(img_file.read()).decode("utf-8")

    markdown_preview = f"![Chart](data:image/png;base64,{encoded})"

    context.add_output_metadata({
        "type_counts": MetadataValue.md(markdown_preview),
        "image_path": output_path,
    })

