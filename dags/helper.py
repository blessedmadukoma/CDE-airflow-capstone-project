import csv
import gzip
import os

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

# import psycopg2


def download_data(URL: str) -> str:
    """download the data from the source

    :params: 
        URL: str
    :returns: 
        download_path: str

    >>>> Example: download_data(WIKI_URL)
    Data Downloaded Successfully!
    'data/pageviews.gz'

    """

    print("Downloading the data from the source!!!")

    # download the data from the source
    response = requests.get(URL)

    # if response is successful, write to file
    if response.status_code != 200:
        raise Exception('Failed to download data')

    download_path = 'data/pageviews.gz'
    # download_path = os.path.join(
    #     os.getenv("AIRFLOW_HOME", "/opt/airflow"), "data/pageviews.gz")

    # Create the directory if it does not exist
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    with open(download_path, 'wb') as f:
        f.write(response.content)

    print('Data Downloaded Successfully!')

    return download_path


def extract_data(file_path: str):
    """extract the data from the gzipped file

    :params: 
        file_path: str - the path to the gzipped file

    :returns: 
        extracted_file: str - the path to the extracted file

    >>>> Example: extract_data('data/pageviews.gz')
    Extracted content to data/pageviews.txt
    'data/pageviews.txt'
    """

    extracted_file = 'data/pageviews.txt'

    # Ensure the data directory exists
    os.makedirs(os.path.dirname(extracted_file), exist_ok=True)

    # Check if the gzipped file exists before attempting to extract
    if os.path.exists(file_path):
        # Open the gzipped file and extract its contents
        with gzip.open(file_path, 'rb') as f_in:
            # Create the output file and write to it
            with open(extracted_file, 'wb') as f_out:
                f_out.write(f_in.read())
        print(f"Extracted content to {extracted_file}")
    else:
        print(f"The file {file_path} does not exist.")

    return extracted_file

# data processing: transform the data


def transform_data(companies: list, extracted_file: str) -> str:
    """transform the data - filter the pageviews based on the 5 companies

    :params:
        companies: list - the list of companies to filter for
        extracted_file: str - the path to the extracted file

    :returns:   
        filtered_file: str - the path to the filtered file

    >>>> Example: transform_data(['Google', 'Facebook', 'Amazon', 'Apple', 'Microsoft'], 'data/pageviews.txt')
    Data transformed!
    'data/filtered_pageviews.csv'
    """

    # using the unzipped file, find the pageviews that are related to the 5 companies
    filtered_data = []

    # Read through the extracted file and filter for the companies of interest
    with open(extracted_file, 'r') as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 3:
                page_title = parts[1].lower()
                for company in companies:
                    if company.lower() == page_title:
                        # Capture the page title and pageviews
                        filtered_data.append((page_title, parts[2]))

    # Store filtered data in a new file
    filtered_file = 'data/filtered_pageviews.csv'
    with open(filtered_file, 'w') as f_out:
        f_out.write('company,pageviews\n')
        for row in filtered_data:
            f_out.write(f"{row[0]},{row[1]}\n")

    print("Data transformed!")

    return filtered_file

# data storage


# def load_data(DATABASE_URL: str, filtered_file: str):
def load_data(filtered_file: str):
    """load the filtered data into the database

    :params:
        DATABASE_URL: str - the database connection string (if using psycopg2)
        filtered_file: str - the path to the filtered file

    :returns: None

    >>>> Example: load_data('postgresql://airflow:airflow@postgres:5432/cde_capstone', 'data/filtered_pageviews.csv')
    Data loaded successfully!
    """

    # print("Database connection: ", DATABASE_URL)

    # connect to the database
    # conn = psycopg2.connect(DATABASE_URL)

    postgres_conn_id = 'DATABASE_URL'

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    conn = pg_hook.get_conn()

    cursor = conn.cursor()

    # create table if it does not exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pageviews (
            company TEXT,
            pageviews INTEGER
        );
    """)

    # open the filtered file and load the data into the database
    with open(filtered_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            # print(f"Inserting row: {row[0]} => {row[1]}")
            cursor.execute(
                "INSERT INTO pageviews (company, pageviews) VALUES (%s, %s)", (row[0], row[1]))
            # print(f"Inserted!")

    # commit the transaction
    conn.commit()

    print("Data loaded successfully!")


# def analyze_data(DATABASE_URL: str):
def analyze_data():
    """analyze the data - find the company with the highest pageviews at that time
    :params: None
    :returns: None

    >>>> Example: analyze_data('postgresql://airflow:airflow@postgres:5432/cde_capstone')
    On October 10, 2024 at 4pm, the company with highest pageviews: Google with 1000 page views
    """
    # connect to the database

    postgres_conn_id = 'DATABASE_URL'

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    conn = pg_hook.get_conn()

    # using psycopg2
    # conn = psycopg2.connect(DATABASE_URL)

    cursor = conn.cursor()

    # query the database
    cursor.execute("""
        SELECT company, MAX(pageviews) FROM pageviews GROUP BY company ORDER BY MAX(pageviews) DESC LIMIT 1;
    """)

    # fetch the ONE record with the highest pageviews for that hour
    result = cursor.fetchone()

    # print the result
    print(
        f"On October 10, 2024 at 4pm, the company with highest pageviews: {result[0]} with {result[1]} page views")
