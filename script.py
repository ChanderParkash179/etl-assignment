import json
import os
import requests
import pandas as pd
from pymongo import MongoClient

# Constants
CSV_URL = 'https://data.cms.gov/sites/default/files/2024-10/b220a101-219f-47d5-acfe-1685596bc727/CostReport_2022_Final.csv'
CSV_FILENAME = './data/mongo/hospitals.csv'
DB_NAME = 'hospitals'
COLLECTION_NAME = 'hospital'
CONFIG_FILE = './config/db_config.json'

# Ensure the directory exists
def ensure_directory(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

# Step 1: Read MongoDB URI from config file
def read_mongo_uri(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config['mongo_uri']

# Step 2: Download CSV (force overwrite)
def download_csv(url, filename):
    ensure_directory(filename)  # Ensure the directory exists
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Removed existing file: {filename}")

    print(f"Downloading CSV from {url}...")
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"Download complete: {filename}")

# Step 3: Connect to MongoDB and prepare collection
def get_mongo_collection(uri):
    client = MongoClient(uri)
    db = client[DB_NAME]

    # Drop the collection if it exists
    if COLLECTION_NAME in db.list_collection_names():
        db[COLLECTION_NAME].drop()
        print(f"Dropped existing collection: {COLLECTION_NAME}")

    collection = db[COLLECTION_NAME]
    return collection

# Step 4: Load CSV and insert into MongoDB
def insert_csv_to_mongo(collection, filename):
    print(f"Reading CSV: {filename}")
    df = pd.read_csv(filename)

    if df.empty:
        print("CSV file is empty. No data to insert.")
        return

    data = df.to_dict(orient='records')
    print(f"Inserting {len(data)} records into MongoDB...")
    collection.insert_many(data)
    print("Data inserted successfully.")

# Main function
def main():
    mongo_uri = read_mongo_uri(CONFIG_FILE)
    download_csv(CSV_URL, CSV_FILENAME)
    collection = get_mongo_collection(mongo_uri)
    insert_csv_to_mongo(collection, CSV_FILENAME)

if __name__ == '__main__':
    main()