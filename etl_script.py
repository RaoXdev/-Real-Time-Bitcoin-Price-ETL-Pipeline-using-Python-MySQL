import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime

# ------------------------------
# Setup Logging
# ------------------------------
logging.basicConfig(
    filename='etl.log', 
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# ------------------------------
# Database Config (MySQL)
# ------------------------------
DB_USER = 'root'           
DB_PASS = ''       
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'etl_db'
TABLE_NAME = 'bitcoin_prices'

DATABASE_URL = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URL)

# ------------------------------
# API Configuration
# ------------------------------
API_URL = 'https://api.api-ninjas.com/v1/bitcoin'
API_KEY = 'API-KEY-HIDDEN' #Hidden the API Key from public access  

# ------------------------------
# Step 1: Extract Data from API
# ------------------------------
def extract():
    try:
        logging.info(" Extracting data from Ninja API...")
        response = requests.get(API_URL, headers={'X-Api-Key': API_KEY})
        response.raise_for_status()
        data = response.json()
        logging.info(" Data extraction successful")
        return data
    except Exception as e:
        logging.error(f" Error during extraction: {e}")
        return None

# ------------------------------
# Step 2: Transform Data
# ------------------------------
def transform(data):
    try:
        price = float(data['price'])
        timestamp = pd.to_datetime(datetime.utcfromtimestamp(int(data['timestamp'])))
        price_change = float(data['24h_price_change'])
        price_change_percent = float(data['24h_price_change_percent'])
        high = float(data['24h_high'])
        low = float(data['24h_low'])
        volume = float(data['24h_volume'])

        df_cleaned = pd.DataFrame({
            'price_usd': [price],
            'timestamp': [timestamp],
            'price_change': [price_change],
            'price_change_percent': [price_change_percent],
            'high_24h': [high],
            'low_24h': [low],
            'volume_24h': [volume]
        })
        logging.info(" Data transformation successful")
        return df_cleaned
    except Exception as e:
        logging.error(f" Error during transformation: {e}")
        return None

# ------------------------------
# Step 3: Load into MySQL
# ------------------------------
def load(df):
    try:
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        logging.info(" Data loaded successfully into DB")
    except Exception as e:
        logging.error(f" Error during load: {e}")

# Run ETL
def run_etl():
    data = extract()
    if data:
        df = transform(data)
        if df is not None:
            load(df)

# Pipeline
if __name__ == "__main__":
    print("Running ETL pipeline...")
    run_etl()
