import os
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import numpy as np
import pandas as pd
import datetime
from dateutil.parser import parse
import psycopg2

# Configuration for soucre1: for reading the data from MongoDB
SOURCE_URI = os.getenv("SOURCE_URI")
SOURCE_DB = os.getenv("SOURCE_DB")
TARGET_COLLECTION = os.getenv("TARGET_COLLECTION")
OUTPUT_FILE = "payment_orders.csv"

# configuration for source2: for writing the updates of the latest processed timestamp
SOURCE_URI2 = os.getenv("SOURCE_URI2")
SOURCE_DB2 = os.getenv("SOURCE_DB2")
TARGET_COLLECTION2 = os.getenv("TARGET_COLLECTION2")

# logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# redshift connection parameters
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")


# Field projection for MongoDB queries
FIELD_PROJECTION = {
    "createdAt": 1, 
    "trackingnumber": 1,
    "provider": 1,
    "itemsType": 1,
    "status": 1,
    "noOfItems": 1,
    "amountInCents": 1,
    "paymentLink": 1,
    "paymentLinkExpireAt": 1
}

# Column mapping for renaming
COLUMN_MAPPING = {
    'createdAt': 'created_at',
    'trackingnumber': 'tracking_number',
    'provider': 'provider',
    'itemsType': 'items_type',
    'status': 'status',
    'noOfItems': 'no_of_items',
    'amountInCents': 'amount_in_cents',
    'paymentLink': 'payment_link',
    'paymentLinkExpireAt': 'payment_link_expire_at'
}

conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    database=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)

cursor = conn.cursor()

def extract_mongodb_connections():
    try:
        client = MongoClient(SOURCE_URI)
        source_db = client[SOURCE_DB]
        collection = source_db[TARGET_COLLECTION]
        
        client2 = MongoClient(SOURCE_URI2)
        source_db2 = client2[SOURCE_DB2]
        collection2 = source_db2[TARGET_COLLECTION2]
        logger.info("Successfully connected to MongoDB")
        return client, collection, client2, collection2
    except PyMongoError as e:
        logger.error("Could not connect to mongoDB. Connection Failed!")
        raise

def extract_all_documents(collection):
    try:
        documents = list(collection.find({},FIELD_PROJECTION))
        logger.info(f"Fetched {len(documents)} documents from MongoDB")
        return documents
    except PyMongoError as e: 
        logger.error(f"Error fetching documents: {e}")
        raise

def extract_incremental_documents(collection, latest_time_processed):
    try:
        query = {"createdAt": {"$gt": latest_time_processed}}
        documents = list(collection.find(query, FIELD_PROJECTION))
        logger.info(f"Fetched {len(documents)} documents incrementally from MongoDB")
        return documents
    except PyMongoError as e:
        logger.error(f"Error fetching incremental documents: {e}")
        raise

def transform_documents_to_dataframe(documents):
    logger.info("Transforming documents is starting")
    if documents is None or len(documents) == 0:
        logger.warning("No data to transform/process")
        return
    
    # first to dataframe
    df = pd.DataFrame(documents)

    # # second do the data conversions
    df['_id'] = df['_id'].astype(str)
    
    df['amountInCents'] = np.floor(df['amountInCents']).fillna(0).astype('Int32')
    df['noOfItems'] = np.floor(df['noOfItems']).fillna(0).astype('Int32')
    
    
    if 'paymentLink' in df.columns:
        df['paymentLink'] = df['paymentLink'].fillna("unknown")
    
    # third deal with datetime fields
    transform_datetime_column(df, 'createdAt')
    transform_datetime_column(df, 'paymentLinkExpireAt')
    # # third rename columns to snake_case
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    return df

def transform_datetime_column(df, col):
    if col in df.columns:
        df[col] = df[col].astype(object).where(pd.notnull(df[col]), None)

def load_dataframe_to_csv(df, mode="w", header=True):
    try:
        df.to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)
        action = "exported" if mode == 'w' else "appended"
        logger.info(f"Data {action} successfully to {OUTPUT_FILE}")
    
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise
    
def load_dataframe_to_redshift(df):
    
    #df_clean = clean_dataframe_for_db(df)
    try:
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO interns.payment_orders (
                    _id, provider, items_type, amount_in_cents, status,
                    created_at, payment_link, payment_link_expire_at, no_of_items
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['_id'], row['provider'], row['items_type'], row['amount_in_cents'],
                    row['status'], row['created_at'], row['payment_link'],
                    row['payment_link_expire_at'], row['no_of_items']
                )
            )
        conn.commit()
        logger.info("Data saved to Redshift successfully")
    except Exception as e:
        logger.error(f"Error saving to Redshift: {e}")
        raise

def extract_last_processed_timestamp():
    with open("last_processed_timestamp.txt", "r") as f:
        return parse(f.read())

def load_last_processed_timestamp(latest_time_processed):
    with open("last_processed_timestamp.txt", "w") as f:
        f.write(latest_time_processed.isoformat())

def extract_timestamp_from_mongodb(collection2):
    try:
        doc = collection2.find_one({"_id": "latest_timestamp"})
        if doc and "timestamp" in doc:
            return doc["timestamp"]
        else:
            logger.warning("No timestamp found in MongoDB, returning default value")
            return datetime.datetime.min
    except PyMongoError as e:
        logger.error(f"Error fetching timestamp from MongoDB: {e}")
        raise

def load_timestamp_to_mongodb(latest_time_processed, collection2):
    try:
        collection2.update_one(
            {"_id": "latest_timestamp"},
            {"$set": {"timestamp": latest_time_processed}},
            upsert=True
        )
        logger.info("Updated latest processed timestamp in MongoDB")
    except PyMongoError as e:
        logger.error(f"Error updating timestamp in MongoDB: {e}")
        raise

def etl_process(collection, collection2):
    logger.info("Starting ETL process")
    #latest_time_processed = extract_last_processed_timestamp()
    latest_time_processed = extract_timestamp_from_mongodb(collection2)
    #print(f"Last processed timestamp: {latest_time_processed}")
    new_documents = extract_incremental_documents(collection, latest_time_processed)

    if not new_documents:
        logger.info("No new documents found for incremental load")
        return
    
    df = transform_documents_to_dataframe(new_documents)
    #file_exists = os.path.isfile(OUTPUT_FILE)
    #load_dataframe_to_csv(df, mode='a', header=not file_exists)
    #print(df.info())
    
    load_dataframe_to_redshift(df)
    if df is not None and not df.empty:
        latest_time_processed = df['created_at'].max()
        #load_last_processed_timestamp(latest_time_processed)
        load_timestamp_to_mongodb(latest_time_processed, collection2)
        logger.info(f"Updated last processed timestamp to {latest_time_processed}")
        
    logger.info("ETL process completed successfully")
    
def main():
    logger.info("Starting ETL process")
    client, collection, client2, collection2 = extract_mongodb_connections()
    
    try:
        # Perform full load
        #full_load(collection)
        
        # Perform incremental load
        etl_process(collection, collection2)
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
        
    finally:
        if client and client2:
            client.close()
            client2.close()
            logger.info("MongoDB connections closed")

if __name__ == "__main__":
    main()
    logger.info("ETL process completed successfully")
    
    
    
    