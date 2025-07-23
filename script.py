import os
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import numpy as np
import pandas as pd
from dateutil.parser import parse

# Configuration
SOURCE_URI = os.getenv("SOURCE_URI")
SOURCE_DB = os.getenv("SOURCE_DB")
TARGET_COLLECTION = os.getenv("TARGET_COLLECTION")
OUTPUT_FILE = "payment_orders.csv"

# logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#constants
LAST_PROCESSED_TIMESTAMP = parse("2025-07-01T22:15:00.281+00:00")

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


def connect_to_mongodb():
    try:
        client = MongoClient(SOURCE_URI)
        source_db = client[SOURCE_DB]
        collection = source_db[TARGET_COLLECTION]
        logger.info("Successfully connected to MongoDB")
        return client, collection
    except PyMongoError as e:
        logger.error("Could not connect to mongoDB. Connection Failed!")
        raise
def fetch_all_documents(collection):
    try:
        query = {"createdAt": {"$lt": LAST_PROCESSED_TIMESTAMP}}
        documents = list(collection.find(query,FIELD_PROJECTION))
        logger.info(f"Fetched {len(documents)} documents from MongoDB")
        return documents
    except PyMongoError as e: 
        logger.error(f"Error fetching documents: {e}")
        raise
def fetch_incremental_documents(collection):
    try:
        query = {"createdAt": {"$gt": LAST_PROCESSED_TIMESTAMP}}
        documents = list(collection.find(query, FIELD_PROJECTION))
        logger.info(f"Fetched {len(documents)} documents incrementally from MongoDB")
        return documents
    except PyMongoError as e:
        logger.error(f"Error fetching incremental documents: {e}")
        raise

def tranform_to_dataframe(documents):
    logger.info("Transforming documents is starting")
    if documents is None or len(documents) == 0:
        logger.warning("No data to transform/process")
        return
    # first to dataframe
    df = pd.DataFrame(documents)
    # second do the data conversions
    df['amountInCents'] = np.floor(df['amountInCents']).fillna(pd.NA).astype('Int32')
    df['noOfItems'] = np.floor(df['noOfItems']).fillna(pd.NA).astype('Int32')

    # fill paymentLinkExpireAt and paymentLink with unknown if it is NaT
    df['paymentLinkExpireAt'] = df['paymentLinkExpireAt'].fillna("unknown")
    df['paymentLink'] = df['paymentLink'].fillna("unknown")

    # third rename columns to snake_case
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    return df

def save_to_csv(df, mode="w", header=True):
    try:
        df.to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)
        action = "exported" if mode == 'w' else "appended"
        logger.info(f"Data {action} successfully to {OUTPUT_FILE}")
    
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise

def full_load(collection):
    logger.info("Starting full load process")
    documents = fetch_all_documents(collection)
    
    if not documents:
        logger.info("No documents found for full load")
        return
    
    df = tranform_to_dataframe(documents)
    save_to_csv(df, mode='w', header=True)
    logger.info("Full data load completed successfully")
    
def incremental_load(collection):
    logger.info("Starting incremental load process")
    new_documents = fetch_incremental_documents(collection)

    if not new_documents:
        logger.info("No new documents found for incremental load")
        return
    
    df = tranform_to_dataframe(new_documents)
    save_to_csv(df, mode='a', header=False)
    logger.info("Incremental data load completed successfully")
    
def etl_process():
    logger.info("Starting ETL process")
    client, collection = connect_to_mongodb()
    try:
        # Perform full load
        full_load(collection)
        
        # Perform incremental load
        incremental_load(collection)
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
        
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")
    

if __name__ == "__main__":
    etl_process()
    logger.info("ETL process completed successfully")
    
    
    
    