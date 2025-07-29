import os
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import numpy as np
import pandas as pd
import datetime
from dateutil.parser import parse
import psycopg2

PAYMENT_ORDERS_URI = os.getenv("SOURCE_URI")
PAYMENT_ORDERS_DATABASE = os.getenv("SOURCE_DB")
PAYMENT_ORDERS_COLLECTION = os.getenv("TARGET_COLLECTION")
CSV_OUTPUT_FILE = "payment_orders.csv"

TIMESTAMP_TRACKING_URI = os.getenv("SOURCE_URI2")
TIMESTAMP_TRACKING_DATABASE = os.getenv("SOURCE_DB2")
TIMESTAMP_TRACKING_COLLECTION = os.getenv("TARGET_COLLECTION2")

# logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# redshift connection parameters
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# Field projection for MongoDB queries
PAYMENT_ORDER_FIELDS_PROJECTION = {
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
MONGODB_TO_REDSHIFT_COLUMN_MAPPING = {
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

redshift_connection = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    database=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)

redshift_cursor = redshift_connection.cursor()
table_name = "interns.payment_orders"

def extract_mongodb_connections():
    try:
        # Connect to the payment_orders collection to extract data
        payment_orders_client = MongoClient(PAYMENT_ORDERS_URI)
        payment_orders_database = payment_orders_client[PAYMENT_ORDERS_DATABASE]
        payment_orders_collection = payment_orders_database[PAYMENT_ORDERS_COLLECTION]
        
        # connect to the timestamp collection to store the latest processed timestamp
        timestamp_tracking_client = MongoClient(TIMESTAMP_TRACKING_URI)
        timestamp_tracking_database = timestamp_tracking_client[TIMESTAMP_TRACKING_DATABASE]
        timestamp_tracking_collection = timestamp_tracking_database[TIMESTAMP_TRACKING_COLLECTION]
        logger.info("Successfully connected to MongoDB")
        
        # return the clients and collections
        return payment_orders_client, payment_orders_collection, timestamp_tracking_client, timestamp_tracking_collection
    except PyMongoError as mongodb_error:
        logger.error("Could not connect to mongoDB. Connection Failed!")
        raise

def extract_all_documents(payment_orders_collection):
    try:
        all_payment_documents = list(payment_orders_collection.find({}, PAYMENT_ORDER_FIELDS_PROJECTION))
        logger.info(f"Fetched {len(all_payment_documents)} documents from MongoDB")
        return all_payment_documents
    except PyMongoError as mongodb_error: 
        logger.error(f"Error fetching documents: {mongodb_error}")
        raise

def extract_incremental_documents(payment_orders_collection, last_processed_timestamp):
    try:
        incremental_query = {"createdAt": {"$gt": last_processed_timestamp}}
        new_payment_documents = list(payment_orders_collection.find(incremental_query, PAYMENT_ORDER_FIELDS_PROJECTION))
        logger.info(f"Fetched {len(new_payment_documents)} documents incrementally from MongoDB")
        return new_payment_documents
    except PyMongoError as mongodb_error:
        logger.error(f"Error fetching incremental documents: {mongodb_error}")
        raise

def transform_documents_to_dataframe(payment_order_documents):
    logger.info("Transforming documents is starting")
    if payment_order_documents is None or len(payment_order_documents) == 0:
        logger.warning("No data to transform/process")
        return
    
    # first to dataframe
    payment_orders_dataframe = pd.DataFrame(payment_order_documents)

    # # second do the data conversions
    payment_orders_dataframe['_id'] = payment_orders_dataframe['_id'].astype(str)
    
    payment_orders_dataframe['amountInCents'] = np.floor(payment_orders_dataframe['amountInCents']).fillna(0).astype('Int32')
    payment_orders_dataframe['noOfItems'] = np.floor(payment_orders_dataframe['noOfItems']).fillna(0).astype('Int32')
    
    
    if 'paymentLink' in payment_orders_dataframe.columns:
        payment_orders_dataframe['paymentLink'] = payment_orders_dataframe['paymentLink'].fillna("unknown")
    
    # third deal with datetime fields
    transform_datetime_column(payment_orders_dataframe, 'createdAt')
    transform_datetime_column(payment_orders_dataframe, 'paymentLinkExpireAt')
    # # third rename columns to snake_case
    payment_orders_dataframe.rename(columns=MONGODB_TO_REDSHIFT_COLUMN_MAPPING, inplace=True)
    return payment_orders_dataframe

def transform_datetime_column(dataframe, datetime_column_name):
    if datetime_column_name in dataframe.columns:
        dataframe[datetime_column_name] = dataframe[datetime_column_name].astype(object).where(pd.notnull(dataframe[datetime_column_name]), None)

def load_dataframe_to_csv(payment_orders_dataframe, file_write_mode="w", include_header=True):
    try:
        payment_orders_dataframe.to_csv(CSV_OUTPUT_FILE, mode=file_write_mode, header=include_header, index=False)
        file_action = "exported" if file_write_mode == 'w' else "appended"
        logger.info(f"Data {file_action} successfully to {CSV_OUTPUT_FILE}")
    
    except Exception as csv_error:
        logger.error(f"Error saving to CSV: {csv_error}")
        raise
    
def load_dataframe_to_redshift(payment_orders_dataframe):
    
    #df_clean = clean_dataframe_for_db(df)
    try:
        for row_index, payment_order_row in payment_orders_dataframe.iterrows():
            redshift_cursor.execute(
                """
                INSERT INTO table_name (
                    _id, provider, items_type, amount_in_cents, status,
                    created_at, payment_link, payment_link_expire_at, no_of_items
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    payment_order_row['_id'], payment_order_row['provider'], payment_order_row['items_type'], payment_order_row['amount_in_cents'],
                    payment_order_row['status'], payment_order_row['created_at'], payment_order_row['payment_link'],
                    payment_order_row['payment_link_expire_at'], payment_order_row['no_of_items']
                )
            )
        redshift_connection.commit()
        logger.info("Data saved to Redshift successfully")
    except Exception as redshift_error:
        logger.error(f"Error saving to Redshift: {redshift_error}")
        raise

def extract_last_processed_timestamp():
    with open("last_processed_timestamp.txt", "r") as timestamp_file:
        return parse(timestamp_file.read())

def load_last_processed_timestamp(latest_timestamp_processed):
    with open("last_processed_timestamp.txt", "w") as timestamp_file:
        timestamp_file.write(latest_timestamp_processed.isoformat())

def extract_timestamp_from_mongodb(timestamp_tracking_collection):
    try:
        timestamp_document = timestamp_tracking_collection.find_one({"_id": "latest_timestamp"})
        if timestamp_document and "timestamp" in timestamp_document:
            return timestamp_document["timestamp"]
        else:
            logger.warning("No timestamp found in MongoDB, returning default value")
            return datetime.datetime.min
    except PyMongoError as mongodb_error:
        logger.error(f"Error fetching timestamp from MongoDB: {mongodb_error}")
        raise

def load_timestamp_to_mongodb(latest_timestamp_processed, timestamp_tracking_collection):
    try:
        timestamp_tracking_collection.update_one(
            {"_id": "latest_timestamp"},
            {"$set": {"timestamp": latest_timestamp_processed}},
            upsert=True
        )
        logger.info("Updated latest processed timestamp in MongoDB")
    except PyMongoError as mongodb_error:
        logger.error(f"Error updating timestamp in MongoDB: {mongodb_error}")
        raise

def etl_process(payment_orders_collection, timestamp_tracking_collection):
    logger.info("Starting ETL process")
    #latest_time_processed = extract_last_processed_timestamp()
    last_processed_timestamp = extract_timestamp_from_mongodb(timestamp_tracking_collection)
    #print(f"Last processed timestamp: {last_processed_timestamp}")
    new_payment_order_documents = extract_incremental_documents(payment_orders_collection, last_processed_timestamp)

    if not new_payment_order_documents:
        logger.info("No new documents found for incremental load")
        return
    
    transformed_payment_orders_dataframe = transform_documents_to_dataframe(new_payment_order_documents)
    #file_exists = os.path.isfile(CSV_OUTPUT_FILE)
    #load_dataframe_to_csv(transformed_payment_orders_dataframe, mode='a', header=not file_exists)
    #print(transformed_payment_orders_dataframe.info())
    
    load_dataframe_to_redshift(transformed_payment_orders_dataframe)
    if transformed_payment_orders_dataframe is not None and not transformed_payment_orders_dataframe.empty:
        latest_timestamp_processed = transformed_payment_orders_dataframe['created_at'].max()
        #load_last_processed_timestamp(latest_timestamp_processed)
        load_timestamp_to_mongodb(latest_timestamp_processed, timestamp_tracking_collection)
        logger.info(f"Updated last processed timestamp to {latest_timestamp_processed}")
        
    logger.info("ETL process completed successfully")
    
def main():
    logger.info("Starting ETL process")
    payment_orders_client, payment_orders_collection, timestamp_tracking_client, timestamp_tracking_collection = extract_mongodb_connections()
    
    try:
        # Perform full load
        #full_load(payment_orders_collection)
        
        etl_process(payment_orders_collection, timestamp_tracking_collection)
    except Exception as etl_error:
        logger.error(f"ETL process failed: {etl_error}")
        raise
        
    finally:
        if payment_orders_client and timestamp_tracking_client:
            payment_orders_client.close()
            timestamp_tracking_client.close()
            logger.info("MongoDB connections closed")

if __name__ == "__main__":
    main()
    logger.info("ETL process completed successfully")