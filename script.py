import os
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import numpy as np
import pandas as pd
import datetime
from dateutil.parser import parse
import psycopg2
import boto3
import io

# Environment variables for MongoDB and Redshift connections
PAYMENT_ORDERS_URI = os.getenv("SOURCE_URI")
PAYMENT_ORDERS_DATABASE = os.getenv("SOURCE_DB")
PAYMENT_ORDERS_COLLECTION = os.getenv("TARGET_COLLECTION")
URIS_COLLECTION = os.getenv("URIS_COLLECTION")

TIMESTAMP_TRACKING_URI = os.getenv("SOURCE_URI2")
TIMESTAMP_TRACKING_DATABASE = os.getenv("SOURCE_DB2")
TIMESTAMP_TRACKING_COLLECTION = os.getenv("TARGET_COLLECTION2")

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

#s3 configuration
LOCAL_FILE_PATH = "merged_df.parquet" 
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = "bosta-fc-s3"
S3_PARTITION_PREFIX = "merged_hashed_link_two"


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
    "paymentLinkExpireAt": 1,
    "totalPaidAmountInCents": 1,
    "serviceFeesInCents": 1,
    "paymentShortenedLinkHash": 1
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
    'paymentLinkExpireAt': 'payment_link_expire_at',
    'totalPaidAmountInCents': 'total_paid_amount_in_cents',
    'serviceFeesInCents': 'service_fees_in_cents',
    'paymentShortenedLinkHash': 'payment_shortened_link_hash'
}

redshift_connection = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    database=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
redshift_cursor = redshift_connection.cursor()

s3 = boto3.client('s3')
table_name = "interns.payment_hashed_data"
#flag = True
# simulating the development and production environment
# if flag:
#     table_name = "interns.payment_orders"  # default table name
# else:
#     table_name = "interns.payment_orders_test"


# logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def mongodb_paymentOrders_connections():
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
    
def mongodb_urls_connections():
    try:
        urls_client = MongoClient(PAYMENT_ORDERS_URI)
        urls_database = urls_client[PAYMENT_ORDERS_DATABASE]
        urls_collection = urls_database[URIS_COLLECTION]
        logger.info("Connected to MongoDB successfully")
        
        return urls_client, urls_collection
    except PyMongoError as mongodb_error:
        logger.error("Could not connect to mongoDB. Connection Failed!")
        raise


# **************Extract************
def extract_paymentOrders_documents(payment_orders_collection, last_processed_timestamp):
    logger.info("Extracting payment orders documents from MongoDB")
    try:
        incremental_query = {"createdAt": {"$gt": last_processed_timestamp}}
        new_payment_documents = list(payment_orders_collection.find(incremental_query, PAYMENT_ORDER_FIELDS_PROJECTION))
        logger.info(f"Fetched {len(new_payment_documents)} documents incrementally from MongoDB")
        return new_payment_documents
    except PyMongoError as mongodb_error:
        logger.error(f"Error fetching incremental documents: {mongodb_error}")
        raise
    
def extract_urls_documents(urls_collection):
    logger.info("Extracting URLs documents from MongoDB")
    try:
        urls_documents = list(urls_collection.find({}, {"_id":0, "hash": 1, "clicks": 1}))
        logger.info(f"Fetched {len(urls_documents)} documents from MongoDB")
        return urls_documents
        
    except PyMongoError as mongodb_error:
        logger.error(f"Error fetching urls documents: {mongodb_error}")
        raise
    

# **************Transform************
def handle_datetime_column(dataframe, datetime_column_name):
    logger.info(f"Handling datetime column: {datetime_column_name}")
    if datetime_column_name in dataframe.columns:
        dataframe[datetime_column_name] = dataframe[datetime_column_name].astype(object).where(pd.notnull(dataframe[datetime_column_name]), None)    

def handle_datatype_conversion(dataframe, column_name, target_dtype):
    logger.info(f"Converting column '{column_name}' to {target_dtype}")
    if column_name in dataframe.columns:
        if target_dtype == 'Int32': 
            dataframe[column_name] = np.floor(dataframe[column_name]).astype('int32')
        elif target_dtype == str:
            dataframe[column_name] = dataframe[column_name].astype(str)
    
def filling_missing_values(dataframe, column_name, fill_value):
    logger.info(f"Filling missing values in column '{column_name}' with {fill_value}")
    if column_name in dataframe.columns:
        dataframe[column_name] = dataframe[column_name].fillna(fill_value)

def columns_renaming(dataframe):
    logger.info("Renaming columns according to mapping")
    if dataframe is not None and not dataframe.empty:
        dataframe.rename(columns=MONGODB_TO_REDSHIFT_COLUMN_MAPPING, inplace=True)
            
        
def transform_main(payment_order_documents, urls_documents):
    logger.info("Transforming documents is starting")
    
    if payment_order_documents is None or len(payment_order_documents) == 0 or urls_documents is None or len(urls_documents) == 0:
        logger.warning("No data to transform/process")
        return
    
    # first to dataframe
    payment_orders_dataframe = pd.DataFrame(payment_order_documents)
    urls_dataframe = pd.DataFrame(urls_documents)

    #filling missing values
    # payment orders 
    filling_missing_values(payment_orders_dataframe, 'amountInCents', 0)
    filling_missing_values(payment_orders_dataframe, 'noOfItems', 0)
    filling_missing_values(payment_orders_dataframe, 'totalPaidAmountInCents', 0)
    filling_missing_values(payment_orders_dataframe, 'serviceFeesInCents', 0)
    # urls
    filling_missing_values(urls_dataframe, 'clicks', 0)
    
    # second do the data conversions
    # payment orders conversions
    handle_datatype_conversion(payment_orders_dataframe, '_id', str)
    handle_datatype_conversion(payment_orders_dataframe, 'amountInCents', 'Int32')
    handle_datatype_conversion(payment_orders_dataframe, 'noOfItems', 'Int32')
    handle_datatype_conversion(payment_orders_dataframe, 'totalPaidAmountInCents', 'Int32')
    handle_datatype_conversion(payment_orders_dataframe, 'serviceFeesInCents', 'Int32')
    #urls conversions
    handle_datatype_conversion(urls_dataframe, 'hash', str)  #urls 
    handle_datatype_conversion(urls_dataframe, 'clicks', 'Int32') # urls 
    
    # third deal with datetime fields
    handle_datetime_column(payment_orders_dataframe, 'createdAt')
    handle_datetime_column(payment_orders_dataframe, 'paymentLinkExpireAt')
    
    # # third rename columns to snake_case
    columns_renaming(payment_orders_dataframe)
    
    logger.info("Documents transformed successfully")
    return payment_orders_dataframe, urls_dataframe


def merge_dataframes(payment_orders_dataframe, urls_dataframe):
    logger.info("Merging payment orders and URLs dataframes")
    if payment_orders_dataframe is None or urls_dataframe is None:
        logger.warning("One or both dataframes are empty, skipping merge")
        return None
    
    merged_dataframe = pd.merge(
        payment_orders_dataframe,
        urls_dataframe,
        left_on='payment_shortened_link_hash',
        right_on='hash',
        how='inner'
    )
    
    return merged_dataframe

# def load_dataframe_to_csv(payment_orders_dataframe, file_write_mode="w", include_header=True):
#     try:
#         payment_orders_dataframe.to_csv(CSV_OUTPUT_FILE, mode=file_write_mode, header=include_header, index=False)
#         file_action = "exported" if file_write_mode == 'w' else "appended"
#         logger.info(f"Data {file_action} successfully to {CSV_OUTPUT_FILE}")
    
#     except Exception as csv_error:
#         logger.error(f"Error saving to CSV: {csv_error}")
#         raise
    
# def load_dataframe_to_redshift(payment_orders_dataframe):
    
#     #df_clean = clean_dataframe_for_db(df)
#     try:
#         for row_index, payment_order_row in payment_orders_dataframe.iterrows():
#             redshift_cursor.execute(
#                 """
#                 INSERT INTO table_name (
#                     _id, provider, items_type, amount_in_cents, status,
#                     created_at, payment_link, payment_link_expire_at, no_of_items
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                 """,
#                 (
#                     payment_order_row['_id'], payment_order_row['provider'], payment_order_row['items_type'], payment_order_row['amount_in_cents'],
#                     payment_order_row['status'], payment_order_row['created_at'], payment_order_row['payment_link'],
#                     payment_order_row['payment_link_expire_at'], payment_order_row['no_of_items']
#                 )
#             )
#         redshift_connection.commit()
#         logger.info("Data saved to Redshift successfully")
#     except Exception as redshift_error:
#         logger.error(f"Error saving to Redshift: {redshift_error}")
#         raise


# ***************TIMESTAMP EXTRACTION AND UPDATING*************
def extract_timestamp_from_mongodb(timestamp_tracking_collection):
    logger.info("Extracting latest processed timestamp from MongoDB")
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
    
def get_last_processed_timestamp(df):
    logger.info("Getting last processed timestamp from DataFrame to update MongoDB")
    if df is not None and not df.empty:
        last_timestamp = df['created_at'].max()
        return last_timestamp
    else:
        logger.warning("DataFrame is empty, returning default timestamp")
        return datetime.datetime.min
    
def update_timestamp_mongodb(latest_timestamp_processed, timestamp_tracking_collection):
    logger.info("Updating latest processed timestamp in MongoDB")
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

#***************LOAD*************
def upload_dataframe_as_parquet_to_s3(df):
    logger.info("Uploading DataFrame as Parquet to S3")
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name="eu-west-1"
        )
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        s3_key = f"{S3_PARTITION_PREFIX}/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
        print(f"Uploading Parquet file to S3 bucket {S3_BUCKET_NAME} with key {s3_key}")
        logger.info(f"Uploading Parquet file to S3 bucket {S3_BUCKET_NAME} with key {s3_key}")
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )
        logger.info("Parquet file uploaded to S3 successfully")
        return s3_key
    except:
        logger.error("Error uploading Parquet file to S3")
        raise

def load_from_s3_to_redshift(s3_key):
    logger.info("Loading data from S3 to Redshift")
    try:
        copy_query = f""" 
        COPY {table_name}
        FROM 's3://{S3_BUCKET_NAME}/{s3_key}'
        ACCESS_KEY_ID '{AWS_ACCESS_KEY_ID}'
        SECRET_ACCESS_KEY '{AWS_SECRET_ACCESS_KEY}'
        FORMAT AS PARQUET;
        """
        redshift_cursor.execute(copy_query)
        redshift_connection.commit()
        logger.info("Data successfully loaded from S3 to Redshift")
    except Exception as redshift_error:
        logger.error(f"Error loading data from S3 to Redshift: {redshift_error}")
        raise

def etl_process(payment_orders_collection, timestamp_tracking_collection, urls_collection):
    logger.info("Starting ETL process")
    last_processed_timestamp = extract_timestamp_from_mongodb(timestamp_tracking_collection)
    #print(f"Last processed timestamp: {last_processed_timestamp}")
    
    # extract 
    payment_order_documents = extract_paymentOrders_documents(payment_orders_collection, last_processed_timestamp)
    urls_documents = extract_urls_documents(urls_collection)
    
    # transform
    payment_order_documents, urls_documents = transform_main(payment_order_documents, urls_documents)
    merged_dataframe = merge_dataframes(payment_order_documents, urls_documents)
    
    if merged_dataframe is None or merged_dataframe.empty:
        logger.warning("Merged DataFrame is empty, skipping timestamp update")
        return
    
    # Update the last processed timestamp in MongoDB
    last_processed_timestamp = get_last_processed_timestamp(merged_dataframe)
    update_timestamp_mongodb(last_processed_timestamp, timestamp_tracking_collection)
   
    # load
    s3_key = upload_dataframe_as_parquet_to_s3(merged_dataframe)
    load_from_s3_to_redshift(s3_key)

    logger.info("ETL process completed successfully")


def main():
    logger.info("Starting ETL process")
    payment_orders_client, payment_orders_collection, timestamp_tracking_client, timestamp_tracking_collection = mongodb_paymentOrders_connections()
    urls_client, urls_collection = mongodb_urls_connections()
    try:
        etl_process(payment_orders_collection, timestamp_tracking_collection, urls_collection)
    except Exception as etl_error:
        logger.error(f"ETL process failed: {etl_error}")
        raise
        
    finally:
        if payment_orders_client and timestamp_tracking_client:
            payment_orders_client.close()
            timestamp_tracking_client.close()
            urls_client.close()
            logger.info("MongoDB connections closed")

if __name__ == "__main__":
    main()
    logger.info("ETL process completed successfully")