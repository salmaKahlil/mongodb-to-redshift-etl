import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import gspread
import numpy as np
import pandas as pd
from gspread_dataframe import set_with_dataframe

SOURCE_URI = os.getenv("SOURCE_URI")
SOURCE_DB = os.getenv("SOURCE_DB")
TARGET_COLLECTION = os.getenv("TARGET_COLLECTION")


try:
    client = MongoClient(SOURCE_URI)
    source_db = client[SOURCE_DB]
    target_collection = source_db[TARGET_COLLECTION]
    documents = list(target_collection.find({},{"createdAt": 1, "trackingnumber": 1,"provider":1,"itemsType":1,"status":1,"noOfItems":1,"amountInCents":1,"paymentLink":1,"paymentLinkExpireAt":1}))
    documents_df = pd.DataFrame(documents)

    # converting datatypes (floats to integers)
    documents_df['amountInCents'] = np.floor(documents_df['amountInCents']).fillna(0).astype('Int32')
    documents_df['noOfItems'] = np.floor(documents_df['noOfItems']).fillna(0).astype('Int32')
    #print(documents_df.info())
    
    # renaming columns
    documents_df.rename(columns={
        'createdAt': 'created_at',
        'trackingnumber': 'tracking_number',
        'provider': 'provider',
        'itemsType': 'items_type',
        'status': 'status',
        'noOfItems': 'no_of_items',
        'amountInCents': 'amount_in_cents',
        'paymentLink': 'payment_link',
        'paymentLinkExpireAt': 'payment_link_expire_at'
    }, inplace=True)
    
    # to csv
    documents_df.to_csv('payment_orders.csv', index=False)
    print("Data exported successfully to payment_orders.csv")
    

except PyMongoError as e:
       print(f"MongoDB connection error: {e}")

finally:
       if 'client' in locals():
           client.close()