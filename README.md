# MongoDB to Redshift ETL Pipeline

A simple Python ETL pipeline that extracts payment order data from MongoDB, transforms it with pandas, and loads it into Amazon Redshift.

## What This Project Does

This ETL pipeline helps move payment order data from MongoDB to Redshift for analytics and reporting. It handles incremental data loading so you don't have to process the same data twice.

## Features

- **Extract**: Gets payment order data from MongoDB collections
- **Transform**: Cleans and converts data using pandas (handles datetime fields, data types, column naming)
- **Load**: Inserts transformed data into Redshift database
- **Incremental Loading**: Only processes new records since last run
- **Timestamp Tracking**: Stores last processed timestamp in MongoDB
- **Error Handling**: Proper logging and error management
- **CSV Export**: Optional CSV output for data backup

## Project Structure

```
mongodb-to-redshift-etl/
├── script.py                      # Main ETL script
├── requirements.txt               # Python dependencies
├── last_processed_timestamp.txt   # Local timestamp backup
├── payment_orders.csv             # CSV output file
├── README.md                      # This file
└── .gitignore                     # Git ignore rules
```

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
# MongoDB source connection
export SOURCE_URI="mongodb://your-mongodb-uri"
export SOURCE_DB="your-source-database"
export TARGET_COLLECTION="your-collection-name"

# MongoDB timestamp tracking
export SOURCE_URI2="mongodb://your-timestamp-tracking-uri"
export SOURCE_DB2="your-timestamp-database"
export TARGET_COLLECTION2="your-timestamp-collection"

# Redshift connection
export REDSHIFT_HOST="your-redshift-host"
export REDSHIFT_PORT="5439"
export REDSHIFT_DB="your-redshift-database"
export REDSHIFT_USER="your-username"
export REDSHIFT_PASSWORD="your-password"
```

## Usage

Run the ETL pipeline:
```bash
python script.py
```

The script will:
1. Connect to MongoDB and Redshift
2. Check for new payment orders since last run
3. Transform the data (clean datetime fields, rename columns, handle nulls)
4. Insert into Redshift table `interns.payment_orders`
5. Update the last processed timestamp

## Data Mapping

MongoDB fields are mapped to Redshift columns:

| MongoDB Field | Redshift Column |
|---------------|-----------------|
| `_id` | `_id` |
| `createdAt` | `created_at` |
| `trackingnumber` | `tracking_number` |
| `provider` | `provider` |
| `itemsType` | `items_type` |
| `status` | `status` |
| `noOfItems` | `no_of_items` |
| `amountInCents` | `amount_in_cents` |
| `paymentLink` | `payment_link` |
| `paymentLinkExpireAt` | `payment_link_expire_at` |

## Requirements

- Python 3.7+
- MongoDB access
- Amazon Redshift access
- Required Python packages (see [`requirements.txt`](requirements.txt))


---

*This ETL pipeline helps automate the data flow from MongoDB to Redshift for better analytics and reporting capabilities.*