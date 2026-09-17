from faker import Faker
import logging
import random
from google.cloud import storage
from google.oauth2 import service_account
import os
import pandas as pd

fake = Faker()

def generate_fake_data(NUM_CUSTOMERS,NUM_PRODUCTS,NUM_ORDERS):
    """Generates fake e-commerce customer, product, and order data."""
    logging.info("Generating fake e-commerce data...")

    # --- 1. Generate Customer Data ---
    customers = []
    for i in range(NUM_CUSTOMERS):
        customer_id = f"CUST{str(i+1).zfill(5)}"
        customers.append({
            'customer_id': customer_id,
            'name': fake.name(),
            'email': fake.unique.email(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.postcode(),
            'country': fake.country(),
            'registration_date': fake.date_time_between(start_date='-2y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        })
    customers_df = pd.DataFrame(customers)
    logging.info(f"Generated {NUM_CUSTOMERS} customers.")

    # --- 2. Generate Product Data ---
    products = []
    product_categories = ['Electronics', 'Books', 'Home & Kitchen', 'Apparel', 'Sports & Outdoors', 'Toys', 'Beauty']
    for i in range(NUM_PRODUCTS):
        product_id = f"PROD{str(i+1).zfill(5)}"
        products.append({
            'product_id': product_id,
            'product_name': fake.unique.word().capitalize() + " " + random.choice(['Gadget', 'Book', 'Supply', 'Tool', 'Accessory']),
            'category': random.choice(product_categories),
            'price': round(random.uniform(5.0, 500.0), 2),
            'stock_quantity': random.randint(0, 1000)
        })
    products_df = pd.DataFrame(products)
    logging.info(f"Generated {NUM_PRODUCTS} products.")

    # --- 3. Generate Order Data ---
    orders = []
    transaction_types = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']

    # Ensure customers and products are available for lookup
    customer_ids = customers_df['customer_id'].tolist()
    product_details = products_df.set_index('product_id')['price'].to_dict()
    customer_locations = customers_df.set_index('customer_id')['city'].to_dict() # Use city as location

    for i in range(NUM_ORDERS):
        order_id = f"ORD{str(i+1).zfill(7)}"
        customer_id = random.choice(customer_ids)
        product_id = random.choice(list(product_details.keys())) # Ensure product_id exists
        quantity = random.randint(1, 5)
        product_price = product_details[product_id]
        amount = round(product_price * quantity, 2)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')

        orders.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price_at_order': product_price, # Price at the time of order
            'amount': amount,
            'transaction_id': fake.uuid4(),
            'transaction_type': random.choice(transaction_types),
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'shipping_address': fake.address(),
            'order_status': random.choice(['pending', 'completed', 'shipped', 'cancelled']),
            'location': customer_locations.get(customer_id, fake.city()) # Get city from customer, fallback if not found
        })
    orders_df = pd.DataFrame(orders)
    logging.info(f"Generated {NUM_ORDERS} orders.")

    return customers_df, products_df, orders_df

def save_data_to_csv(customers_df, products_df, orders_df,DATA_DIR):
    """Saves DataFrames to CSV files in a local directory."""
    logging.info(f"Saving data to local directory: {DATA_DIR}")
    os.makedirs(DATA_DIR, exist_ok=True)

    customers_df.to_csv(os.path.join(DATA_DIR, 'customers.csv'), index=False)
    products_df.to_csv(os.path.join(DATA_DIR, 'products.csv'), index=False)
    orders_df.to_csv(os.path.join(DATA_DIR, 'orders.csv'), index=False)
    logging.info("Data saved successfully as CSV files.")

def upload_to_gcs(bucket_name,file_path, source_directory, project_id=None, service_account_key_path=None):
    """Uploads CSV files from a local directory to a GCS bucket."""
    print(f"Attempting to upload files from '{source_directory}' to GCS bucket: '{bucket_name}'")

    if service_account_key_path is not None:
        # Authenticate using a service account key file
        print(f"Authenticating with service account key: {service_account_key_path}")
        try:
            credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
            storage_client = storage.Client(project=project_id, credentials=credentials)
        except Exception as e:
            raise Exception(f"Failed to load service account credentials from {service_account_key_path}: {e}")
    else:
        # Fallback to Application Default Credentials
        print("Authenticating using Application Default Credentials (e.g., gcloud auth application-default login)")
        storage_client = storage.Client(project=project_id)

    bucket = storage_client.bucket(bucket_name)

    for filename in os.listdir(source_directory):
        local_file_path = os.path.join(source_directory, filename)
        if os.path.isfile(local_file_path):
            blob_path = f"{file_path}/{filename}" # Path inside the GCS bucket
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {filename} to gs://{bucket_name}/{blob_path}")
    logging.info("All files uploaded to GCS.")