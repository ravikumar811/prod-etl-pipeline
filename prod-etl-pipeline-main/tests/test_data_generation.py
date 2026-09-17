import unittest
import os
import pandas as pd
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import sys
import io
import logging
# Add the 'scripts' directory to the Python path
# This allows importing modules from 'scripts' directly.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

# Import the functions from your data_generator.py (assuming it's named generate_fake_data.py)
try:
    from data_genarator import generate_fake_data, save_data_to_csv, upload_to_gcs
except ImportError:
    # Fallback/Error handling if import fails.
    # In a real CI/CD, this should probably fail the build if the script isn't found.
    print("Error: Could not import functions from scripts/generate_fake_data.py.")
    print("Please ensure 'generate_fake_data.py' exists in the 'scripts' directory.")
    sys.exit(1) # Exit if the core script cannot be imported


class TestDataGeneration(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory for test files and define test parameters."""
        self.test_dir = tempfile.mkdtemp()
        self.num_customers = 3
        self.num_products = 5
        self.num_orders = 10
        self.bucket_name = "test-fake-bucket"
        self.file_path_prefix = "raw_data/test" # Corresponds to 'file_path' in upload_to_gcs

        # Capture logging output
        self.log_stream = io.StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.root_logger = logging.getLogger()
        self.root_logger.setLevel(logging.INFO)
        self.root_logger.addHandler(self.handler)

    def tearDown(self):
        """Clean up the temporary directory and logging handler after tests."""
        shutil.rmtree(self.test_dir)
        self.root_logger.removeHandler(self.handler)
        self.handler.close()

    def test_generate_fake_data_output_types_and_counts(self):
        """Test that generate_fake_data returns DataFrames with correct row counts."""
        customers_df, products_df, orders_df = generate_fake_data(
            self.num_customers, self.num_products, self.num_orders
        )

        self.assertIsInstance(customers_df, pd.DataFrame)
        self.assertIsInstance(products_df, pd.DataFrame)
        self.assertIsInstance(orders_df, pd.DataFrame)

        self.assertEqual(len(customers_df), self.num_customers)
        self.assertEqual(len(products_df), self.num_products)
        self.assertEqual(len(orders_df), self.num_orders)

    def test_generate_fake_data_column_names(self):
        """Test that generated DataFrames have expected column names."""
        customers_df, products_df, orders_df = generate_fake_data(1, 1, 1) # Minimal data

        expected_customer_cols = {
            'customer_id', 'name', 'email', 'address', 'city', 'state',
            'zip_code', 'country', 'registration_date'
        }
        self.assertSetEqual(set(customers_df.columns), expected_customer_cols)

        expected_product_cols = {
            'product_id', 'product_name', 'category', 'price', 'stock_quantity'
        }
        self.assertSetEqual(set(products_df.columns), expected_product_cols)

        expected_order_cols = {
            'order_id', 'customer_id', 'product_id', 'quantity', 'unit_price_at_order',
            'amount', 'transaction_id', 'transaction_type', 'order_date',
            'shipping_address', 'order_status', 'location'
        }
        self.assertSetEqual(set(orders_df.columns), expected_order_cols)

    def test_save_data_to_csv_creates_files(self):
        """Test that save_data_to_csv creates the expected CSV files."""
        customers_df, products_df, orders_df = generate_fake_data(1, 1, 1)
        save_data_to_csv(customers_df, products_df, orders_df, self.test_dir)

        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'customers.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'products.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, 'orders.csv')))

        # Verify file contents are not empty
        self.assertGreater(os.path.getsize(os.path.join(self.test_dir, 'customers.csv')), 0)
        self.assertGreater(os.path.getsize(os.path.join(self.test_dir, 'products.csv')), 0)
        self.assertGreater(os.path.getsize(os.path.join(self.test_dir, 'orders.csv')), 0)

        # Optional: Read back a file and check its content
        read_customers_df = pd.read_csv(os.path.join(self.test_dir, 'customers.csv'))
        self.assertEqual(len(read_customers_df), 1)
        self.assertEqual(read_customers_df['customer_id'].iloc[0], customers_df['customer_id'].iloc[0])


    @patch('google.cloud.storage.Client')
    def test_upload_to_gcs_with_service_account(self, MockStorageClient):
        """Test upload_to_gcs using a mocked service account authentication."""
        # Create dummy local files to be uploaded
        dummy_file_1 = os.path.join(self.test_dir, 'dummy_customer.csv')
        dummy_file_2 = os.path.join(self.test_dir, 'dummy_product.csv')
        with open(dummy_file_1, 'w') as f:
            f.write("id,name\n1,test1")
        with open(dummy_file_2, 'w') as f:
            f.write("id,name\n2,test2")

        mock_credentials = MagicMock()
        mock_bucket = MagicMock()
        mock_blob_customer = MagicMock()
        mock_blob_product = MagicMock()

        MockStorageClient.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.side_effect = [mock_blob_customer, mock_blob_product]

        # Mock service_account.Credentials.from_service_account_file
        with patch('google.oauth2.service_account.Credentials.from_service_account_file', return_value=mock_credentials) as mock_from_file:
            # Define a dummy service account key path
            dummy_key_path = "/tmp/dummy-key.json"
            # Ensure the dummy key file exists so os.path.isfile passes
            open(dummy_key_path, 'a').close()

            upload_to_gcs(
                bucket_name=self.bucket_name,
                file_path=self.file_path_prefix,
                source_directory=self.test_dir,
                project_id="test-project",
                service_account_key_path=dummy_key_path
            )

            # Clean up the dummy key file
            os.remove(dummy_key_path)

            mock_from_file.assert_called_once_with(dummy_key_path)
            MockStorageClient.assert_called_once_with(project="test-project", credentials=mock_credentials)
            MockStorageClient.return_value.bucket.assert_called_once_with(self.bucket_name)

            # Assert upload calls for each dummy file
            mock_blob_customer.upload_from_filename.assert_called_once_with(dummy_file_1)
            mock_blob_product.upload_from_filename.assert_called_once_with(dummy_file_2)

            # Check blob paths
            mock_bucket.blob.assert_any_call(f"{self.file_path_prefix}/dummy_customer.csv")
            mock_bucket.blob.assert_any_call(f"{self.file_path_prefix}/dummy_product.csv")

    @patch('google.cloud.storage.Client')
    def test_upload_to_gcs_with_adc(self, MockStorageClient):
        """Test upload_to_gcs using Application Default Credentials (no key path)."""
        # Create dummy local files to be uploaded
        dummy_file_1 = os.path.join(self.test_dir, 'dummy_adc_1.csv')
        with open(dummy_file_1, 'w') as f:
            f.write("id,name\n1,adc_test")

        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        MockStorageClient.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Ensure no service account key path is passed
        upload_to_gcs(
            bucket_name=self.bucket_name,
            file_path=self.file_path_prefix,
            source_directory=self.test_dir,
            project_id="test-project-adc",
            service_account_key_path=None # Explicitly None for ADC
        )

        # Assert Client was initialized without credentials
        MockStorageClient.assert_called_once_with(project="test-project-adc")
        MockStorageClient.return_value.bucket.assert_called_once_with(self.bucket_name)
        mock_blob.upload_from_filename.assert_called_once_with(dummy_file_1)
        mock_bucket.blob.assert_called_once_with(f"{self.file_path_prefix}/dummy_adc_1.csv")

    def test_upload_to_gcs_empty_directory(self):
        """Test upload_to_gcs when the source directory is empty."""
        with patch('google.cloud.storage.Client') as MockStorageClient:
            mock_bucket = MagicMock()
            MockStorageClient.return_value.bucket.return_value = mock_bucket

            upload_to_gcs(
                bucket_name=self.bucket_name,
                file_path=self.file_path_prefix,
                source_directory=self.test_dir, # This is an empty directory
                project_id="test-empty-dir"
            )
            # Assert that no upload operations were attempted
            mock_bucket.blob.assert_not_called()
            MockStorageClient.return_value.bucket.assert_called_once_with(self.bucket_name)


if __name__ == '__main__':
    unittest.main()