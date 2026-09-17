import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import logging

def main():

    parser = argparse.ArgumentParser(description="Load data from GCS to BigQuery using Dataproc.")
    parser.add_argument("--gcs_input_path", required=True, help="Path to the input data in GCS (e.g., gs://your-source-bucket/data/my_data.csv)")
    parser.add_argument("--bq_output_dataset", required=True, help="BigQuery output dataset name (e.g., your_dataset_name)")
    parser.add_argument("--bq_output_table", required=True, help="BigQuery output table name (e.g., your_table_name)")
    parser.add_argument("--gcs_temp_bucket", required=True, help="Temporary GCS bucket for BigQuery connector (e.g., gs://your-temp-bucket)")
    parser.add_argument("--input_format", default="csv", help="Input file format (e.g., csv, json, parquet, avro)")
    parser.add_argument("--infer_schema", action="store_true", help="Infer schema for CSV/JSON files")
    parser.add_argument("--header", action="store_true", help="Treat first row as header for CSV files")

    args = parser.parse_args()

    spark = SparkSession.builder.appName("GCStoBigQueryPipeline").getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    # The temporary GCS bucket is crucial for the BigQuery connector
    print(args.gcs_temp_bucket)
    spark.conf.set("spark.datasource.bigquery.temporaryGcsBucket", args.gcs_temp_bucket.split('/')[0])
    file_path = f"gs://{args.gcs_input_path}"

    print(f"Reading data from: {file_path}")
    print(f"Input format: {args.input_format}")

    # Read data from GCS
    if args.input_format == "csv":
        df = spark.read.format("csv") \
                       .option("header", args.header) \
                       .option("inferSchema", args.infer_schema) \
                       .load(file_path)
    elif args.input_format == "json":
        df = spark.read.format("json") \
                       .option("inferSchema", args.infer_schema) \
                       .load(file_path)
    elif args.input_format == "parquet":
        df = spark.read.format("parquet").load(file_path)
    elif args.input_format == "avro":
        df = spark.read.format("avro").load(file_path)
    else:
        raise ValueError(f"Unsupported input format: {args.input_format}")
        
    rows = df.count()
    print(f"No of rows : {rows}")
    
    print("Schema of the loaded DataFrame:")
    df.printSchema()
    
    table_id = f"{args.bq_output_dataset}.{args.bq_output_table}" # simpler if project is default or auto-detected

    print(f"Writing data to BigQuery table: {table_id}")
    df.repartition(4)
    # Write data to BigQuery
    df.write.format("bigquery") \
            .option("table", table_id) \
            .option("temporaryGcsBucket", args.gcs_temp_bucket.split('/')[0]) \
            .mode("overwrite") \
            .save()

    print("Data successfully loaded to BigQuery!")

    spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
