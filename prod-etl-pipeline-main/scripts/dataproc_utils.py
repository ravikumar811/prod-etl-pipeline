import subprocess
import logging


def create_dataproc_cluster_cmd():
    """
    Constructs and returns the command list for creating a Dataproc cluster.
    """
    return [
        "gcloud", "dataproc", "clusters", "create", "cluster-dev-01",
        "--region", "asia-south1",
        "--master-machine-type", "n1-standard-4",
        "--master-boot-disk-size", "50",
        "--image-version", "2.2-debian12",
        "--project", "pro-env-test",
        "--service-account", "supreme-prod-test@pro-env-test.iam.gserviceaccount.com",
        "--initialization-actions", "gs://repo-dev-buck/central_hub/scripts/install_jdbc.sh",
        "--single-node"
    ]

def delete_dataproc_cluster_cmd(cluster_name="cluster-dev-01", region="asia-south1", project_id="pro-env-test"):
    """
    Constructs and returns the command list for deleting a Dataproc cluster.
    """
    return [
        "gcloud", "dataproc", "clusters", "delete", cluster_name,
        "--region", region,
        "--project", project_id,
        "--quiet"
    ]

def submit_dataproc_gcs_to_bq_job_cmd(
    gcs_input_path,
    bq_output_dataset,
    bq_output_table,
    project_id,
    gcs_temp_bucket,
    input_format,
    header,
    infer_schema,
    cluster_name="cluster-dev-01",
    region="asia-south1",
    job_file_uri="gs://repo-dev-buck/central_hub/scripts/gcs_to_bigquery.py"
):
    """
    Constructs and returns the command list for submitting the gcs_to_bigquery.py PySpark job.

    Args:
        gcs_input_path (str): GCS path to the input data.
        bq_output_dataset (str): BigQuery output dataset name.
        bq_output_table (str): BigQuery output table name.
        gcs_temp_bucket (str): Temporary GCS bucket for BigQuery connector.
        input_format (str): Input file format (e.g., 'csv', 'json').
        header (bool): True if input file has a header.
        infer_schema (bool): True to infer schema from input data.
        cluster_name (str): Name of the Dataproc cluster.
        region (str): Region of the Dataproc cluster.
        project_id (str): GCP project ID.
        job_file_uri (str): GCS URI of the PySpark script.
    """
    command = [
        "gcloud", "dataproc", "jobs", "submit", "pyspark",
        job_file_uri,
        "--cluster", cluster_name,
        "--region", region,
        "--project", project_id,
        "--" # Separator for arguments passed to the PySpark script
    ]

    # Append job-specific arguments
    command.append(f"--gcs_input_path={gcs_input_path}")
    command.append(f"--bq_output_dataset={bq_output_dataset}")
    command.append(f"--bq_output_table={bq_output_table}")
    command.append(f"--gcs_temp_bucket={gcs_temp_bucket}")
    command.append(f"--input_format={input_format}")

    if header:
        command.append("--header")
    if infer_schema:
        command.append("--infer_schema")

    return command

def run_gcloud_command(command_list, success_message_contains=None, failure_message_contains="ERROR:"):
    """
    Executes a gcloud command, streams its output, and monitors for success/failure.

    Args:
        command_list (list): The gcloud command as a list of strings.
        success_message_contains (str, optional): A substring to look for to confirm success.
                                                  Defaults to None.
        failure_message_contains (str, optional): A substring to look for to indicate failure.
                                                  Defaults to "ERROR:".
    Raises:
        RuntimeError: If the command fails or an error message is detected in the output.
        FileNotFoundError: If 'gcloud' command is not found.
    """
    log = logging.getLogger(__name__) # Get logger specific to this module

    log.info(f"Executing command: {' '.join(command_list)}")

    try:
        process = subprocess.Popen(
            command_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        success_found = False
        for line in process.stdout:
            log.info(f"Command output: {line.strip()}")
            if failure_message_contains and failure_message_contains in line:
                log.error(f"Failure detected in output: {line.strip()}")
                raise RuntimeError(f"Command failed: {line.strip()}")
            if success_message_contains and success_message_contains in line:
                success_found = True

        return_code = process.wait()

        if return_code == 0:
            if success_message_contains and not success_found:
                log.warning(f"Command completed successfully but '{success_message_contains}' not found in output.")
            log.info(f"Command completed successfully with exit code: {return_code}")
            return True
        else:
            log.error(f"Command failed with exit code: {return_code}")
            raise RuntimeError(f"Command failed with exit code: {return_code}")

    except FileNotFoundError:
        log.error("gcloud command not found. Please ensure Google Cloud SDK is installed and in your PATH.")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')