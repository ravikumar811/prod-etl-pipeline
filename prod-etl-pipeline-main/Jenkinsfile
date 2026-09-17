pipeline {
    agent any

    environment {
        GCP_PROJECT_ID = 'pro-env-test'
        GCS_BUCKET_NAME = 'asia-south1-cloud-publishin-9441e4f8-bucket'
        COMPOSER_DAGS_FOLDER = 'dags' // This is typically 'dags' in Composer's GCS bucket
        // a variable for the Python executable for consistency
        PYTHON_EXEC = 'python3'
        VENV_DIR = '.venv'
    }


    stages {

        stage('Setup Python Environment') {
            steps {
                script {
                    echo "Creating Python virtual environment..."
                    // Create's the virtual environment
                    sh "${PYTHON_EXEC} -m venv ${VENV_DIR}"

                    echo "Upgrading pip within the virtual environment..."

                    sh "${VENV_DIR}/bin/pip install --upgrade pip"

                    echo "Installing project dependencies from requirements.txt..."
                    // Install project dependencies into the virtual environment
                    sh "${VENV_DIR}/bin/pip install -r requirements.txt"
                }
            }
        }


        stage('Linting') {
            steps {
                // Run's pylint on the Python files
                //sh "${PYTHON_EXEC} -m pylint dags/*.py scripts/*.py"
                sh "${VENV_DIR}/bin/pylint dags/*.py scripts/*.py"
            }
        }

        stage('Unit Tests') {
            steps {
                // Run's pytest on the Python files
                //sh "${PYTHON_EXEC} -m pytest tests/"
                sh "${VENV_DIR}/bin/pytest tests/"
            }
        }

        stage('Deploy to Staging GCS') {
            steps {
                // Useing withCredentials to securely access GCP service account key
                withCredentials([file(credentialsId: 'jenkins-service-acc', variable: 'GCP_KEY')]) {
                    sh """
                    # Authenticate gcloud with the service account key
                    gcloud auth activate-service-account --key-file=\$GCP_KEY --project=${GCP_PROJECT_ID}

                    # Copy DAGs to a 'staging' subfolder in your Composer DAGs bucket
                    gsutil -m cp -r dags/* gs://${GCS_BUCKET_NAME}/staging/${COMPOSER_DAGS_FOLDER}/

                    # Copy scripts to a 'staging' subfolder for scripts (or within dags folder if they are part of dags)
                    // Note: If your Composer environment expects scripts in a different path (e.g., 'data' or 'plugins'), adjust this path.
                    gsutil -m cp -r scripts/* gs://${GCS_BUCKET_NAME}/staging/scripts/
                    """
                }
            }
        }

        stage('Deploy to Production Composer') {
            steps {
                script {
                    // This input step provides a manual gate before deploying to production.

                    input message: "Approve deployment to production?", ok: "Deploy"
                }
                withCredentials([file(credentialsId: 'jenkins-service-acc', variable: 'GCP_KEY')]) {
                    sh """
                    # Authenticate gcloud with the service account key
                    gcloud auth activate-service-account --key-file=\$GCP_KEY --project=${GCP_PROJECT_ID}

                    // This is the key to not disturbing running jobs. We copy to a final location from staging.
                    // Airflow's sync mechanism will pick up the changes atomically.

                    # Copy DAGs from staging to the main Composer DAGs folder
                    gsutil -m cp -r gs://${GCS_BUCKET_NAME}/staging/${COMPOSER_DAGS_FOLDER}/* gs://${GCS_BUCKET_NAME}/${COMPOSER_DAGS_FOLDER}/

                    # Copy scripts from staging to the main scripts folder (adjust if scripts go elsewhere in Composer)
                    gsutil -m cp -r gs://${GCS_BUCKET_NAME}/staging/scripts/* gs://${GCS_BUCKET_NAME}/scripts/
                    """
                }
            }
        }
    }
}
