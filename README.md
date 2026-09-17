Ultimate_datapipeline
Creating a ETL data pipeline -

Extracting data from different source system to Google Big Query. The idea is to create data pipeline and create some dashboards on top of it - Retail Analytics Pipeline

Author - Ravikumar
Retail Analytics Pipeline
Use Case :

Let's say there is a online retail company like Amazon . They want to devide their customers into Platinum,Dimond,Gold, Silver,Normal customers based on the customer monthly activity - on attributes like money_spent ,no_of_orders ,time_spent ....,

Creating dashboards with the help of looker like

progression of daily active users
progression of product sales...
We are going to create an etl pipeline in GCP cloud with gcp services.

Services used -
Cloud Composer ( Airflow)
Cloud Storage
Big Query
Data Proc (Spark)
git
Jenkins ( CI/CD pipeline )
ETL Pipeline
Extract:

We are gonna use python to generate some fake data using faker library every day . Data like orders, Customers, Products data and loading this to gcs bucket. We will generate every day about 1000 records for each table and load them into gcs bucket in one go.

Transform:

Created a python script to raise a data proc cluster ,to submit the main python class to perfrom the extraction ,transforming and loading the data and finally after the job completion the cluster will be deleted ( for the cost optimization perpose)

We will use a pyspark script to get the data from gcs bucket and cache it for further transformations. We are going to implement some quality checks and joining the tables to satisy the usecase and atlast uncache the data.

Load:

Finally load the transformed data into the Big Query Data Warehouse

Git
Created git hub repository to stage all the application code and integrated git with VScode editor to push the code into github repo

CI/CD pipeline
We will raise a Jenkin server to set up a CI/CD pipeline from git hub to the production

1. Created a google compute engine for the Jenkins.
2. Installed Jenkins,java ,git on the GCE.
3. In the Jenkins interface under the credentials added the gcp service account credential ,Git Personal Access Token.
4. Created webhook for the jenkins trigger in git.
5. Built pipeline with webhook as the trigger.
So, whenever we push the code into the github with new changes the code will be automatically deployed into the production
