# Ultimate_datapipeline

Creating a ETL data pipeline - 
<p>
Extracting data from different source system to Google Big Query.
The idea is to create data pipeline and create some dashboards on top of it -
Retail Analytics Pipeline
</p>
Author - Patil Nithin Kumar Reddy


# Retail Analytics Pipeline

Use Case : 
1. Let's say there is a online retail company like Amazon . They want to devide their customers into Platinum,Dimond,Gold, Silver,Normal customers based on the customer monthly activity - on attributes like money_spent ,no_of_orders ,time_spent ....,

2. Creating dashboards with the help of looker like
   1. progression of daily active users
   2. progression of product sales...



<p> We are going to create an etl pipeline in GCP cloud with gcp services. </p>

# Services used -

1. Cloud Composer ( Airflow)
2. Cloud Storage
3. Big Query
4. Data Proc (Spark)
5. git 
6. Jenkins ( CI/CD pipeline )

           
# ETL Pipeline

Extract:

<p> We are gonna use python to generate some fake data using faker library every day . Data like orders, Customers, Products data and loading this to gcs bucket. 
We will generate every day about 1000 records for each table and load them into gcs bucket in one go.</p>

Transform:

<p>Created a python script to raise a data proc cluster ,to submit the main python class to perfrom the extraction ,transforming and loading the data and finally after the job completion the cluster will be deleted ( for the cost optimization perpose)
<p>We will use a pyspark script to get the data from gcs bucket and cache it for further transformations. We are going to implement some quality checks and 
joining the tables to satisy the usecase and atlast uncache the data.</p>

Load:

<p> Finally load the transformed data into the Big Query Data Warehouse </p>


# Git 

<p>Created git hub repository to stage all the application code and integrated git with VScode editor to push the code into github repo </p>


# CI/CD pipeline

<P>We will raise a Jenkin server to set up a CI/CD pipeline from git hub to the production</p>
1. Created a google compute engine for the Jenkins.<br>
2. Installed Jenkins,java ,git on the GCE.<br>
3. In the Jenkins interface under the credentials added the gcp service account credential ,Git Personal Access Token.<br>
4. Created webhook for the jenkins trigger in git.<br>
5. Built pipeline with webhook as the trigger.<br>

<p> So, whenever we push the code into the github with new changes the code will be automatically deployed into the production </p>
