# Airflow, Dataproc, and DataStax Astra
In this demo, we will show you how to do the following all orchestrated by Airflow
1. Create a Dataproc cluster
2. Submit a Scala Spark Job that runs a `select * from database.keyspace.table` against Astra
3. Destroy Dataproc cluster upon job completion

If you want to modify the Scala Spark Jar, you can reference `spark-cassandra.scala` and the comments within the file.

## Click below to get started!

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/Anant/example-airflow-dataproc-astra)

### 1. Google Cloud Storage Bucket Setup

#### 1.1 Create a Google Cloud Storage Bucket: (https://console.cloud.google.com/storage/browser)
#### 1.2 Download and upload the `spark-cassandra-assembly-0.1.0-SNAPSHOT.jar` to your bucket
#### 1.3 Download and upload the your database's secure-connect-<db_name>.zip to your bucket
#### 1.4 Copy and paste down the `gsutil URI`'s for both of the files

### 2. Create and upload service account keyfile json to working directory
[Documentation on how to do so](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)

### 3. Update Google Connection in Airflow
#### 3.1 When prompted in the lower right-hand corner for `port 8080`, click `open browser`
Airflow credentials are `admin` `admin`
#### 3.2 Go to Admin -> Connections -> google_cloud_default

#### 3.3 Copy the full path of the keyfile.json and paste into the `Keyfile Path` section of the Google connection setup and save.

### 4. Update Variables

#### 4.1 Go to Admin -> Variables

#### 4.2 Update the values of all of the variables that we imported ahead of time.
Reference comments in poc.py if unsure

### 5. Turn on `example_airflow_google_dataproc_astra` DAG and run

### 6. Confirm cluster is being created in Google Dataproc (https://console.cloud.google.com/dataproc/clusters)

### 7. Once `spark_submit` task begins in Airflow, click on the job that is in the starting/running state (https://console.cloud.google.com/dataproc/jobs)

### 8. Confirm a dataframe containing data from `database.keyspace.table` is shown in the job's log

### 9. After spark_submit task completion, confirm the cluster is being destroyed / is destroyed in Google Dataproc

### 10. Delete Google Cloud Storage bucket as needed
