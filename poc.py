from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator

PROJECT_ID = Variable.get("PROJECT_ID")

# cluster name is up to you
CLUSTER_NAME = Variable.get("CLUSTER_NAME")

# region is up to you
REGION = Variable.get("REGION")

# path for bundle should be gsutil path from google cloud storage
JAR_URI = Variable.get("JAR_URI")

# path for bundle should be gsutil path from google cloud storage
ASTRA_BUNDLE_PATH = Variable.get("ASTRA_BUNDLE_PATH")

# https://docs.datastax.com/en/astra-serverless/docs/manage/org/manage-tokens.html
ASTRA_USERNAME = Variable.get("ASTRA_USERNAME")
ASTRA_SECRET = Variable.get("ASTRA_SECRET")

# grabbing db, keyspace, and table names so that the jar can run a select * from database.keyspace.table from Astra
ASTRA_DB_NAME = Variable.get("ASTRA_DB_NAME")
ASTRA_KEYSPACE_NAME = Variable.get("ASTRA_KEYSPACE_NAME")
ASTRA_TABLE_NAME = Variable.get("ASTRA_TABLE_NAME")

# more config options can be found here: https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig
# if adding any configs, need to convert camel casing to "_". For example masterConfig -> master_config
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    # came from datastax to help resolve the SSL issues
    "software_config": {
        "properties": {
            "dataproc:dataproc.conscrypt.provider.enable": "false"
        }
    }
}

LABELS = {
    "example_label": "example",
}

# configuration can be found here: https://cloud.google.com/dataproc/docs/reference/rest/v1/SparkJob
# if adding any configs, need to convert camel casing to "_". For example masterConfig -> master_config
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": [JAR_URI],
        "main_class": "sparkCassandra.POC",
        "properties": {
             # came from datastax to help resolve the SSL issues
            "spark.jars.packages": "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
        },
        "args": [ASTRA_BUNDLE_PATH, ASTRA_USERNAME, ASTRA_SECRET, ASTRA_DB_NAME, ASTRA_KEYSPACE_NAME, ASTRA_TABLE_NAME]
    },
    "labels": LABELS,
}

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='example_airflow_google_dataproc_astra',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        # if we want to use GKE, then need to swap cluster_config for virtual_cluster_config:
        # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html#airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator
        # https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#VirtualClusterConfig
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        labels=LABELS
    )

    spark_submit = DataprocSubmitJobOperator(
        task_id="spark_submit", job=SPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )
    
    create_cluster >> spark_submit >> delete_cluster
