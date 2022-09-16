airflow db init

mkdir ~/airflow/dags

mv /workspace/example-airflow-dataproc-astra/poc.py ~/airflow/dags

airflow users create \
    --username admin \
    --password admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

airflow variables import /workspace/example-airflow-dataproc-astra/variables.json

airflow webserver --port 8080 &

airflow scheduler &
