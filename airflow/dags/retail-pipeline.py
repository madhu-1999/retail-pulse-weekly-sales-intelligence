from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Fetch as integer directly from Airflow Variables
JOB_ID = int(Variable.get("databricks-job-id", default_var=155057253797066))

with DAG(
    dag_id="retail-pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * 3",
    catchup=False,
) as dag:
    # Trigger Databricks Job
    trigger_job = DatabricksRunNowOperator(
        task_id="trigger_existing_job",
        databricks_conn_id="databricks_default",
        job_id=JOB_ID,
    )

    # Publish to Kafka
    publish_to_kafka = BashOperator(
        task_id="publish_to_kafka",
        bash_command="python /home/madhulikasawant/retailpulse/kafka-cluster/producer.py",
    )

    # Consume from Kafka
    consume_kafka_events = BashOperator(
        task_id="consume_kafka_events",
        bash_command="python /home/madhulikasawant/retailpulse/kafka-cluster/consumer.py",
    )

    # Set dependencies
    trigger_job >> publish_to_kafka >> consume_kafka_events
