from airflow.sdk import DAG, task
import pendulum

with DAG(
    dag_id="print_task_context",
    start_date=pendulum.datetime(2025, 9, 10),
    schedule=None
) as dag:
    @task
    def print_context(**context):
        print(context)

    print_context()
