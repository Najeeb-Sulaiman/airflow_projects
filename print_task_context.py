from airflow.sdk import DAG, task
import pendulum

with DAG(
    dag_id="print_task_context",
    start_date=pendulum.datetime(),
    schedule=None
):
    @task
    def print_context(**kwargs):
        print(kwargs)

    print_context()
