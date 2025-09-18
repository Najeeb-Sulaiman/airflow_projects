from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum


with DAG(
    dag_id="rocket",
    start_date=pendulum.datetime(2025, 9, 10),
    schedule=None,
    catchup=False
) as dag:

    # Linear and Parallel dependecies
    fetch_customer = EmptyOperator(task_id="fetch_customer")
    fetch_complaint = EmptyOperator(task_id="fetch_complaint")

    clean_customer = EmptyOperator(task_id="clean_customer")
    clean_complaint = EmptyOperator(task_id="clean_complaint")

    # Linear and Parallel dependecies
    # fetch_customer >> clean_customer
    # fetch_complaint >> clean_complaint

    # Fan out dependency
    join_datasets = EmptyOperator(task_id="join_datasets")
    train_model = EmptyOperator(task_id="train_model")
    deploy_model = EmptyOperator(task_id="deploy_model")

    # Fan out dependency
    # fetch_customer >> clean_customer
    # fetch_complaint >> clean_complaint
    # [clean_customer, clean_complaint] >> join_datasets
    # join_datasets >> train_model >> deploy_model

    # Fan in dependency
    start = EmptyOperator(task_id="start")

    start >> [fetch_customer, fetch_complaint]
    fetch_customer >> clean_customer
    fetch_complaint >> clean_complaint
    [clean_customer, clean_complaint] >> join_datasets
    join_datasets >> train_model >> deploy_model
