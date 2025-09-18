from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
import pendulum


with DAG(
    dag_id="rocket",
    start_date=pendulum.datetime(2025, 9, 10),
    schedule=None,
    catchup=False
) as dag:

    CRM_CHANGE_DATE = pendulum.datetime(2025, 9, 15)

    start = EmptyOperator(task_id="start")

    # Old CRM tasks
    fetch_customer_old = EmptyOperator(task_id="fetch_customer_old")
    clean_customer_old = EmptyOperator(task_id="clean_customer_old")

    fetch_customer_new = EmptyOperator(task_id="fetch_customer_new")
    clean_customer_new = EmptyOperator(task_id="clean_customer_new")

    # Function to pick the right CRM to process data from
    def _pick_crm_system(**context):
        if context["execution_date"] < CRM_CHANGE_DATE:
            return "fetch_customer_old"
        else:
            return "fetch_customer_new"

    pick_crm_system = BranchPythonOperator(
        task_id="pick_crm_system",
        python_callable=_pick_crm_system,
        op_args=[CRM_CHANGE_DATE]
    )

    fetch_complaint = EmptyOperator(task_id="fetch_complaint")
    clean_complaint = EmptyOperator(task_id="clean_complaint")

    join_datasets = EmptyOperator(task_id="join_datasets")
    train_model = EmptyOperator(task_id="train_model")
    deploy_model = EmptyOperator(task_id="deploy_model")

    start >> [pick_crm_system, fetch_complaint]
    pick_crm_system >> [fetch_customer_old, fetch_customer_new]
    fetch_customer_old >> clean_customer_old
    fetch_customer_new >> clean_customer_old
    fetch_complaint >> clean_complaint
    [clean_customer_old, clean_customer_new, clean_complaint] >> join_datasets
    join_datasets >> train_model >> deploy_model
