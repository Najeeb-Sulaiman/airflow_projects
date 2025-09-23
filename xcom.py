import uuid
from airflow.sdk import DAG  # , task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum


def _train_model(**context):
    model_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="model_id", value=model_id)


def _deploy_model(**context):
    model_id = context["task_instance"].xcom_pull(
        task_ids="train_model", key="model_id"
    )
    print(f"Deploying model {model_id}")


# A better way to do it
# def _train_model(ti):
#     model_id = str(uuid.uuid4())
#     ti.xcom_push(key="model_id", value=model_id)


# def _deploy_model(ti):
#     model_id = ti.xcom_pull(
#         task_ids="train_model", key="model_id"
#     )
#     print(f"Deploying model {model_id}")


# Another way to do it
# @task
# def _train_model():
#     model_id = str(uuid.uuid4())
#     return model_id


# @task
# def _deploy_model(model_id):
#     print(f"Deploying model {model_id}")


# model_id = _train_model()
# _deploy_model(model_id)


with DAG(
    dag_id="xcom",
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
    def _pick_crm_system(CRM_CHANGE_DATE, **context):
        if context["logical_date"] < CRM_CHANGE_DATE:
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

    # These will not run due to lack of trigger rule
    # join_datasets = EmptyOperator(task_id="join_datasets")
    join_datasets = EmptyOperator(task_id="join_datasets",
                                  trigger_rule="none_failed")
    train_model = PythonOperator(task_id="train_model", python_callable=_train_model)
    deploy_model = PythonOperator(task_id="deploy_model", python_callable=_deploy_model)

    start >> [pick_crm_system, fetch_complaint]
    pick_crm_system >> [fetch_customer_old, fetch_customer_new]
    fetch_customer_old >> clean_customer_old
    fetch_customer_new >> clean_customer_old
    fetch_complaint >> clean_complaint
    [clean_customer_old, clean_customer_new, clean_complaint] >> join_datasets
    join_datasets >> train_model >> deploy_model

    # Dummy task to explicitly join the different branches before continuing

    # join_crm_branch = EmptyOperator(task_id="join_crm_branch",
    #                                 trigger_rule="none_failed")

    # start >> [pick_crm_system, fetch_complaint]
    # pick_crm_system >> [fetch_customer_old, fetch_customer_new]
    # fetch_customer_old >> clean_customer_old
    # fetch_customer_new >> clean_customer_old
    # [clean_customer_old, clean_customer_new] >> join_crm_branch
    # fetch_complaint >> clean_complaint
    # [join_crm_branch, clean_complaint] >> join_datasets
    # join_datasets >> train_model >> deploy_model
