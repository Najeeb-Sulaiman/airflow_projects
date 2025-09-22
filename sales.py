from airflow.sdk import DAG
from pendulum import datetime
import pandas as pd


with DAG(
    dag_id="sales_data",
    start_date=datetime(22, 9,2025),
    schedule=None
) as dag:

    def extract():
        data_source = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTIBPRSnUp10oHj5fhnUTlUJn-W7DtjSRtXIhySqW_1nZak5u4b7bPc9gTcWPHM22JL0aZJJa5QakM1/pub?gid=0&single=true&output=csv"
        df = pd.read_csv(data_source)
        df.to_csv("dags/rockets/data/sales.csv")

    def transform():
        df = pd.read_csv("dags/rockets/data/sales.csv")
        # Clean missing values
        df = df.fillna("Unkmown")
        # Remove duplicate
        df = df.drop_duplicates()
        # Convert dates to datetime
        df['order_date'] = pd.to_datetime(df['order_date'])
        # Add a calculated column total_sale = quantity * price
        df['total_sale'] = df['quantity'] * df['price']
