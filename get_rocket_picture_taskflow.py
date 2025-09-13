from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
import requests
import json
import requests.exceptions as requests_exceptions

with DAG(
    dag_id="rocket",
    start_date=pendulum.datetime(2025, 9, 10),
    schedule=None,
    catchup=False
) as dag:

    # Apply Bash to download the URL response with curl
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /opt/airflow/dags/rockets/launches/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming"
    )

    # A Python function that will parse the response and download all rocket pictures
    @task
    def _get_pictures():
        with open("/opt/airflow/dags/rockets/launches/launches.json") as f:
            launches = json.load(f)

            image_urls = []
            for launch in launches["results"]:
                image_urls.append(launch["image"])

            for image_url in image_urls:
                try:
                    response = requests.get(image_url)
                    image_filename = image_url.split("/")[-1]
                    target_file = f"/opt/airflow/dags/rockets/images/{image_filename}"

                    with open(target_file, "wb") as f:
                        f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")
                except requests_exceptions.MissingSchema:
                    print(f"{image_url} appears to be an invalid URL.")
                except requests_exceptions.ConnectionError:
                    print(f"Could not connect to {image_url}.")

    # Echo the number of images downloaded into the terminal
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /opt/airflow/dags/rockets/images/ | wc -l) images"'
    )

    # Set the order of execution of tasks.
    download_launches.set_downstream(_get_pictures)
    _get_pictures().set_downstream(notify)
