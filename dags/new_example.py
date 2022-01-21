from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.utils import dates


default_args = {
    "owner": "Roberto Noorduijn Londono",
    "start_date": dates.days_ago(0),
    "email": [
        "roberto.noorduijn.londono@ibm.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="testing_storage",
    default_args=default_args,
    description="Testing if we can properly store things between tasks",
    schedule_interval=timedelta(days=1),
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'extract'",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="sleep 9999",
    )

    load = BashOperator(
        task_id="load",
        bash_command="echo 'load'",
    )

    extract >> transform >> load
