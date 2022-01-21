from airflow import DAG
from airflow.operators.bash import BashOperator

import datetime as dt

default_args = {
    "owner": "roberto",
    "start_date": dt.datetime(2021, 7, 28),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "simple_example",
    description="Simple example DAG",
    default_args=default_args,
    schedule_interval=dt.timedelta(seconds=5),
) as dag:

    task1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Greetings!. The date and time are'",
    )

    task2 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task1 >> task2
