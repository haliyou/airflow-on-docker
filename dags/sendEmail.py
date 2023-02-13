from datetime import datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator


default_args = {
    "owner": "doyle",
    "start_date": datetime(2022, 2, 16),
    'email': ['doyle.wei.chen@gmail.com'],
    'email_on_failure': True,
}

with DAG(dag_id="Sending_mail",
        schedule_interval="@once",
        default_args=default_args,
        ) as dag:

    task_1 = BashOperator(
            task_id="task_1",
            bash_command="echo 'email sending'",
        )

    sending_email = EmailOperator(
        task_id='sending_email',
        to='doyle.wei.chen@gmail.com',
        subject='Airflow Alert Demo!!!',
        html_content="""<h1>Demo for Testing Email using Airflow</h1>""",
        )

    task_2 = BashOperator(
        task_id='task_2',
        bash_command="echo 'email sent'",
    )

task_1 >> sending_email >> task_2