try:

    # from airflow.hooks.S3_hook import S3Hook
    from datetime import timedelta
    from airflow import DAG
    
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email_operator import EmailOperator

    from datetime import datetime

    # Setting up Triggers
    from airflow.utils.trigger_rule import TriggerRule

    print("All Dag modules are ok ......")
 # def failureFunction(context):
#     dag_run = context.get('dag_run')
#     msg = "DAG Failed"
#     subject = f"DAG {dag_run} Failed"
#     EmailOperator(to=net.weichen@gmail.com, subject=subject, html_content=msg)

# def successFunction(context):
#     dag_run = context.get('dag_run')
#     msg = "DAG run succeeded"
#     subject = f"DAG {dag_run} has completed!"
#     EmailOperator(to=net.weichen@gmail.com, subject=subject,html_content=msg)

except Exception as e:
    print("Error  {} ".format(e))


def first_function(**context):
    # airflow.utils.email.send_email('doyle.wei.chen@gmail.com', 'airflow test here', 'this is airflow sending notice')
    print("Hello world this works ")


def on_failure_callback(context):
    print("Fail works  !  ")


with DAG(dag_id="first_dag",
         schedule_interval="@once",
         default_args={
             "owner": "airflow",
             "start_date": datetime(2020, 11, 1),
             "retries": 1,
             "retry_delay": timedelta(minutes=1),
             'on_failure_callback': on_failure_callback,
             'email': ['doyle.wei.chen@gmail.com'],
             'email_on_failure': False,
             'email_on_retry': False,
         },
         catchup=False) as dag:

    first_function = PythonOperator(
        task_id="first_function",
        python_callable=first_function,
    )

    email = EmailOperator(
        task_id='send_email',
        to='doyle.wei.chen@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test Airflow </h3> """,
    )


first_function >> email