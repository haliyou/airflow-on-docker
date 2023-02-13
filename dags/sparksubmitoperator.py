# from datetime import datetime, timedelta
# import pendulum
# from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.models import Variable



# default_args = {
#   'owner' : 'doyle',
#     'depends_on_past': False,
#     'start_date': datetime(2020, 10, 10),
#     'email': 'doyle.wei.chen@gmail.com',
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 0,
#     'retry_delay': timedelta(minutes=5)
# }

# dag = DAG(dag_id='tackle_pg_by_spark',
#     default_args = default_args,
#     catchup = False,
#     schedule_interval="0 * * * *")

# pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

# retrieve_data_from_pg_container = SparkSubmitOperator(task_id='tackle_pg',
#     conn_id='spark_local',
#     application=f'{pyspark_app_home}/spark/sparksubmitoperator.py',
#     total_executor_cores=4,
#     executor_cores=2,
#     executor_memory='5g',
#     driver_memory='5g',
#     name='show_pg_tables',
#     execution_timeout=timedelta(minutes=10),
#     dag=dag)

# retrieve_data_from_pg_container