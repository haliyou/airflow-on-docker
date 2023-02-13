import json
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import config as c
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor

from airflow.hooks.base_hook import BaseHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator

from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
import requests
import random
# from airflow.operators.sensors import S3KeySensor
#comment out below due to botocore error -Broken DAG: [/usr/local/airflow/dags/e2eetl.py] No module named 'botocore'
# from airflow.hooks.S3_hook import S3Hook

default_args = {
    "owner":"airflow",
    "start_date":datetime(2020,11,1),
    "depend_on_past":False,
    "email_on_failure":True,
    "email_on_retry":False,
    "email":"doyle.wei.chen@gmail.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

with DAG(dag_id="ondemand_airflow_workflow_demo",
        schedule_interval="@once",
        default_args=default_args,
        template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
        catchup=False) as dag:
        
    fileSensorTask = FileSensor(task_id='fileDetector',
        filepath='/usr/local/airflow/train.csv',    
        poke_interval=30,
        dag=dag)

    def extractdata(*args, **kwargs):
        parameters= { 'q':'Toronto, CA', 'appid': c.API_KEY}
        result = requests.get("http://api.openweathermap.org/data/2.5/weather?", parameters)

        if result.status_code == 200:
            json_data = result.json()
            file_name = str(datetime.now().date()) + '.json'
            print(os.path.dirname(__file__))
            tot_name = os.path.join(os.path.dirname(__file__), file_name)
            print(tot_name)
            with open(tot_name, 'w' ) as outputfile:
                json.dump(json_data, outputfile)
        else:
            print("Error in API CALL")

    #if you want to comply with production standard, using below to log
        #set spark context and session
    #     conf = SparkConf()
    #     sc = SparkContext.getOrCreate(conf=conf)
    #     spark = SparkSession(sc)
    # log4jLogger = sc._jvm.org.apache.log4jLogger
    # LOGGER = log4jLogger.LogManager.getLogger(__name__)
    # LOGGER.info(f"logger: foo of avaerage age is :{df.age.mean}")
    def getMeanValueForMale():
        df = pd.read_csv('/usr/local/airflow/train.csv')      
        df = df.loc[df.Sex == 'male']
        print(f"Foo of average age is : {df.Age.mean}")


    pythonBranchForMale = PythonOperator(
        task_id = 'pythonBranchForFoo',
        python_callable = getMeanValueForMale,
        dag = dag
    )

    
    def getMeanValueForFemale():
        df = pd.read_csv('/usr/local/airflow/train.csv')
        df = df.loc[df.Sex == 'female']
        print(f"Boo of average age is: {df.Age.mean}")
        #print will not persist in logs which means print statement will not show in airflow logs if logger has been used

    pythonBranchForFemale = PythonOperator(
        task_id = 'pythonBranchForBar',
        python_callable = getMeanValueForFemale,
        dag = dag
    )

    def randomizedFactor():
        return random.choice(['male', 'female'])

    randomizedTask = PythonOperator(
        task_id='randomizedTask',
        python_callable = randomizedFactor,
        dag=dag
    )

    def forkCondition(**context):
        value = context['task_instance'].xcom_pull(task_ids='randomizedTask')
        if value == 'male':
            return 'pythonBranchForFoo'
        if value == 'female':
            return 'pythonBranchForBar'


    seperateBranch = BranchPythonOperator(
        task_id = 'conditional',
        python_callable = forkCondition,
        provide_context = True,
        dag = dag
    )


    fileDetected = BashOperator(
        task_id='fileArrived',
        bash_command='echo "file detected" ',
        dag=dag)

   

    sending_email = EmailOperator(
        task_id='sending_email',
        to='doyle.wei.chen@gmail.com',
        subject='Airflow workflow Demo!!!',
        html_content="""<h1>Demo for simplified Airflow Workflow</h1>""",
        )
    
    operation_done = DummyOperator(
        task_id='storing',
        trigger_rule='none_failed_or_skipped'
        )
    # extractPythonTask >> sensor
    # t1.set_upstream(fileSensorTask)
    fileSensorTask >> fileDetected >> randomizedTask >> seperateBranch >> [pythonBranchForMale, pythonBranchForFemale] >> operation_done >> sending_email