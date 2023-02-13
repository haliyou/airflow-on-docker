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
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
import requests
import numpy as np
# from airflow.operators.sensors import S3KeySensor
#comment out below due to botocore error -Broken DAG: [/usr/local/airflow/dags/e2eetl.py] No module named 'botocore'
# from airflow.hooks.S3_hook import S3Hook

default_args = {
    "owner":"airflow",
    "start_date":datetime(2020,11,1),
    "depend_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"doyle.wei.chen@gmail.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

# get_data = BashOperator(
#     task_id="get-data",
#     # bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/train.csv',
#     bash_command='curl -LJO https://raw.githubusercontent.com/NetWeiChen/MoviesApp/master/train.csv -o /usr/local/airflow/train.csv',
#     dag=dag
# )


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

# def transformdata(*args, **kwargs):
#     return NULL

# def writetextfile(ds, **kwargs):
#     with open("..\data\data.csv", "w") as fp:
#         # Add file generation/processing step here, E.g.:
#         fp.write(ds)

#         # Upload generated file to Minio
#         s3 = airflow.hooks.S_hook.S3Hook('local_minio')
#         s3.load_file("..\data\data.csv",
#                      key=f"my-test-file.txt",
#                      bucket_name="airflow")

 

def loaddata(*args, **kwargs):

    pg_hook = PostgresHook(postgres_conn_id='weather_id')

    file_name = str(datetime.now().date())+'.json'
    tot_name = os.path.join(os.path.dirname(__file__),  file_name)
    print(tot_name)

    with open(tot_name, 'r') as inputfile:
        doc = json.load(inputfile)

    city = str(doc['name'])
    country = str(doc['sys']['country'])
    lat = float(doc['coord']['lat'])
    lon = float(doc['coord']['lon'])
    humid = float(doc['main']['humidity'])
    press = float(doc['main']['pressure'])
    min_temp = float(doc['main']['temp_min'])
    max_temp = float(doc['main']['temp_max'])
    temp = float(doc['main']['temp'])
    weather = str(doc['weather'][0]['description'])
    todays_date = datetime.now().date()

    valid_data = True
    for valid in np.isnan([lat, lon, humid, press, min_temp, max_temp, temp]):
        if valid is False:
            valid_date = False
            break;

    row = (city, country, lat, lon, todays_date, humid, press, min_temp, max_temp, temp, weather)
    insert_cmd = """ INSERT INTO weather
                    (city, country, latitude, longitude, todays_date, humidity, pressure, min_temp, max_temp, temp, weather)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"""

    if valid_data is True:
        pg_hook.run(insert_cmd, parameters = row)


with DAG(dag_id="e2e_etl_dag",
        schedule_interval="@daily",
        default_args=default_args,
        template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
        catchup=False) as dag:

    

    loadTask = PythonOperator(
        task_id='transform_load',
        provide_context=True,
        python_callable=loaddata,
        dag=dag
    )

    # makeTableTask = BashOperator(
    #     task_id='prepare_table',
    #     bash_command = 'python makeTable.py',
    #     dag = dag

    # )

    fileSensorTask = FileSensor(task_id='fileDetector',
        # file_name = str(datetime.now().date())+'.json'
        # tot_name = os.path.join(os.path.dirname(__file__), file_name)
        # print('fileSensorTask: '+ tot_name)
        filepath='/usr/local/airflow/dags/2022-05-04.json',    
        poke_interval=30,
        dag=dag)
    #you can perform extract job by either calling BashOperator, or PythonOperator
    #option 1: BashOperator
    # extractBashTask = BashOperator(
    #     task_id='extract_weather_via_bash',
    #     bash_command = 'python .\src\apicall.py',
    #     dag=dag
    # )

    #option 2: PythonOperator
    extractPythonTask = PythonOperator(
    task_id = 'extract_weather_via_python',
    provide_context=True,
    python_callable=extractdata,
    dag=dag
    )

    #File could come in multiple scenarios: s3, kafka, nifi, datastage, sftp, restapi
    # isNewDataAvailable = FileSensor(
    # task_id="check_s3_for_file_in_s3",
    # filepath="2022-04-20.json",
    # poke_interval=5,
    # timeout=20
    # )

    t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "file detected" ',
    dag=dag)

    # t2 = PythonOperator(
    #     task_id='generate_and_upload_to_S3',
    #     provide_context=True,
    #     python_callable=writetextfile,
    #     dag=dag
    # )



    # sensor = S3KeySensor(
    # task_id='check_s3_for_file_in_s3',
    # bucket_key='data.csv',
    # wildcard_match=True,
    # bucket_name='airflow',
    # s3_conn_id='my_conn_S3',
    # timeout=18*60*60,
    # poke_interval=120,
    # dag=dag
    # )

    # createTableTask = PostgresOperator(
    # task_id="create_table",
    # sql = '''CREATE TABLE IF NOT EXISTS weather (
    #              city        TEXT,
    #                     country     TEXT,
    #                     latitude    REAL,
    #                     longitude   REAL,
    #                     todays_date DATE,
    #                     humidity    REAL,
    #                     pressure    REAL,
    #                     min_temp    REAL,
    #                     max_temp    REAL,
    #                     temp        REAL,
    #                     weather     TEXT
    #             );''',
    # postgres_conn_id='weather_id',
    # database='airflow'
    # )


    # extractPythonTask >> sensor
    extractPythonTask >> fileSensorTask >> loadTask