from minio import Minio
# from dotenv import load_dotenv
import os
from minio.error import S3Error
# load_dotenv()
# LOCAL_FILE_PATH = os.environ.get('LOCAL_FILE_PATH')
# ACCESS_KEY = os.environ.get('ACCESS_KEY')
# SECRET_KEY = os.environ.get('SECRET_KEY')
LOCAL_FILE_PATH = "c:/gdap/airflow/airflow2-on-docker/spark/trial.txt"

# LOCAL_FILE_PATH = "/home/jovyan/work/charCount.txt"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
MINIO_API_HOST = "http://localhost:9000/buckets"
MINIO_CLIENT = Minio("localhost:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)


def main():
    found = MINIO_CLIENT.bucket_exists("doyle")
    if not found:
       MINIO_CLIENT.make_bucket("doyle")
    else:
       print("Bucket already exists")
    MINIO_CLIENT.fput_object("doyle", "trial.txt",           LOCAL_FILE_PATH,)
    print("It is successfully uploaded to bucket")

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)