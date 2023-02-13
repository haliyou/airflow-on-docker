import pyspark
import os

def createSparSession():
    spark=pyspark.sql.SparkSession.builder.appName("pyspark_commnicate_with_postgre")\
    .config('spark.driver.extraClassPath', "/home/jovyan/work/postgresql-42.4.0.jar")\
    .config('spark.executor.extraClassPath',"/home/jovyan/work/postgresql-42.4.0.jar")\
    .getOrCreate()
    return spark

def loadPgTable():
    spark = createSparSession()
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_CLASSPATH"] = "/home/jovyan/work/postgresql-42.4.0.jar"
    roles_df = spark.read.format("jdbc")\
    .option("url","jdbc:postgresql://172.17.0.3:5432/postgres")\
    .option("dbtable","roles")\
    .option("user","postgres")\
    .option("password","airflow")\
    .option("driver","org.postgresql.Driver")\
    .load()
    return roles_df

if __name__ == "__main__":
    roles = loadPgTable()
    print(roles.show())


