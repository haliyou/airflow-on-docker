from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

def makeDatabase():

    dbName = 'weather_db'
    userName = 'airflow'
    tableName = 'weather_table'

    engine = create_engine('postgresql_psycopg2://%s@localhost/%s'%(userName, dbName))

    if not database_exists(engine.url):
        create_database(engine.url)

    conn = psycopg2.connect(database = dbName, user = userName)

    curr = conn.cursor()

    createTable = """CREATE TABLE IF NOT EXISTS %s
    (
        city   TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        todays_date DATE,
        humudity REAL,
        pressure REAL,
        min_temp REAL,
        max_temp REAL,
        temp REAL,
        weather TEXT
    )
    """ % tableName
    curr.execute(createTable)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    makeDatabase()