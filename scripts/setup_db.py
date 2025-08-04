from psycopg2 import connect
from psycopg2.extensions import connection as Connection
from dotenv import load_dotenv
import os,sqlparse

load_dotenv()

SQL_STATEMENTS=["SQL/create_tables.sql","SQL/trigger.sql"]

def server_connect(admin:bool=False)->Connection|None:
    try:
        usr=os.getenv("USER")
        passwd=os.getenv("PASSWORD")
        db=os.getenv("DB_NAME")
        if admin:
            usr=os.getenv("ADMIN")
            passwd=os.getenv("ADMIN_PASSWORD")
            db=os.getenv("ADMIN_DB_NAME")
        connection:Connection=connect(
                dbname=db,
                user=usr,
                password=passwd,
                host=os.getenv("HOST"),
                port=os.getenv("PORT")
        )
        return connection
    except ConnectionError as err:
        raise Exception(f"Error ocurred during server connection: {err}")
    
def server_disconnect(connection:Connection|None):
    try:
        if connection:
            connection.close()
    except ConnectionError as err:
        raise Exception(f"Exception ocurred during server disconnection:{err}")

def reset_db():
    connection:Connection|None=server_connect(admin=True)
    try:
        if connection:
            connection.autocommit=True
            with connection.cursor() as cursor:
                cursor.execute(f"""DROP DATABASE IF EXISTS "{os.getenv("DB_NAME")}" WITH (FORCE)""")
                cursor.execute(f"""CREATE DATABASE "{os.getenv("DB_NAME")}" """)
    except RuntimeError as err:
        raise Exception(f"Exception ocurred during reseting database. Exception:{err}")
    finally:
        server_disconnect(connection)

if __name__=="__main__":
    reset_db()
    for file in SQL_STATEMENTS:
        with open(file,encoding="UTF-8") as file:
            raw_sql=file.read()
            cleaned_sql=sqlparse.split(raw_sql)
            for statement in cleaned_sql:
                connection:Connection|None=server_connect()
                if connection:
                    connection.autocommit=True
                    with connection.cursor() as cursor:
                        if statement:
                            cursor.execute(statement.strip())
                server_disconnect(connection)