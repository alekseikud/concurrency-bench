from scripts.bench_tests import bench_all
import scripts.setup_db as setup_db
from psycopg2.extensions import connection as Connection
import inspect
import asyncio
import sqlparse


if __name__=="__main__":
    with open("create_tables.sql") as file:
        raw_sql=file.read()
        cleaned_sql=sqlparse.split(raw_sql)
        for statement in cleaned_sql:
            connection:Connection|None=setup_db.server_connect()
            if connection:
                connection.autocommit=True
                with connection.cursor() as cursor:
                    if statement:
                        cursor.execute(statement.strip())
            setup_db.server_disconnect(connection)
    for i in range(2,6):
        bench_all(i)