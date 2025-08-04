from psycopg2.extensions import connection as Connection
from setup_db import server_connect,server_disconnect
import pandas as pd
import csv

def rank_test_types()->None:  
    connection:Connection|None=server_connect()
    try:
        sql="""SELECT concurrency,action_type,test_type,
                RANK() OVER (PARTITION BY concurrency,action_type ORDER BY duration_ms) as ranking
                FROM test"""
        if connection:
            df=pd.read_sql_query(sql,connection)
            df["rank"]=df["ranking"].apply(func=lambda x: "max" if x==3 else ("min" if x==1 else "avg"))
            print(df)
            df=df.pivot_table(index=["concurrency","action_type"],columns="test_type",values="rank",aggfunc="last")
            df.to_csv("reports/rank_test_types.csv")
    except:
        raise Exception("Exception ocurred during reporting in rank_test_types function")
    finally:
        server_disconnect(connection)

def normalised_table()->None:
    connection:Connection|None=server_connect()
    try:
        sql=""" SELECT test_name,concurrency,avg_time,
                COALESCE(best_run,avg_time) AS best_run,
                COALESCE(best_run,avg_time) AS worst_run
                FROM (
                    select 
                    test_name,concurrency,
                    AVG(duration_ms)::NUMERIC(10,3) AS avg_time,
                    MIN(best_time_ms)::NUMERIC(10,3) AS best_run,
                    MAX(worst_time_ms)::NUMERIC(10,3) AS worst_run
                    from test
                    GROUP BY test_name,concurrency
                )
                ORDER BY test_name,concurrency"""
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows=cursor.fetchall()
                with open("reports/normalised_table.csv",mode="w") as file:
                    writer=csv.writer(file)
                    for row in rows:
                        file.write(",".join(str(word) for word in row))
    except:
        raise Exception("Exception ocurred during reporting in normalised_table function")
    finally:
        server_disconnect(connection)
