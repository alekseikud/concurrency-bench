import os
import csv
import asyncio
import time
from typing import List,Any,Iterable
import math
import pandas as pd
from scripts.setup_db import server_connect,server_disconnect,reset_db
from psycopg2.extensions import connection as Connection

CONST=12000
TEST_FILES=["customers-100000","leads-100000","organizations-100000","products-100000"]


def timer(func):
    def wrapper(*args,**kwargs):
        start=time.perf_counter()
        result=func(*args,**kwargs)
        end=time.perf_counter()
        wrapper.time=(end-start) #type:ignore
        return result
    wrapper.time=None #type:ignore
    return wrapper


def clear_dataset()->None:
    os.system("rm datasets/*test*")

#########################################
############# IO-BOUND TESTS ############
#########################################

@timer
def read_csvs(files:List[str])->None:
    for i in range(CONST*10):
        try:
            for file in files:
                with open(f'datasets/{file}.csv') as f:
                    reader=csv.reader(f)
        except Exception as _ex:
            raise _ex

async def read_csvs_async(files:List[str])->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,read_csvs,files)

@timer
def write_csvs(files:List[str])->None:
    for file in files:
        df=pd.read_csv(f'datasets/{file}.csv')
        for i in range(CONST//2000):
            df.to_csv(f'datasets/{file}_test.csv')

async def write_csvs_async(files:List[str])->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,write_csvs,files)

@timer
def query_execution(query:str)->None:
    connection:Connection|None=server_connect()
    if connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

async def query_execution_async(query:str)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,query_execution,query)

#########################################
############# OS-BOUND TESTS ###########
#########################################

@timer
def copy_data(count:int=2000000):
    os.system(f"dd if=/dev/urandom of=test.txt count={count} bs=1")
    clear_dataset()

async def copy_data_async(count:int=2000000):
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,copy_data,count)

@timer
def tar_files(number:int=1):
    for i in range(number):
        os.system("""tar -czf datasets/archive.tar.gz datasets/* &&
                   rm datasets/*.csv && tar -xzf datasets/archive.tar.gz &&
                  rm datasets/*.gz""")
    
async def tar_files_async(number:int=1):
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,tar_files,number)

@timer
def gz_files(number:int=1):
    for i in range(number):
        os.system("gzip datasets/*")
        os.system("gunzip datasets/*")

#########################################
############# CPU-BOUND TESTS ###########
#########################################

@timer
def loop_sqrt():
    return [math.sqrt(itr) for itr in range(CONST**2)]

@timer
def loop_pow():
    return [math.pow(itr,itr) for itr in range(CONST)]

@timer
def loop_mult():
    return [itr*itr for itr in range(CONST**2)]


gz_files()
print(gz_files.time)