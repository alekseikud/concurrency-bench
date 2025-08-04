import os,requests,csv,asyncio,time,math,secrets
from typing import List,Any,Iterable,Tuple
from queue import Queue
import pandas as pd
from scripts.setup_db import server_connect,server_disconnect,reset_db
from psycopg2.extensions import connection as Connection
from inspect import iscoroutinefunction as is_async

CONST=11000
TEST_FILES=["customers-100000","leads-100000","organizations-100000","products-100000"]
URLS=["https://randomuser.me/api/","https://dog.ceo/api/breeds/image/random","https://pokeapi.co",
      "https://randomuser.me/api/?results=1000","https://jsonplaceholder.typicode.com/posts"]


def timer(func):
    if not is_async(func):
        def sync_wrapper(*args,**kwargs):
            start=time.perf_counter()
            result=func(*args,**kwargs)
            end=time.perf_counter()
            sync_wrapper.times.append(end-start)#type:ignore
            return result
        sync_wrapper.times=[] #type:ignore
        sync_wrapper.__name__=func.__name__
        return sync_wrapper
    else:
        async def async_wrapper(*args,**kwargs):
            start=time.perf_counter()
            result=await func(*args,**kwargs)
            end=time.perf_counter()
            async_wrapper.times.append(end-start)#type:ignore
            return result
        async_wrapper.times=[] #type:ignore
        async_wrapper.__name__=func.__name__
        return async_wrapper


def clear_dataset()->None:
    os.system("rm datasets/*test*")

#########################################
############# IO-BOUND TESTS ############
#########################################

@timer
def read_csvs(files:List[str],results_q:Queue)->None:
    success:int=0
    error:int=0
    for i in range(CONST*4):
        try:
            for file in files:
                with open(f'datasets/{file}.csv') as f:
                    reader=csv.reader(f)
                    success+=1
        except Exception as _ex:
            error+=1
    results_q.put((success,error))

async def read_csvs_async(files:List[str],results_q:Queue)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,read_csvs,files,results_q)

@timer
def write_csvs(files:List[str],results_q:Queue)->None:
    success:int=0
    error:int=0
    for file in files:
        df=pd.read_csv(f'datasets/{file}.csv')
        for i in range(CONST//6000):
            try:
                df.to_csv(f'datasets/{file}_test.csv')
                os.system(f"rm -f datasets/{file}_test.csv")
                success+=1
            except:
                error+=1
    results_q.put((success,error))

async def write_csvs_async(files:List[str],results_q:Queue)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,write_csvs,files,results_q)

@timer
def query_execution(query:str,time_queue:Queue)->None:
    connection:Connection|None=server_connect()
    try:
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
        time_queue.put((1,0))
    except:
        time_queue.put((0,1))
    finally:
        server_disconnect(connection)

async def query_execution_async(query:str,time_queue:Queue)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,query_execution,query,time_queue)

@timer
def fetch_urls(time_queue:Queue,number:int=5):
    success=0
    error=0
    for i in range(number):
        for url in URLS:
            try:
                resp = requests.get(url)
                success+=1
            except:
                error+=1
    time_queue.put((success,error))

async def fetch_urls_async(time_queue:Queue,number:int=5):
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,fetch_urls,time_queue,number)

########################################
############# OS-BOUND TESTS ###########
########################################

@timer
def copy_data(time_queue:Queue,count:int=500000):
    try:
        os.system(f"dd if=/dev/urandom of=test.txt count={count} bs=1")
        os.system("rm -f text.txt")
        clear_dataset()
        time_queue.put((1,0))
    except:
        time_queue.put((0,1))

async def copy_data_async(time_queue:Queue,count:int=2000000):
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,copy_data,time_queue,count)

@timer
def tar_files(time_queue:Queue,number:int=20):
    success=0
    error=0
    cipher=secrets.token_urlsafe(8)
    os.system(f'mkdir datasets/"{cipher}" && cp *.csv datasets/"{cipher}"')
    for i in range(number):
        try:
            os.system(f"""tar -czf datasets/"{cipher}"/archive.tar.gz datasets/"{cipher}"/* && 
                      tar -xzf datasets/"{cipher}"/archive.tar.gz && rm -rf datasets/"{cipher}" """)
            success+=1
        except:
            error+=1
        time_queue.put((success,error))
        os.system(f'rm -rf datasets/"{cipher}"')

    
async def tar_files_async(time_queue:Queue,number:int=1):
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,tar_files,time_queue,number)

@timer
def gz_files(time_queue:Queue,number:int=10):
    success=0
    error=0
    cipher=secrets.token_urlsafe(8)
    os.system(f'mkdir datasets/"{cipher}" && cp *.csv datasets/"{cipher}"')
    for i in range(number):
        try:
            os.system(f'gzip -f datasets/"{cipher}"/*.csv')
            os.system(f'gunzip datasets/"{cipher}"/*')
            os.system(f'rm -rf datasets/"{cipher}"')
            success+=1
        except:
            error+=1
        time_queue.put((success,error))
    os.system(f'rm -rf datasets/"{cipher}"')

async def gz_files_async(time_queue:Queue,number:int=1):
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,gz_files,time_queue,number)

#########################################
############# CPU-BOUND TESTS ###########
#########################################

@timer
def loop_sqrt(time_queue:Queue)->None:
    success=0
    error=0
    try:
        for itr in range(int((CONST**2)/2)):
            _=math.sqrt(itr) 
        success+=1
    except:
        error+=1
    return time_queue.put((success,error))

async def loop_sqrt_async(time_queue:Queue)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,loop_sqrt,time_queue)

@timer
def loop_pow(time_queue:Queue):
    success=0
    error=0
    try:
        for itr in range(CONST//3):
            _=itr**itr
        success+=1
    except:
        error+=1
    return time_queue.put((success,error))

async def loop_pow_async(time_queue:Queue)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,loop_pow,time_queue)

@timer
def loop_mult(time_queue:Queue):
    success=0
    error=0
    try:
        for itr in range(CONST**2):
            _=itr*itr
        success+=1
    except:
        error+=1
    return time_queue.put((success,error))

async def loop_mult_async(time_queue:Queue)->None:
    loop=asyncio.get_running_loop()
    await loop.run_in_executor(None,loop_mult,time_queue)

