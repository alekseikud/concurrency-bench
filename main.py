import scripts.tests as tests
import asyncio
from multiprocessing import Process,Manager
from threading import Thread
from typing import Tuple
from queue import Queue

#Top‑level worker: safe to pickle.
#Calls the right tests.* function and puts its return into results_q.
def _csv_process_worker(func: str,files: list[str],results_q: Queue) -> None:

    if func == "read":
        tests.read_csvs(files,results_q)
    else:
        tests.write_csvs(files,results_q)

def _query_execution_worker(query:str,time_queue:Queue)->None:
    tests.query_execution(query,time_queue)

def _copy_data_worker(time_queue:Queue):
    tests.copy_data(time_queue)

def _tar_files_worker(time_queue:Queue):
    tests.tar_files(time_queue)

def _gz_files_worker(time_queue:Queue):
    tests.gz_files(time_queue)

def _fetch_urls_worker(time_queue):
    tests.fetch_urls(time_queue)

def _loop_sqrt_worker(time_queue):
    tests.loop_sqrt(time_queue)

def _loop_mult_worker(time_queue):
    tests.loop_mult(time_queue)

def _loop_pow_worker(time_queue):
    tests.loop_pow(time_queue)

@tests.timer
def csv_threading_test(func="read",thread_number:int=2)->tuple[int,int]:
    if func!="read" and func!="write":
        raise ValueError("Function csv_threading_test() cannot have such function argument")
    if func=="read":
        tests.read_csvs.times= []
    else:
        tests.write_csvs.times= []
    threads:list[Thread]=[]
    success=0
    error=0
    results_q = Queue()
    for i in range(thread_number):
        if func=="read":
            thread=Thread(target=tests.read_csvs,args=(tests.TEST_FILES,results_q))
        else:
            thread=Thread(target=tests.write_csvs,args=(tests.TEST_FILES,results_q))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not results_q.empty():
        s,e=results_q.get()
        success+=s
        error+=e
    return (success,error)

@tests.timer
def csv_processing_test(func="read",thread_number:int=2)->tuple[int,int]:
    if func!="read" and func!="write":
        raise ValueError(f"Function csv_processing_test() cannot have such function argument")
    if func=="read":
        tests.read_csvs.times= []
    else:
        tests.write_csvs.times= []
    processes:list[Process]=[]
    success=0
    error=0
    results_q = Manager().Queue()
    for i in range(thread_number):
        process=Process(target=_csv_process_worker,args=(func,tests.TEST_FILES,results_q))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not results_q.empty():
        s,e=results_q.get()
        success+=s
        error+=e
    return (success,error)


@tests.timer
async def csv_async_test(func="read",thread_number:int=2)->tuple[int,int]:
    if func!="read" and func!="write":
        raise ValueError(f"Function csv_processing_test() cannot have such function argument")
    if func=="read":
        tests.read_csvs.times= []
    else:
        tests.write_csvs.times= []
    success=0
    error=0
    tasks=[]
    results_q = Manager().Queue()

    async def worker(func,results_q):
        if func=="read":
            await tests.read_csvs_async(tests.TEST_FILES,results_q)
        else:
            await tests.write_csvs_async(tests.TEST_FILES,results_q)

    for i in range(thread_number):
        task=asyncio.create_task(worker(func,results_q))
        tasks.append(task)

    await asyncio.gather(*tasks)

    while not results_q.empty():
        s,e=results_q.get()
        success+=s
        error+=e
    return (success,error)


@tests.timer
def query_threading_test(number=2)->tuple[int,int]:
    tests.query_execution.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.query_execution,
                      args=(f"SELECT pg_sleep({6})",time_queue))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)
    

@tests.timer
def query_processing_test(number=2)->tuple[int,int]:
    tests.query_execution.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_query_execution_worker,
                      args=(f"SELECT pg_sleep({6/number})",time_queue))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def query_async_test(number=2)->tuple[int,int]:
    tests.query_execution.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker(query:str,time_queue:Queue):
        await tests.query_execution_async(f"SELECT pg_sleep({6})",time_queue)
    for i in range(number):
        task=asyncio.create_task(worker(f"SELECT pg_sleep({6})",time_queue))
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def copy_data_threading_test(number=2)->tuple[int,int]:
    tests.copy_data.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.copy_data,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def copy_data_processing_test(number=2)->tuple[int,int]:
    tests.copy_data.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_copy_data_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def copy_data_async_test(number=2)->tuple[int,int]:
    tests.copy_data.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker(time_queue):
        await tests.copy_data_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker(time_queue))
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def tar_files_threading_test(number=2)->tuple[int,int]:
    tests.tar_files.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.tar_files,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def tar_files_processing_test(number=2)->tuple[int,int]:
    tests.tar_files.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_tar_files_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def tar_files_async_test(number=2)->tuple[int,int]:
    tests.tar_files.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker():
        await tests.tar_files_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker())
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def gz_files_threading_test(number=2)->tuple[int,int]:
    tests.gz_files.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.gz_files,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def gz_files_processing_test(number=2)->tuple[int,int]:
    tests.gz_files.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_gz_files_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def gz_files_async_test(number=2)->tuple[int,int]:
    tests.tar_files.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker():
        await tests.gz_files_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker())
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def fetch_urls_threading_test(number=2)->tuple[int,int]:
    tests.fetch_urls.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.fetch_urls,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def fetch_urls_processing_test(number=2)->tuple[int,int]:
    tests.fetch_urls.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_fetch_urls_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def fetch_urls_async_test(number=2)->tuple[int,int]:
    tests.fetch_urls.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker():
        await tests.fetch_urls_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker())
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def loop_sqrt_threading_test(number=2)->tuple[int,int]:
    tests.loop_sqrt.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.loop_sqrt,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def loop_sqrt_processing_test(number=2)->tuple[int,int]:
    tests.loop_sqrt.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_loop_sqrt_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def loop_sqrt_async_test(number=2)->tuple[int,int]:
    tests.loop_sqrt.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker():
        await tests.loop_sqrt_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker())
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def loop_pow_threading_test(number=2)->tuple[int,int]:
    tests.loop_pow.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.loop_pow,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def loop_pow_processing_test(number=2)->tuple[int,int]:
    tests.loop_pow.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_loop_pow_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def loop_pow_async_test(number=2)->tuple[int,int]:
    tests.loop_pow.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker():
        await tests.loop_pow_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker())
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def loop_mult_threading_test(number=2)->tuple[int,int]:
    tests.loop_mult.times= []
    threads=[]
    succcess=0
    error=0
    time_queue=Queue()
    for i in range(number):
        thread=Thread(target=tests.loop_mult,args=(time_queue,))
        threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
def loop_mult_processing_test(number=2)->tuple[int,int]:
    tests.loop_mult.times= []
    processes=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    for i in range(number):
        process=Process(target=_loop_mult_worker,args=(time_queue,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)

@tests.timer
async def loop_mult_async_test(number=2)->tuple[int,int]:
    tests.loop_mult.times= []
    tasks=[]
    succcess=0
    error=0
    time_queue=Manager().Queue()
    async def worker():
        await tests.loop_mult_async(time_queue)

    for i in range(number):
        task=asyncio.create_task(worker())
        tasks.append(task)

    await asyncio.gather(*tasks)
    while not time_queue.empty():
        s,e=time_queue.get()
        succcess+=s
        error+=e
    return (succcess,error)


if __name__ == "__main__":
    print(loop_mult_threading_test())
    print(loop_mult_threading_test.times)
    print(tests.loop_mult.times)
    # print(asyncio.run(loop_mult_async_test()))
    # print(loop_mult_async_test.times)
    # print(tests.loop_mult.times)