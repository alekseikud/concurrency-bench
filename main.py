import scripts.tests as tests
import asyncio
from multiprocessing import Process,Manager
from threading import Thread
from typing import Tuple
from queue import Queue


def _csv_process_worker(func: str,files: list[str],results_q: Queue) -> None:
    """
    Top‑level worker: safe to pickle.
    Calls the right tests.* function and puts its return into results_q.
    """
    if func == "read":
        tests.read_csvs(files,results_q)
    else:
        tests.write_csvs(files,results_q)

@tests.timer
def csv_threading_test(func="read",thread_number:int=2)->tuple[int,int]:
    if func!="read" and func!="write":
        raise ValueError("Function csv_threading_test() cannot have such function argument")
    if func=="read":
        tests.read_csvs.best = None #type:ignore
        tests.read_csvs.worst = None #type:ignore
    else:
        tests.write_csvs.best = None #type:ignore
        tests.write_csvs.worst = None #type:ignore
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
        tests.read_csvs.best = None
        tests.read_csvs.worst = None
    else:
        tests.write_csvs.best = None
        tests.write_csvs.worst = None
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


def csv_async_test(func="read",thread_number:int=2)->tuple[int,int]:
    if func!="read" and func!="write":
        raise ValueError(f"Function csv_processing_test() cannot have such function argument")
    if func=="read":
        tests.read_csvs.best = None
        tests.read_csvs.worst = None
    else:
        tests.write_csvs.best = None
        tests.write_csvs.worst = None
    success=0
    error=0
    results_q = Manager().Queue()
    for i in range(thread_number):
        if func=="read":
            asyncio.run(tests.read_csvs_async(tests.TEST_FILES,results_q))
        else:
            asyncio.run(tests.write_csvs_async(tests.TEST_FILES,results_q))
    while not results_q.empty():
        s,e=results_q.get()
        success+=s
        error+=e
    return (success,error)
if __name__ == "__main__":
    print(csv_async_test("read",4))
    print(tests.read_csvs.times)
    print(csv_async_test("write",4))
    print(tests.write_csvs.times)