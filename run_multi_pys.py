"""
同时执行多项任务:

"""

import os

from concurrent.futures import as_completed, ThreadPoolExecutor

Thread_Pool = ThreadPoolExecutor(5)
BASE_PATH = os.path.dirname(__file__)


# 实现队列
class Queue:
    def __init__(self):
        self.items = []

    def enqueue(self, item):
        self.items.append(item)

    def dequeue(self):
        return self.items.pop(0)

    def empty(self):
        return self.size() == 0

    def size(self):
        return len(self.items)


def run_py():
    if queue.size() == 0:
        return
    py_name = queue.dequeue()
    py_name = os.path.join(BASE_PATH, py_name)
    py_name = py_name.replace('\\', '/')
    print(py_name)
    with open(py_name, 'r', encoding='utf-8') as f:
        exec(f.read())


# 多线程执行test1.py和test2.py
def process_read_file():
    global queue
    # 日常运行脚本
    run_pys = ['store_five_files.py', 'price_dist/price_dist.py', 'search_sku_asin/update_st_info.py',
               'daily_tasks','api_request_files/api_request_five_reports.py','api_request_files/api_request_chujin_reports.py','calc_station_st_and_sku']
    queue = Queue()
    [queue.enqueue(py) for py in run_pys]
    while 1:
        all_task = []
        for one_page in range(len(run_pys)):
            all_task.append(Thread_Pool.submit(run_py))
        for future in as_completed(all_task):
            future.result()
        if queue.size() == 0:
            break


if __name__ == "__main__":
    process_read_file()
