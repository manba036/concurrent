"""
Example of processing multiple time-consuming tasks simultaneously using multiple workers
"""
import random
import time

from webapi_worker import WebapiWorker


settings = {
    'webapi_url_list': [
        'http://hogehoge:5000',
        'http://hogehoge:5001',
    ],
}


def task_function(settings, task_index, task_info, worker_index):
    url = settings['webapi_url_list'][worker_index]

    sec = random.randint(task_index, 10)
    time.sleep(sec)

    result = (task_index, worker_index, sec, url)
    return result


def main():
    worker = WebapiWorker(len(settings['webapi_url_list']))
    task_info_list = [{'id': x} for x in ['A', 'B', 'C', 'D', 'E', 'F']]

    results = worker.process_tasks(task_function, settings, task_info_list)

    print()
    print('# Results')
    for result in results['results']:
        print(result)

if __name__ == "__main__":
    main()
