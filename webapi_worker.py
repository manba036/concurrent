import concurrent.futures
import threading
import time


lock_get_or_release_worker_index = threading.Lock()


class WebapiWorker:
    def __init__(self, num_of_workers):
        self.num_of_workers = num_of_workers
        self.worker_is_ready_list = [ True for _ in range(self.num_of_workers) ]


    def _get_or_release_worker_index(self, worker_index=None):
        lock_get_or_release_worker_index.acquire()

        if worker_index is None:
            for index, worker_is_ready in enumerate(self.worker_is_ready_list):
                if worker_is_ready:
                    worker_index = index
                    self.worker_is_ready_list[worker_index] = False
                    break
        else:
            if self.worker_is_ready_list[worker_index]:
                print(f'# [WARNING] Worker{worker_index} is already ready before release')
            self.worker_is_ready_list[worker_index] = True

        lock_get_or_release_worker_index.release()
        return worker_index


    def _process_task(self, task_function, settings, task_index, task_info):
        processing_time = 0.0
        worker_index = self._get_or_release_worker_index()
        if worker_index is not None:
            print(f'# [{task_index}][{worker_index}] START')
            start_time = time.time()
            result = task_function(settings, task_index, task_info, worker_index)
            end_time = time.time()
            processing_time = end_time - start_time
            print(f'# [{task_index}][{worker_index}] STOP : {processing_time:.2f} sec.')

            self._get_or_release_worker_index(worker_index)
        else:
            print(f'# [{task_index}][{worker_index}] ERROR: Each worker is not ready')

        return {
            'task_index': task_index,
            'task_info': task_info,
            'worker_index': worker_index,
            'result': result,
            'processing_time': processing_time,
        }


    def process_tasks(self, task_function, settings, task_info_list):
        print('# <<< Start >>>')

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_of_workers) as executor:
            futures = [executor.submit(self._process_task, task_function, settings, task_index, task_info) for task_index, task_info in enumerate(task_info_list)]

            # Waiting for all tasks to complete
            executor.shutdown(wait=True)

            # Get all task results
            results = [future.result() for future in futures]

        end_time = time.time()
        total_processing_time = end_time - start_time

        print('# <<< All tasks completed >>>')
        print(f'# Total processing time: {total_processing_time:.2f} s')

        return {
            'results': results,
            'total_processing_time': total_processing_time,
        }
