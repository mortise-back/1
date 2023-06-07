import heapq
import threading
import time
import queue

class TaskState:
    Pending = 0
    Queued = 1
    Running = 2
    Done = 3
    Failed = 4


class Task:
    def __init__(self, func, args=(), kwargs={}, priority=0, timeout=None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.priority = priority

        self.done = False
        self.result = None
        self.exception = None

        self.timeout = timeout

        if self.timeout:
            self.timer = threading.Timer(self.timeout, self._handle_timeout)
            self.timer.daemon = True

    def _handle_timeout(self):
        self.exception = TimeoutError("任务执行超时")
        self.done = True

    def run(self):
        try:
            self.result = self.func(*self.args, **self.kwargs)
        except Exception as e:
            self.exception = e
        finally:
            self.done = True

            if self.timeout and self.timer:
                self.timer.cancel()

    def get(self):
        if self.exception:
            raise self.exception
        elif not self.done:
            raise RuntimeError("请在任务完成后调用 get 方法")
        else:
            return self.result

    def pause(self):
        if self.pause_signal:
            self.pause_signal.wait()

    def execute(self):
        self.state = TaskState.Running
        try:
            self.result = self.func(*self.args, **self.kwargs)
            self.state = TaskState.Done
        except Exception as e:
            self.exception = e
            self.state = TaskState.Failed
        self.is_completed = True


    def _execute_async(self, *args, **kwargs):
        def target(*args, **kwargs):
            try:
                self.result = self.func(*args, **kwargs)
            except Exception as e:
                self.exception = e
        thread = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        thread.start()

    def _execute_sync(self, *args, **kwargs):
        if self.timeout is None:
            self.result = self.func(*args, **kwargs)
        else:
            self.result = self._execute_with_timeout(self.timeout, self.func, *args, **kwargs)

    def _execute_with_timeout(self, timeout, func, *args, **kwargs):
        def target(*args, **kwargs):
            try:
                self.result = func(*args, **kwargs)
            except Exception as e:
                self.exception = e
        thread = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
        thread.start()
        thread.join(timeout=timeout)
        if thread.is_alive():
            raise TimeoutError("Task {} timed out after {} seconds".format(self.func.__name__, timeout))
        elif self.exception is not None:
            raise self.exception
        else:
            return self.result
    def __lt__(self, other):
        return self.priority < other.priority


class TaskQueue:
    """任务队列"""

    def __init__(self):
        self.queue = []

    def push(self, task):
        """添加任务"""
        heapq.heappush(self.queue, (-task.priority, task))

    def pop(self):
        """获取优先级最高的任务"""
        if self.queue:
            return heapq.heappop(self.queue)[1]

    def remove(self, task):
        """移除任务"""
        try:
            self.queue.remove((-task.priority, task))
            heapq.heapify(self.queue)
        except ValueError:
            pass

class TaskQueueManager:
    """任务队列管理器"""

    def __init__(self):
        self.task_queue = TaskQueue()  # 待执行的任务队列
        self.completed_tasks = []  # 已完成的任务

    def add_task(self, task):
        """添加任务"""
        self.task_queue.push(task)
        self.prioritize_tasks()

    def remove_task(self, task):
        """移除任务"""
        self.task_queue.remove(task)

    def prioritize_tasks(self):
        """根据优先级重新排序所有任务"""
        tasks = self.task_queue.queue[:]
        self.task_queue.queue.clear()
        for task in tasks:
            self.task_queue.push(task[1])

    def get_next_task(self):
        """获取下一个优先级最高的待执行任务"""
        return self.task_queue.pop()

    def mark_task_completed(self, task):
        """标记任务为已完成"""
        self.completed_tasks.append(task)

class MultiThreadTaskExecution:
    """多线程任务执行"""

    def __init__(self, queue_manager, num_threads=1):
        self.queue_manager = queue_manager  # 任务队列管理器
        self.num_threads = num_threads  # 线程数
        self.threads = []  # 线程列表
        self.stop_event = threading.Event()  # 停止事件

    def start(self):
        """开始执行任务"""
        print("开始执行任务")
        self.stop_event.clear()  # 清除停止事件
        for i in range(self.num_threads):
            thread = threading.Thread(target=self._do_work)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        """停止执行任务"""
        self.stop_event.set()  # 设置停止事件

    def wait(self):
        """等待所有任务执行完成"""
        for thread in self.threads:
            thread.join()

    def _do_work(self):
        """执行任务的线程函数"""
        while not self.stop_event.is_set():
            task = self.queue_manager.get_next_task()
            if task is None:
                # 如果当前没有待执行的任务，则等待一段时间
                self.stop_event.wait(timeout=0.1)
                continue

            task.execute()
            self.queue_manager.mark_task_completed(task)

class TaskFactory:
    """任务工厂"""

    def __init__(self, task_manager):
        self.task_manager = task_manager

    def create_task(self, func, args=None, kwargs=None, priority=0):
        """根据函数、参数和优先级生成任务"""
        args = args or []
        kwargs = kwargs or {}
        task = Task(func, args=args, kwargs=kwargs, priority=priority)
        self.task_manager.add_task(task)
        return task

class CustomTaskManager:
    """任务管理和执行系统"""

    def __init__(self, num_threads=1):
        self.task_manager = TaskQueueManager()  # 任务队列管理器
        self.multi_thread_execution = MultiThreadTaskExecution(
            self.task_manager, num_threads=num_threads)  # 多线程任务执行
        self.task_factory = TaskFactory(self.task_manager)  # 任务工厂

    def start(self):
        """开始执行任务"""
        self.multi_thread_execution.start()

    def stop(self):
        """停止执行任务"""
        self.multi_thread_execution.stop()

    def add_task(self, func, args=None, kwargs=None, priority=0):
        """添加任务"""
        self.task_factory.create_task(func, args=args, kwargs=kwargs, priority=priority)

    def wait(self):
        """等待所有任务执行完成"""
        self.multi_thread_execution.wait()







class ThreadPool:
    """线程池"""

    def __init__(self, num_threads=1, max_queue_size=None):
        self._task_queue = []
        self._threads = []
        self._lock = threading.Lock()
        self._stopped = False
        self._results = []
        self._running_tasks = []
        self._num_threads = num_threads
        self._default_priority = 1
        self._pause_signal = threading.Event()

        for i in range(self._num_threads):
            thread = threading.Thread(target=self._worker, daemon=True)
            self._threads.append(thread)

    def start(self):

        for thread in self._threads:
            thread.start()

    def stop(self):
        self._stopped = True
        self._pause_signal.set()
        self.join()

    def pause(self):
        self._pause_signal.set()

    def resume(self):
        self._pause_signal.clear()

    def add_task(self, task):
        with self._lock:
            found = False
            for t in self._running_tasks + self._task_queue:
                if t.func == task.func and t.args == task.args and t.kwargs == task.kwargs:
                    found = True
                    break
            if not found:
                self._task_queue.append(task)

    def set_task_priority(self, func, priority):
        """设置任务优先级"""
        with self._lock:
            for task in self._running_tasks + self._task_queue:
                if task.func == func:
                    task.priority = priority
                    return True
            return False

    def set_task_async(self, func):
        """将任务切换为异步执行模式"""
        with self._lock:
            for task in self._running_tasks + self._task_queue:
                if task.func == func and not task.async_mode:
                    task.async_mode = True
                    return True
            return False

    def set_task_sync(self, func):
        """将一个任务设置为同步执行"""
        print(f"尝试将异步任务 {func} 转换为同步任务...")

        num_async_tasks = sum(1 for task in self._running_tasks if task.async_mode) + len(self._task_queue)

        if num_async_tasks == 0:
            print("任务队列中没有任何异步任务，无法切换为同步执行")
            return
        found = False
        for task in self._task_queue:
            if (task.func == func) and task.async_mode:
                found = True
                if not task.is_completed:
                    while not task.is_completed:
                        time.sleep(0.1)

                self._task_queue.remove(task)
                task.async_mode = False
                task.priority = self._default_priority
                self._task_queue.append(task)
                print(f"已将异步任务 {func} 转换为同步任务")
                break
        if not found:
            for task in self._running_tasks:
                if (task.func == func) and task.async_mode:
                    found = True
                    if not task.is_completed:
                        while not task.is_completed:
                            time.sleep(0.1)

                    task.async_mode = False
                    task.priority = self._default_priority
                    self._task_queue.append(task)
                    print(f"已将异步任务 {func} 转换为同步任务")
                    break
        if not found:
            raise ValueError(f"Cannot find async task with function {func}")

    def get_pending_tasks(self):
        return self._task_queue

    def get_running_tasks(self):
        return self._running_tasks

    def join(self, timeout=None):
        for thread in self._threads:
            thread.join(timeout=timeout)

    def get_done_tasks(self):
        """获取已完成的任务列表"""
        with self._lock:
            return [task for task in self._results if task.state == TaskState.Done]

    def get_failed_tasks(self):
        """获取已失败的任务列表"""
        with self._lock:
            return [task for task in self._results if task.state == TaskState.Failed]

    def get_task_priority(self, func):
        """获取任务优先级"""
        with self._lock:
            for task in self._running_tasks + self._task_queue:
                if task.func == func:
                    return task.priority
            return None

    def get_task_result(self, func):
        """获取任务结果"""
        with self._lock:
            for task in self._results:
                if task.func == func:
                    return task.result, task.exception
            return None, None

    def get_task_return_values(self):
        """获取所有任务返回结果"""
        with self._lock:
            return [(task.result, task.exception) for task in self._results]

    def set_default_priority(self, priority):
        """设置默认任务优先级"""
        self._default_priority = priority

    def _worker(self):
        while not self._stopped:
            try:
                priority, task = self._get_next_task()
            except queue.Empty:
                continue
            with self._lock:
                task.pause_signal = self._pause_signal
                task.state = TaskState.Running
                self._running_tasks.append(task)
            self._execute_task(task)
            with self._lock:
                self._running_tasks.remove(task)
                self._results.append(task)

    def _execute_task(self, task):
        try:
            task.execute()
        except Exception as e:
            task.exception = e
            task.state = TaskState.Failed
        else:
            task.state = TaskState.Done

    def _get_next_task(self):
        with self._lock:
            if not self._task_queue:
                raise queue.Empty
            highest_priority = max(task.priority for task in self._task_queue)
            for ix, task in enumerate(self._task_queue):
                if task.priority == highest_priority:
                    return ix, task
            assert False, 'unreachable'

    def __len__(self):
        """获取当前队列中任务数量"""
        with self._lock:
            return len(self._task_queue)

    def __enter__(self):
        """启用上下文管理器"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""
        self.stop()
        return False

    def __del__(self):
        """删除线程池对象时自动停止线程池"""
        self.stop()

    def _cleanup(self):
        """清理已完成的任务"""
        with self._lock:
            still_running = []
            for task in self._running_tasks:
                if task.state == TaskState.Running:
                    still_running.append(task)
                else:
                    self._results.append(task)
            self._running_tasks = still_running

            # 清理已完成的任务队列
            completed_tasks = [task for task in self._task_queue if task.is_completed]
            for task in completed_tasks:
                self._task_queue.remove(task)

    def wait_completion(self, timeout=None):
        """等待所有任务完成"""
        start_time = time.monotonic()

        while not self._stopped:
            with self._lock:
                if not self._task_queue and not self._running_tasks:
                    break

            elapsed = time.monotonic() - start_time
            if timeout and elapsed > timeout:
                break

            time.sleep(0.1)

        self.stop()

    def map(self, func, iterable, priority=None):
        """高级功能，对可迭代对象的所有元素执行特定函数"""
        tasks = [Task(func, args=(x,), priority=priority) for x in iterable]

        for task in tasks:
            self.add_task(task)

        self.wait_completion()

        return [task.result for task in tasks]

    def starmap(self, func, iterable, priority=None):
        """高级功能，对可迭代对象的所有元素执行特定函数（支持可迭代的参数）"""
        tasks = [Task(func, args=x, priority=priority) for x in iterable]

        for task in tasks:
            self.add_task(task)

        self.wait_completion()

        return [task.result for task in tasks]

    def imap(self, func, iterable, priority=None):
        """高级功能，返回迭代器"""
        tasks = [Task(func, args=(x,), priority=priority) for x in iterable]

        for task in tasks:
            self.add_task(task)

        def generate_results():
            for result in [task.result for task in tasks]:
                yield result

        return generate_results()

    def apply_async(self, func, args=None, kwargs=None, priority=None):
        """异步执行单个任务"""
        task = Task(func, args=args, kwargs=kwargs, priority=priority)
        self.add_task(task)

    def close(self):
        self.wait_completion()

    def __call__(self, func, *args, **kwargs):
        self.add_task(Task(func, args=args, kwargs=kwargs))

    def __repr__(self):
        return f"<ThreadPool(num_threads={self._num_threads}, size={len(self)})>"

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        """获取当前队列中任务数量"""
        with self._lock:
            return len(self._task_queue) + len(self._running_tasks)

    def __bool__(self):
        """检查线程池是否正在运行"""
        try:
            with self._lock:
                return not self._stopped and (len(self._task_queue) > 0 or len(self._running_tasks) > 0)
        except:
            return False

    def __del__(self):
        """删除线程池对象时自动停止线程池"""
        self.stop()


