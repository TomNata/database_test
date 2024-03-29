import time


def time_of_function(function):
    def wrapped(*args, **kwargs):
        start_time = time.perf_counter()
        res = function(*args, **kwargs)
        elapsed = time.perf_counter() - start_time
        print(f'Время выполнения: {elapsed:0.4}')
        return res
    return wrapped
