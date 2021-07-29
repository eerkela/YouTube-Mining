import functools
import os
import time
from concurrent.futures import ThreadPoolExecutor

def add_to_executor(_func=None, *, executor=None):
    """Submit function/method to given executor at runtime.  Works with or
    without arguments, but, if used without arguments on a method, requires
    the method to be encapsulated in a class which has a valid .executor field
    to submit to.  The decorator finds this field lazily, avoiding syntax
    errors at compile time.

    :param executor: executor (concurrent.futures) to submit to. Keyword-only
    :raises AttributeError: if a valid executor could not be found
    :returns: future object corresponding to given function/method call
    """
    def function_decorator(func):
        """decorator for naked functions not contained within an
        encapsulating class
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return executor.submit(func, *args, **kwargs)
        return wrapper

    def method_decorator(func):
        """decorator for methods inside a class that has an assigned
        executor field
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return args[0].executor.submit(func, *args, *kwargs)
        return wrapper

    if _func is None:
        if executor is None:
            return method_decorator
        return function_decorator
    else:
        if executor is None:
            return method_decorator(_func)
        return function_decorator(_func)

def debug(func):
    """Print the function signature and return value upon execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        print(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        print(f"{func.__name__!r} returned {value!r}")
        return value
    return wrapper

def slow_down(_func=None, *, rate=1):
    """
    Execute function at no faster than the specified rate in Hz

    :param rate: rate of execution (in Hz, # of executions per second)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(1 / rate)
            return func(*args, **kwargs)
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)

def timer(func):
    """Print the elapsed time of a function to console upon execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        value = func(*args, **kwargs)
        end = time.perf_counter()
        elapsed = end - start
        print(f"Finished {func.__name__!r} in {elapsed:.4f} seconds")
        return value
    return wrapper


if __name__ == "__main__":

    # Examples:

    @slow_down(rate=1)
    def countdown(n):
        if n < 1:
            print("Done!")
        else:
            print(n)
            countdown(n - 1)

    @timer
    def waste_time(n, num_iterations=10000):
        for _ in range(n):
            sum([i**2 for i in range(num_iterations)])

    @debug
    def approximate_e(terms=18):
        import math
        return sum(1 / math.factorial(n) for n in range(terms))

    def test_add_to_executor():
        exec = ThreadPoolExecutor(max_workers=2)
        @add_to_executor(executor=exec)
        def countdown_async(n):
            countdown(n)

        for _ in range(4):
            countdown_async(5)
        print("Doing stuff in main thread...")

    def test_add_to_executor_in_classes():
        # https://stackoverflow.com/questions/11731136/class-method-decorator-with-self-arguments
        class test:
            executor = ThreadPoolExecutor(max_workers=2)
            @add_to_executor
            def countdown_async(self, n):
                countdown(n)

        t = test()
        for _ in range(4):
            t.countdown_async(5)
        print("Doing stuff in main thread...")

    @functools.lru_cache()
    def fibonacci(num):
        print(f"Calculating fibonacci({num})")
        if num < 2:
            return num
        return fibonacci(num - 1) + fibonacci(num - 2)

    test_add_to_executor_in_classes()
