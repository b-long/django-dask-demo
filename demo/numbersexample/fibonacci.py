from dask import delayed, compute

@delayed
def fib(n):
    """
    Based on https://distributed.dask.org/en/stable/task-launch.html
    """
    # if n is None:
    #     return 0  # Or any other default value you prefer
    # elif n < 2:
    #     return n
    if n < 2:
        return n
    # We can use dask.delayed and dask.compute to launch
    # computation from within tasks
    a = fib(n - 1)  # these calls are delayed
    b = fib(n - 2)
    a, b = compute(a, b)  # execute both in parallel
    return a + b