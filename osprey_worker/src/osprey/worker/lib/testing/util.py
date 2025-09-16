from time import time

from gevent import sleep


def wait_for_condition(condition, max_wait=2):
    start_time = time()
    while not condition():
        if time() - start_time > max_wait:
            raise Exception('Timed out after %ss.' % max_wait)
        sleep(0.001)
