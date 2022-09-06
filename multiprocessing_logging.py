import os
import logging

_ = """
This is the logging functions removed from the base example file in the 4th version of the script. 

the log_listener_configurer function was changed to move the log_dir and log_name values to parameters rather than just
referencing the global variables

the log_listener_process also had to be updated to accept the extra parameters that were required in order to change the
configurer to not reference the global variables
"""


def log_listener_configurer(log_dir, log_name):
    """
    From https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    """
    root = logging.getLogger(log_name)
    h = logging.FileHandler(os.path.join(log_dir, f'{log_name}.log'), encoding='utf-8')
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)


def log_listener_process(log_queue, log_configurer_func, log_dir, log_name):
    """
    From https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    :param log_queue: the multiprocessing queue
    :param log_configurer_func: the log_listener_configurer function (above)
    """
    log_configurer_func(log_dir, log_name)
    while True:
        try:
            record = log_queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import sys, traceback
            print('Whoops! Logging Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


def worker_log_configurer(log_queue):
    """
    From https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    :param log_queue: the log_queue
    """
    h = logging.handlers.QueueHandler(log_queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.INFO)
