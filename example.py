#!/usr/bin/env python3
import os
import logging
import logging.handlers
import argparse
import multiprocessing
from random import random
from time import sleep
from queue import Empty

cwd = os.path.dirname(__file__)
LOG_DIR = os.path.join(cwd, "logging")
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)
LOG_NAME = "producer_consumer_example"


def log_listener_configurer():
    """
    From https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    """
    root = logging.getLogger(LOG_NAME)
    h = logging.FileHandler(os.path.join(LOG_DIR, f'{LOG_NAME}.log'), encoding='utf-8')
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)


def log_listener_process(log_queue, log_configurer_func):
    """
    From https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    :param log_queue:
    :param log_configurer_func:
    :return:
    """
    log_configurer_func()
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
    :param log_queue:
    :return:
    """
    h = logging.handlers.QueueHandler(log_queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.INFO)


def producer_func(processing_queue, log_queue, logging_configurer, consumer_count):
    # Set up the logging for the producer
    logging_configurer(log_queue)
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.INFO)

    logger.info('Producer Running')
    # Do the production of the data to queue - simulate the Extract & Transform of my ETL process
    for i in range(25):
        # Simulate some random amount of processing
        sleep(random())
        # add some value to the queue
        processing_queue.put(i)
    # Once all the producing is completed, I need to signal to the consumer(s) to end (each needs a signal)
    for _ in range(consumer_count):
        processing_queue.put(None)
    logger.info('Producer Done')


def consumer_func(processing_queue, log_queue, logging_configurer):
    # Set up logging for each consumer to all use the same log file
    logging_configurer(log_queue)
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.INFO)

    logger.info('Consumer - Consumer Running')
    # Consume from the produced queue - simulate the L part of my ETL pipeline
    while True:
        # Get the next item from my FIFO queue
        try:
            item = processing_queue.get(block=False)
        except Empty:
            logger.info('Nothing to consume now, waiting...')
            sleep(0.5)
            continue
        # Look for the signal from the producer to shut-down the consumer(s)
        if item is None:
            break
        # If we actually got something, we need to do something with it:
        sleep(random()*3)  # The sleep is to simulate some processing being done which takes longer than the producer
        logger.info(f'Consumed value {item}')
    # The consumer process got the shutdown command - is there anything I need to do for clean-up?
    logger.info('Consumer shut down.')


if __name__ == "__main__":
    # Parser Arguments
    parser = argparse.ArgumentParser("Execute the ETL Process")
    parser.add_argument("-c", "--consumers",
                        type=int,
                        help="The number of consumers to create/use",
                        dest='consumer_count',
                        default=2)
    args = parser.parse_args()

    # Create the multiprocessing manager
    manager = multiprocessing.Manager()

    # Set up the logging listener
    log_queue = manager.Queue()
    listener = multiprocessing.Process(target=log_listener_process, args=(log_queue, log_listener_configurer))
    listener.start()

    # Configure logging for the main process
    worker_log_configurer(log_queue)
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.INFO)

    # log the log queue with the log queue
    logger.info(f"Created Log Queue: {str(log_queue)}")

    # Create the processing queue
    # setting maxsize small to demonstrate that producers will wait if the queue gets too big
    processing_queue = manager.Queue(maxsize=3)
    logger.info(f"Created the Processor Queue: {str(processing_queue)}")

    # Start the consumer(s)
    logger.info(f"Starting the {args.consumer_count} consumer(s)")
    consumers = []
    for _ in range(args.consumer_count):
        c = multiprocessing.Process(target=consumer_func,
                                    args=(processing_queue,
                                          log_queue,
                                          worker_log_configurer))
        consumers.append(c)
        c.start()
    # Start the producer
    logger.info("Starting the producer")
    producer_process = multiprocessing.Process(target=producer_func,
                                               args=(processing_queue,
                                                     log_queue,
                                                     worker_log_configurer,
                                                     args.consumer_count))
    producer_process.start()

    # Join Everything
    logger.info("Join Producer")
    producer_process.join()
    logger.info("Join Consumers")
    for c in consumers:
        c.join()
    logger.info("Signal the end of the logging and join")
    log_queue.put_nowait(None)  # Ends the logging Queue
    listener.join()  # Stops the listener
