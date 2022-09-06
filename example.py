#!/usr/bin/env python3
import os
import signal
import logging
import logging.handlers
import argparse
import multiprocessing
from random import random
from time import sleep
from queue import Empty

from multiprocessing_logging import log_listener_configurer, log_listener_process, worker_log_configurer

changelog = ["""
The first version demonstrates how to build a multiprocessing single-producer / multiple-consumer script
 - it implements single-file logging with a log-listener process that works through a multiprocessing queue of logs
 - it uses a share multiprocessing queue of "work items" which the producers add to and the consumers remove from
""", """
The second version starts with the same multiprocessing single-producer / multiple-consumer script and adds:
 - a signal handler to accept the `kill` (15) signal from bash to gracefully shut down the producer 
 - it also allows the application to gracefully be killed 
   - it stops production on receipt of the signal, and sill writes the end of the queue
   - the consumers do nothing with the `kill` signal, and continue draining the queue until it is complete 
   - everything still exits as normal. 
""", """
The third version builds on the prior multiprocessing single-producer / multiple-consumer script and adds:
 - a modification to the signal handling to allow for different shutdown options: 
    - `kill` (15) will stop the producer
    - KeyboardInterrupt (2) will shut down the consumers and have them write to deadletter the rest of the queue
""", """
The 4th version modifies the logging and moves the functions to a separate file (multiprocessing_logging.py)
"""]

# TODO - 1. move the Logging & Deadletter file paths to a configuration
cwd = os.path.dirname(__file__)
LOG_DIR = os.path.join(cwd, "logging")
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)
LOG_NAME = f"producer_consumer_example"

# Adding a location for my deadletter queue - just using a text file for simplicity
DEADLETTER_QUEUE = os.path.join(cwd, "deadletter_queue.txt")

# Separate global variables for the different shutdown commands/options
STOP_PRODUCTION = False
WRITE_DEADLETTER = False


def signal_handler(signum, frame):
    # Handle different signals differently
    print(f"Received signal number {signum} on process {os.getpid()}", flush=True)
    global STOP_PRODUCTION
    if signum == 15:
        # The `kill` (15) signal will stop the producer from running
        STOP_PRODUCTION = True
    elif signum == 2:
        # We want to stop production (if we haven't already) AND start writing to deadletter
        STOP_PRODUCTION = True
        # KeyboardInterrupt will tell the consumer to write the remaining queue to deadletter
        global WRITE_DEADLETTER
        WRITE_DEADLETTER = True


# Define how we will react to SIGTERM [kill] and SIGINT [KeyboardInterrupt]
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# TODO - 2. make the application work with multiple producers AND multiple consumers
# TODO - 3. make the producer/consumer base functions generic so that they can be generally implemented


def producer_func(processing_queue, log_queue, logging_configurer, consumer_count):
    """
    My Example producer function for testing - "upgrading" it to produce a lot more data, faster
    :param processing_queue: the queue object that the producer is goinng to add items to
    :param log_queue:  the queue object for the logs
    :param logging_configurer: the configuratin function for worker processes (worker_log_configurer)
    :param consumer_count: how many consumer functions are going to be created - based on the arguments passed to main
    """
    # Set up the logging for the producer
    logging_configurer(log_queue)
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.INFO)

    logger.info(f'Producer Running on pid {os.getpid()}')
    # the global variable that is modified by the signal handler to determine if we are still producing values
    global STOP_PRODUCTION
    # Do the production of the data to queue - simulate the Extract & Transform of my ETL process
    for i in range(250):
        # Check to see if we're killing the producer prematurely
        if STOP_PRODUCTION:
            # Logging where we ended here, but we could write the
            logger.info(f'Producer KILLED on pid {os.getpid()} - restart from {i}')
            break
            # We now stop producing the rest of the data, but we still signal to the consumers to end as normal
            # this will allow them to drain whatever is left in the queue
        else:
            # Simulate some random amount of processing
            sleep(random()/2)
            # add some value to the queue
            processing_queue.put(i)
    # Once all the producing is completed, I need to signal to the consumer(s) to end (each needs a signal)
    for _ in range(consumer_count):
        processing_queue.put(None)
    logger.info(f'Producer Done on pid {os.getpid()}')


def consumer_func(processing_queue, log_queue, logging_configurer):
    """
    My example consumer function
    :param processing_queue: the queue object that the consumer(s) are working through
    :param log_queue: queue object for the logs
    :param logging_configurer: the configuratin function for worker processes (worker_log_configurer)
    """
    # Set up logging for each consumer to all use the same log file
    logging_configurer(log_queue)
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.INFO)

    logger.info(f'Consumer - Consumer Running on pid {os.getpid()}')
    # Consume from the produced queue - simulate the L part of my ETL pipeline
    while True:
        # Get the next item from my FIFO queue - we will always do this, even if we've shut down the producer and/or
        # are writing to the deadletter to drain the consumer queue
        try:
            item = processing_queue.get(block=False)
        except Empty:
            logger.info(f'Nothing to consume now, waiting... pid {os.getpid()}')
            sleep(0.5)
            continue

        # Handling whatever we got from the processing queue:
        if item is None:  # this is the shut-down indicator, telling us the producer is no longer adding to the queue
            break  # so we want to get out of this consumer loop
        else:  # we still have items in the queue that we need to handle
            if WRITE_DEADLETTER:
                # Received the KeyboardInterrupt; empty the processing queue to deadletter
                with open(DEADLETTER_QUEUE, "a") as f:
                    f.write(str(item) + "\n")
            else:
                # If we actually got something, we need to do something with it:
                sleep(random()*3)  # The sleep is to simulate consuming is slower than producing
                logger.info(f'Consumed value {item} on pid {os.getpid()}')
    # The consumer process got the shutdown command - is there anything I need to do for clean-up?
    logger.info(f'Consumer shut down. on pid {os.getpid()}')


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
    # the log_listener_process call has been updated (v4) and now needs the following parameters:
    # log_queue, log_configurer_func, log_dir, log_name
    listener = multiprocessing.Process(target=log_listener_process, args=(log_queue,
                                                                          log_listener_configurer,
                                                                          LOG_DIR,
                                                                          LOG_NAME))
    listener.start()

    # Configure logging for the main process
    worker_log_configurer(log_queue)
    logger = logging.getLogger(LOG_NAME)
    logger.setLevel(logging.INFO)

    # log the log queue with the log queue
    logger.info(f"Created Log Queue: {str(log_queue)} from pid {os.getpid()}")

    # Create the processing queue
    # setting maxsize small to demonstrate that producers will wait if the queue gets too big
    processing_queue = manager.Queue(maxsize=3)
    logger.info(f"Created the Processor Queue: {str(processing_queue)} from pid {os.getpid()}")

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
    logger.info(f"Starting the producer from pid {os.getpid()}")
    producer_process = multiprocessing.Process(target=producer_func,
                                               args=(processing_queue,
                                                     log_queue,
                                                     worker_log_configurer,
                                                     args.consumer_count))
    producer_process.start()

    # Join Everything
    logger.info(f"Join Producer from pid {os.getpid()}")
    producer_process.join()
    logger.info(f"Join Consumers from pid {os.getpid()}")
    for c in consumers:
        c.join()

    logger.info(f"Signal the end of the logging and join from pid {os.getpid()}")
    log_queue.put_nowait(None)  # Ends the logging Queue
    listener.join()  # Stops the listener
