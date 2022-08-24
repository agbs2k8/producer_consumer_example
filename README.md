# Producer-Consumer with Shared (single file) Logging in Python
I couldn't find anywhere that someone else had already put these pieces together in once place. 

This uses the Python `multiprocessing` library to spin up multiple processes to accomplish the following:
- Create a logging queue / handler as demonstrated in the [Python Cookbook](https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes) to log to a single file from multiple processes without issues.
- Create a second multiprocessing queue to share work that needs to be done.  Specifically make the queue max-size small as to demonstrate that the producer will wait if the queue gets too large.
- Create a single producer process to add items to the queue, which would appropriately signal to all consumers to shut down when production finished.
- Created multiple consumers to share the queue's workload.

I was generally trying to simulate an ETL process where the Extract & Transform are handled by the producer, which is faster than the Load process being done by multiple consumers.  It could just as easily be flipped the other way.

Other References Used:
[Multiprocessing Queue in Python](https://superfastpython.com/multiprocessing-queue-in-python/) by Jason Brownlee was a good reference to refresh my memory on producers/consumers with a shared queue
