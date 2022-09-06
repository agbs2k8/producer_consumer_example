import signal
import time

signals = """
if __name__ == "__main__":
    signal_names = dict()
    for name in dir(signal):
        if name.startswith("SIG") and not name.startswith("SIG_"):
            signal_names[int(getattr(signal, name))] = name

    for signum, name in signal_names.items():
        handler = signal.getsignal(signum)
        if handler is signal.SIG_DFL:
            handler = 'SIG_DFL (default)'
        elif handler is signal.SIG_IGN:
            handler = 'SIG_IGN (ignored)'
        print(f'{signum} - {name}\t{handler}')

SIGABRT = <Signals.SIGABRT: 6>
SIGALRM = <Signals.SIGALRM: 14>
SIGBUS = <Signals.SIGBUS: 10>
SIGCHLD = <Signals.SIGCHLD: 20>
SIGCONT = <Signals.SIGCONT: 19>
SIGEMT = <Signals.SIGEMT: 7>
SIGFPE = <Signals.SIGFPE: 8>
SIGHUP = <Signals.SIGHUP: 1>
SIGILL = <Signals.SIGILL: 4>
SIGINFO = <Signals.SIGINFO: 29>
SIGINT = <Signals.SIGINT: 2>
SIGIO = <Signals.SIGIO: 23>
SIGKILL = <Signals.SIGKILL: 9>
SIGPIPE = <Signals.SIGPIPE: 13>
SIGPROF = <Signals.SIGPROF: 27>
SIGQUIT = <Signals.SIGQUIT: 3>
SIGSEGV = <Signals.SIGSEGV: 11>
SIGSTOP = <Signals.SIGSTOP: 17>
SIGSYS = <Signals.SIGSYS: 12>
SIGTERM = <Signals.SIGTERM: 15>
SIGTRAP = <Signals.SIGTRAP: 5>
SIGTSTP = <Signals.SIGTSTP: 18>
SIGTTIN = <Signals.SIGTTIN: 21>
SIGTTOU = <Signals.SIGTTOU: 22>
SIGURG = <Signals.SIGURG: 16>
SIGUSR1 = <Signals.SIGUSR1: 30>
SIGUSR2 = <Signals.SIGUSR2: 31>
SIGVTALRM = <Signals.SIGVTALRM: 26>
SIGWINCH = <Signals.SIGWINCH: 28>
SIGXCPU = <Signals.SIGXCPU: 24>
SIGXFSZ = <Signals.SIGXFSZ: 25>"""

# Define what should take place when a signal is received
def kill_handler(signum, frame):
    # Identify what signal we got
    print(f"Kill Signal number {signum}")
    # Kill the whole python process.  Using 15 which is the same as `kill` from bash
    exit(15)


# Define what should take place when a signal is received
def interrupt_handler(signum, frame):
    # Identify what signal we got
    print(f"Interrupt Signal number {signum}")
    # Kill the whole python process.
    exit(15)


# Override the standard handling of the SIGTERM signal (15) -> the result of kill on the command line
signal.signal(signal.SIGTERM, kill_handler)
# Override SIGINT (2) which is what we would get from the KeyboardInterrupt
# This should prevent the KeyboardInterrupt in the loop from ever hitting
signal.signal(signal.SIGINT, interrupt_handler)


if __name__ == "__main__":
    # This just keeps the whole thing running while we wait for the signal
    # catching KeyboardInterrupt just makes sure it's clear what's happening
    loop = True
    while loop:
        try:
            print("...", end="", flush=True)
            time.sleep(1)
        except KeyboardInterrupt:
            # This shouldn't work
            print("Interrupted the usual way")
            loop = False
