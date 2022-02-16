#!/usr/bin/env python3

import sys
import argparse
import signal
import math
import copy
import curses
import queue
import time
import multiprocessing as mp
import datetime

import filemgmt.compare_utils as compare
from filemgmt import compact_utils as cu

COMPLETE = "Complete"

def parse_cmd_line(argv):
    """ Parse command line arguments

        Parameters
        ----------
        args : command line arguments

        Returns
        -------
        Dictionary continaing the command line arguments
    """
    epilog = """\
The files to be migrated can be specified in multiple ways:
    - pfwid will select all files from the given pfw_attempot_id(s), note that multiple
      pfw_attempt_ids can be given as a comma separated list
    - tag will select all files from all pfw_attempt_ids linked to the given tag

All of the above are mutually exclusive selection criteria.
The use of the date_range argument can be used to narrow the selection criteria.
The following are all valid ways to select the files:
    --pfwid 123456
    --pfwid 123456,789012,345678
    --tag Y1A2_JUNK
"""

    parser = argparse.ArgumentParser(description='Migrate files from one filesystem to another, performing integrity checks of all files',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=epilog)
    parser.add_argument('--des_services', action='store', help='Services file.')
    parser.add_argument('--section', '-s', action='store', help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('--archive', action='store', default='desar2home', help='archive_name from file_archive_info table for the files to be moved')
    parser.add_argument('--reqnum', action='store', help='Not used, but maintined for code compatability.')
    parser.add_argument('--relpath', action='store', help='Not used, but maintined for code compatability.')
    parser.add_argument('--unitname', action='store', help='Not used, but maintined for code compatability.')
    parser.add_argument('--attnum', action='store', help='Not used, but maintined for code compatability.')
    parser.add_argument('--verbose', action='store_true', help='print differences between db and disk')
    parser.add_argument('--debug', action='store_true', help='print all files, recommend >= 300 char wide terminal')
    parser.add_argument('--script', action='store_true', help='Print only if there are errors, usefule for running in loops in scripts')
    parser.add_argument('--pfwid', action='store', help='pfw attempt id to search for')
    parser.add_argument('--silent', action='store_true', help='Run with minimal printing, only print ERROR or OK')
    parser.add_argument('--date_range', action='store', help='Not used, but maintined for code compatability.')
    parser.add_argument('--tag', action='store', help='Compare all data from a specific tag (this can take a long time)')
    parser.add_argument('--start_at', action='store', help='Not used, but maintined for code compatability.', type=int, default=1)
    parser.add_argument('--end_at', action='store', help='Not used, but maintined for code compatability.', type=int, default=0)
    parser.add_argument('--dbh', action='store', help=argparse.SUPPRESS) # used internally
    parser.add_argument('--log', action='store', help='Log file to write to, default is to write to sdtout')
    parser.add_argument('--parallel', action='store', help='Specify the parallelization of the work, e.g. 3 would spread the work across 3 subprocesses.', type=int, default=1)
    cargs = parser.parse_args(argv)
    if cargs.script:
        cargs.verbose = False
    return cargs

def printProgressBar(win, iteration, count, length = 100, fill = '█', printEnd = "\n"):
    """ Print a progress bar
    """
    percent = (f"{iteration:d}/{count:d}")
    filledLength = int(length * iteration // count)
    pbar = fill * filledLength + '-' * (length - filledLength)
    win.addstr(2, 0, f"Progress: |{pbar}| {percent}{printEnd}")

def run(inputs):
    """ Method to launch a multiprocessing run
    """
    try:
        (wn, args, pfwids, event, que) = inputs
        cu.CompactLogs(wn, args, pfwids, event, que)
    finally:
        que.put_nowait(cu.Message(wn, COMPLETE, 0))

def results_error(err):
    """ Error handling routine
    """
    print("Exception raised:")
    print(err)
    raise err

def main():
    """ Main program module

    """
    start = datetime.datetime.now()
    args = parse_cmd_line(sys.argv[1:])
    if args.date_range:
        print("Date ranges cannot be used with the migration script")
        return
    if args.start_at != 1 or args.end_at != 0:
        print("start_at and end_at cannot be used with the migration script")
        return
    if args.relpath:
        print("relpath cannot be used with migration script")
        return
    if args.reqnum or args.attnum or args.unitname:
        print("Cannot specify reqnum, attnum, or unitname")
        return
    if args.log is not None:
        stdp = compare.Print(args.log)
        sys.stdout = stdp
    (args, pfwids) = compare.determine_ids(args)
    manager = mp.Manager()
    event = manager.Event()

    def interrupt(x, y):
        event.set()

    signal.signal(signal.SIGINT, interrupt)

    if args.parallel <= 1:
        _ = cu.CompactLogs(0, args, pfwids, event)
    else:
        if args.parallel > 8:
            args.parallel = 8
        args.dbh.close()
        args.dbh = None
        npids = len(pfwids)
        jobs = []
        for _ in range(args.parallel):
            jobs.append([])
        pos = 0
        while pfwids:
            jobs[pos].append(pfwids.pop())
            pos += 1
            if pos >= args.parallel:
                pos = 0
        manager = mp.Manager()
        queu = manager.Queue()
        done = [False] * len(jobs)
        wins = []
        errors = {}
        try:
            stdscr = curses.initscr()
            curses.cbreak()
            num_rows, num_cols = stdscr.getmaxyx()
            step = math.floor(num_rows/len(jobs))
            for i in range(len(jobs)):
                wins.append(curses.newwin(step, num_cols, i*step, 0))

            with mp.Pool(processes=len(jobs), maxtasksperchild=1) as pool:
                _ = [pool.apply_async(run, args=((i, copy.deepcopy(args), jobs[i], event, queu,),), error_callback=results_error) for i in range(len(jobs))]
                pool.close()
                #pool.join()
                while not all(done):
                    while True:
                        try:
                            ms = queu.get_nowait()
                            if ms.err:
                                if ms.pfwid not in errors:
                                    errors[ms.pfwid] = []
                                errors[ms.pfwid].append(ms.msg)
                                continue
                            if ms.msg == COMPLETE:
                                done[ms.win] = True
                                wins[ms.win].clear()
                                wins[ms.win].addstr("Complete\n")
                            elif ms.msg is not None:
                                wins[ms.win].clear()
                                wins[ms.win].addstr(ms.msg + '\n')
                            else:
                                printProgressBar(wins[ms.win], ms.iteration, ms.count)
                            wins[ms.win].refresh()
                        except queue.Empty:
                            break
                    time.sleep(0.2)
        finally:
            curses.endwin()
        if errors:
            print(f"Issues were encountered in {len(errors)}/{npids} jobs.")
            for pid, msgs in errors.items():
                print(f"pfwid: {pid}")
                for m in msgs:
                    m = m.strip()
                    print(f"   {m}")
        else:
            print("All tasks accomplished")

    end = datetime.datetime.now()
    duration = end-start
    print(f"\nJob took {duration.total_seconds():.1f} seconds")

    if args.log is not None:
        sys.stdout.flush()
        sys.stdout = stdp.close()
    sys.exit(0)

if __name__ == "__main__":
    main()