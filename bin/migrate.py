#!/usr/bin/env python3
""" Module to migrate files from one archive location to another, keeping the DB consistent.

"""
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

from filemgmt import fmutils
from filemgmt import migrate_utils as mu

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
    - reqnum will select all files of a given reqnum
      - unitname (must also have a reqnum supplied) will select all files from the given
        reqnum/unitname
      - attnum (must have reqnum, but not necessarily unitname supplied) will select all files
        from the given reqnum/(unitname)/attnum
    - pfwid will select all files from the given pfw_attempot_id(s), note that multiple
      pfw_attempt_ids can be given as a comma separated list
    - tag will select all files from all pfw_attempt_ids linked to the given tag

All of the above are mutually exclusive selection criteria.
The use of the date_range argument can be used to narrow the selection criteria.
The following are all valid ways to select the files:
    --reqnum 1115
    --reqnum 1115 --attnum 5
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
    parser.add_argument('--destination', action='store', required=True, help='Destination folder for the files.')
    parser.add_argument('--current', action='store', default=None, help='Section of the current path that will be replaced by the `destination` path. Examples:\n   --current=\'OPS/\' --destination=\'taiga/\' will move the selected files from <archive root>/OPS to <archive root>/taiga.\n   If left empy the `destinataion` will be prepended to the current directory --destination=\'taiga/\' will move the selected files from <archive root>/ to <archive root>/taiga.\n   --current=\'Y5A1/\' --destination=\'Y5A1_taiga/\' will move <archive root>/OPS/finalcut/Y5A1/XYZ/p02 to <archive root>/OPS/finalcut/Y5A1_taiga/XYZ/p02')
    parser.add_argument('--reqnum', action='store', help='Request number to search for')
    parser.add_argument('--unitname', action='store', help='Unit name to search for')
    parser.add_argument('--attnum', action='store', help='Attempt number to search for')
    parser.add_argument('--verbose', action='store_true', help='print differences between db and disk')
    parser.add_argument('--debug', action='store_true', help='print all files, recommend >= 300 char wide terminal')
    parser.add_argument('--script', action='store_true', help='Print only if there are errors, usefule for running in loops in scripts')
    parser.add_argument('--pfwid', action='store', help='pfw attempt id to search for')
    parser.add_argument('--silent', action='store_true', help='Run with minimal printing, only print ERROR or OK')
    parser.add_argument('--tag', action='store', help='Compare all data from a specific tag (this can take a long time)')
    parser.add_argument('--dbh', action='store', help=argparse.SUPPRESS) # used internally
    parser.add_argument('--log', action='store', help='Log file to write to, default is to write to sdtout')
    parser.add_argument('--parallel', action='store', help='Specify the parallelization of the migration, e.g. 3 would spread the work across 3 subprocesses.', type=int, default=1)
    parser.add_argument('--raw', action='store', default=None, help='Migrate RAW files')
    cargs = parser.parse_args(argv)
    if cargs.script:
        cargs.verbose = False
    return cargs


def main():
    """ Main program module

    """
    start = datetime.datetime.now()
    args = parse_cmd_line(sys.argv[1:])
    if args.log is not None:
        stdp = fmutils.Print(args.log)
        sys.stdout = stdp
    if args.raw is None:
        (args, pfwids) = fmutils.determine_ids(args)
        rpaths = []
    else:
        (args, rpaths) = fmutils.get_unique_paths(args)
        pfwids = []
    manager = mp.Manager()
    event = manager.Event()

    def interrupt(x, y):
        event.set()

    signal.signal(signal.SIGINT, interrupt)

    if args.parallel <= 1:
        if rpaths:
            mul = mu.Migration(0, args, [], event, rpaths)
        else:
            mul = mu.Migration(0, args, pfwids, event)
        mul.run()
    else:
        if args.parallel > 8:
            args.parallel = 8
        args.dbh.close()
        args.dbh = None
        npids = len(pfwids)
        jobs = []
        rjobs = []
        for _ in range(args.parallel):
            jobs.append([])
            rjobs.append([])
        pos = 0
        while rpaths:
            rjobs[pos].append(rpaths.pop())
            pos += 1
            if pos >= args.parallel:
                pos = 0
        pos = 0
        while pfwids:
            jobs[pos].append(pfwids.pop())
            pos += 1
            if pos >= args.parallel:
                pos = 0
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
                _ = [pool.apply_async(fmutils.run, args=((mu.Migration, i, copy.deepcopy(args), jobs[i], event, rjobs[i], queu,),), error_callback=fmutils.results_error) for i in range(len(jobs))]
                pool.close()
                while not all(done):
                    while True:
                        try:
                            ms = queu.get_nowait()
                            if ms.err:
                                if ms.pfwid not in errors:
                                    errors[ms.pfwid] = []
                                errors[ms.pfwid].append(ms.msg)
                                continue
                            if ms.msg == fmutils.COMPLETE:
                                done[ms.win] = True
                                wins[ms.win].clear()
                                wins[ms.win].addstr("Complete\n")
                            elif ms.msg is not None:
                                wins[ms.win].clear()
                                wins[ms.win].addstr(ms.msg + '\n')
                            else:
                                fmutils.printProgressBar(wins[ms.win], ms.iteration, ms.count)
                            wins[ms.win].refresh()
                        except queue.Empty:
                            break
                    time.sleep(0.2)
        except Exception as ex:
            with open("error.log", 'w') as fh:
                fh.write(str(ex))
            print("An exception occured see error.log for details.")

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
