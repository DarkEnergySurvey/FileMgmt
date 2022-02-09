#!/usr/bin/env python3
""" Module to migrate files from one archive location to another, keeping the DB consistent.

"""
import sys
import argparse
import signal
import math
import copy
import multiprocessing as mp

import filemgmt.compare_utils as compare
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
    - relpath will select all files at or below the given path
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
    parser.add_argument('--relpath', action='store', help='relative path on disk within archive (no archive root)')
    parser.add_argument('--unitname', action='store', help='Unit name to search for')
    parser.add_argument('--attnum', action='store', help='Attempt number to search for')
    parser.add_argument('--verbose', action='store_true', help='print differences between db and disk')
    parser.add_argument('--debug', action='store_true', help='print all files, recommend >= 300 char wide terminal')
    parser.add_argument('--script', action='store_true', help='Print only if there are errors, usefule for running in loops in scripts')
    parser.add_argument('--pfwid', action='store', help='pfw attempt id to search for')
    parser.add_argument('--silent', action='store_true', help='Run with minimal printing, only print ERROR or OK')
    parser.add_argument('--date_range', action='store', help='Not used')
    parser.add_argument('--force', action='store_true', help='Do not ask to delete files, just do it')
    parser.add_argument('--tag', action='store', help='Compare all data from a specific tag (this can take a long time)')
    parser.add_argument('--start_at', action='store', help='Not used', type=int, default=1)
    parser.add_argument('--end_at', action='store', help='Not used', type=int, default=0)
    parser.add_argument('--dbh', action='store', help=argparse.SUPPRESS) # used internally
    parser.add_argument('--log', action='store', help='Log file to write to, default is to write to sdtout')
    parser.add_argument('--parallel', action='store', help='Specify the parallelization of the migration, e.g. 3 would spread the work across 3 subprocesses.', type=int, default=1)
    cargs = parser.parse_args(argv)
    if cargs.script:
        cargs.verbose = False
    return cargs

def run(inputs):
    """ Method to launch a multiprocessing run
    """
    (args, pfwids, event) = inputs
    mu.Migration(args, pfwids, event)

def results_error(err):
    print("Exception raised:")
    print(err)
    raise err

def main():
    """ Main program module

    """
    args = parse_cmd_line(sys.argv[1:])
    if args.date_range:
        print("Date ranges cannot be used with the migration script")
        return
    if args.start_at != 1 or args.end_at != 0:
        print("start_at and end_at cannot be used with the migration script")
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
        _ = mu.Migration(args, pfwids, event)
    else:
        args.dbh.close()
        args.dbh = None
        rem = len(pfwids)%args.parallel
        if rem < args.parallel/2:
            count = math.ceil(len(pfwids)/args.parallel)
        else:
            count = int(len(pfwids)/args.parallel)
        jobs = []
        pos = 0
        while pos < len(pfwids) - count:
            jobs.append(pfwids[pos:pos+count])
            pos += count
        jobs.append(pfwids[pos:])
        with mp.Pool(processes=len(jobs), maxtasksperchild=10) as pool:
            _ = [pool.apply_async(run, args=((copy.deepcopy(args), jobs[i], event,),), error_callback=results_error) for i in range(len(jobs))]
            pool.close()
            pool.join()


    if args.log is not None:
        sys.stdout.flush()
        sys.stdout = stdp.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
