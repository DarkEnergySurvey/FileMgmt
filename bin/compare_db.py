#!/usr/bin/env python3

""" Compare files between disk and DB, based on relative path, reqnum, unitname, attnum, and/or
    pfw_attempt_id
"""

import sys
import argparse
import multiprocessing as mp

import filemgmt.compare_utils as compare
from filemgmt import fmutils


def parse_cmd_line(argv):
    """ Parse command line arguments

        Parameters
        ----------
        argv : command line arguments

        Returns
        -------
        Dictionary continaing the command line arguments
    """
    epilog = """\
The files to be compared can be specified in multiple ways:
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

    parser = argparse.ArgumentParser(description='Compare files on disk with their entry in the database, including file size and md5sum, if requested',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=epilog)
    parser.add_argument('--des_services', action='store', help='Services file.')
    parser.add_argument('--section', '-s', action='store', help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('--archive', action='store', default='desar2home', help='archive_name from file_archive_info table')
    parser.add_argument('--reqnum', action='store', help='Request number to search for')
    parser.add_argument('--relpath', action='store', help='relative path on disk within archive (no archive root)')
    parser.add_argument('--unitname', action='store', help='Unit name to search for')
    parser.add_argument('--attnum', action='store', help='Attempt number to search for')
    parser.add_argument('--md5sum', action='store_true', help='compare db vs disk check_md5sums')
    parser.add_argument('--verbose', action='store_true', help='print differences between db and disk')
    parser.add_argument('--debug', action='store_true', help='print all files, recommend >= 300 char wide terminal')
    parser.add_argument('--script', action='store_true', help='Print only if there are errors, usefule for running in loops in scripts')
    parser.add_argument('--pfwid', action='store', help='pfw attempt id to search for')
    parser.add_argument('--silent', action='store_true', help='Run with minimal printing, only print ERROR or OK')
    parser.add_argument('--date_range', action='store', help='Date range of submit time in YYYY-MM-DD format. If date is given then only data from that date are checked, if two comma separated dates are given then all data from between those dates (inclusive) are checked.')
    parser.add_argument('--pipeline', action='store', help='Compare data from a specific pipeline (subpipeprod in pfw_attempt), only used in conjunction with date_range')
    parser.add_argument('--tag', action='store', help='Compare all data from a specific tag (this can take a long time)')
    parser.add_argument('--start_at', action='store', help='Index to start at (1 based), useful for doing checking in chunks.', type=int, default=1)
    parser.add_argument('--end_at', action='store', help='Index to end at (1 based), useful for doing checking in chunks.', type=int, default=0)
    parser.add_argument('--dbh', action='store', help=argparse.SUPPRESS) # used internally
    parser.add_argument('--log', action='store', help='Log file to write to, default is to write to sdtout')
    cargs = parser.parse_args(argv)
    if cargs.script:
        cargs.verbose = False
    return cargs

def main():
    """ Main program module

    """
    args = parse_cmd_line(sys.argv[1:])
    if args.log is not None:
        stdp = compare.Print(args.log)
        sys.stdout = stdp
    (args, pfwids) = fmutils.determine_ids(args)
    manager = mp.Manager()
    event = manager.Event()

    def interrupt(x, y):
        event.set()

    comp = compare.FileCompare(args, pfwids, event)
    ret = comp.run()
    if args.log is not None:
        sys.stdout.flush()
        sys.stdout = stdp.close()
    sys.exit(ret)

if __name__ == "__main__":
    main()
