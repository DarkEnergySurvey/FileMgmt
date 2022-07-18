#!/usr/bin/env python3

""" Delete files from local disk and DB location tracking based upon an archive path, reqnum,
    unitname, attnum, and or pfw_attempt_id
"""

import sys
import argparse

from filemgmt import fmutils
from filemgmt import delfile_utils as dut


def parse_and_check_cmd_line(argv):
    """ Method to parse command line arguments

    """
    epilog = """\
The files to be deteted can be specified in multiple ways:
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
The use of the filetype argument will narrow the selected files to be of a specific file type.
The following are all valid ways to select the files:
    --reqnum 1115
    --reqnum 1115 --attnum 5
    --pfwid 123456
    --pfwid 123456,789012,345678
    --tag Y1A2_JUNK
"""

    parser = argparse.ArgumentParser(description='Delete files from DB and disk based upon path, tag, pfw_attempt_id, triplet and/or filetype.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=epilog)
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store', help='Must be specified if DES_DB_SECTION is not set in environment.')
    parser.add_argument('--archive', action='store', required=True, help='archive_name from file_archive_info table (usually desar2home or prodbeta).')
    parser.add_argument('--relpath', action='store', help='Relative path to files on disk within archive. All subdirectories below this will also be deleted.')
    parser.add_argument('--reqnum', action='store', help='Request number to delete data from.')
    parser.add_argument('--unitname', action='store', help='Unit name to delete data from, must be accompanied by a reqnum.')
    parser.add_argument('--attnum', action='store', help='Attempt number to delete data from, must be accompanied by a reqnum, can also be accompanied by a unitname.')
    parser.add_argument('--tag', action='store', help='Tag to delete data from.')
    parser.add_argument('--filetype', action='store', help='File type to delete.')
    parser.add_argument('--dbh', action='store')
    parser.add_argument('--dryrun', action='store_true', help='Used to determine and report what files would be deleted, but no deletion is performed.')
    parser.add_argument('--pfwid', action='store', help='Pfw attempt id(s) to delete data from. Can be a single pfw_attempt_id or a comma separated list.')

    args = parser.parse_args(argv)
    # check for validity
    if args.relpath and (args.reqnum or args.unitname or args.attnum or args.tag or args.pfwid):
        print("Relpath was specified, thus reqnum, unitname, attnum, tag, and pfwid cannot be specified.")
        sys.exit(1)
    if args.reqnum and (args.tag or args.pfwid):
        print("Reqnum was specified, thus tag and pfwid cannot be specified.")
        sys.exit(1)
    if args.tag and args.pfwid:
        print("Tag was specified, thus pfwid cannot be specified.")
        sys.exit(1)
    if (args.unitname or args.attnum) and not args.reqnum:
        print("Unitname and/or attnum were specified, but reqnum was not, please supply a reqnum and run again.")
        sys.exit(1)
    return args


def main():
    """ Main control """

    args = parse_and_check_cmd_line(sys.argv[1:])
    if args.tag:
        if not args.filetype:
            print('WARNING, specifying a tag without a filetype will delete all data from the tag.')
            should_continue = input("Please verify you want to do this [yes/no]: ")
            shdelchar = should_continue[0].lower()
            if shdelchar in ['y', 'yes']:
                pass
            else:
                sys.exit(0)
    (args, pfwids) = fmutils.determine_ids(args)
    dfl = dut.DelFiles(args, pfwids)
    dfl.run()

if __name__ == "__main__":
    main()
