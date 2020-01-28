#!/usr/bin/env python3

""" Delete files from local disk and DB location tracking based upon an archive path, reqnum,
    unitname, attnum, and or pfw_attempt_id
"""

import os
import sys
import argparse

import filemgmt.disk_utils_local as diskutils
import filemgmt.db_utils_local as dbutils
import filemgmt.filemgmt_defs as fmdef
import filemgmt.fmutils as fmutils
import despydmdb.desdmdbi as desdbi


def report(operator, archive_path, archive, files_from_disk, files_from_db, filesize, fend, pfwids=None):
    """ Method to print(information about the files to be deleted

        Parameters
        ----------
        operator: str
            The name of the operator for the attempt (from pfw_attempt table)

        archive_path: str
            The path in the archive for the attempt (from pfw_attempt and ops_archive tables)

        archive: str
            The name of the archive the files exist in

        files_from_disk: int
            The number of files existing on disk for the attempt (or a sum total for all attempts)

        files_from_db: int
            The number of files listed in the DB for the attempt (or a sum total for all attempts)

        filesize: float
            The total size of the files on disk for the attempt (or a sum total for all attempts)

        fend: str
            The units of the file size (e.g. Gb, Mb, Tb)

        pfwids: list
            A list of all associated pfw_attempt_ids

    """
    # if no operator is given do not report it (usually when there are multiple pfw_attempt_ids being reported)
    if operator:
        print(f"  Operator = {operator}")
    # if no archive path is given do not report it (usually when there are multiple pfw_attempt_ids being reported)
    if archive_path:
        print(f"  Path = {archive_path}")
    print(f"  Archive name = {archive}")
    print(f"  Number of files from disk = {files_from_disk}")
    print(f"  Number of files from db   = {files_from_db}")
    print(f"  Total file size on disk = {filesize:.3f} {fend}")
    # if there is no given pfw_attempt_id do not report it (usually if there is only 1 pfw_attempt_id)
    if pfwids:
        print(f"  pfw_attempt_ids: {', '.join(pfwids)}")
    print('\n')

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

def gather_data(dbh, args):
    """ Make sure command line arguments have valid values """

    part = False
    # if a relative path is given it gets precedence
    if args.relpath:
        # make sure relpath is not an absolute path
        if args.relpath[0] == '/':
            print(f"Error: relpath is an absolute path  ({args.relpath[0]})")
            print("\tIt should be the portion of the path after the archive root.")
            print("\tAborting")
            sys.exit(1)

        archive_root, archive_path, state, operator, pfwid = dbutils.get_paths_by_path(dbh, args)
        relpath = args.relpath
        # check to see if we are deleting full attempt or part
        if pfwid is None:
            part = True
    elif args.pfwid:
        archive_root, archive_path, relpath, state, operator, pfwid = dbutils.get_paths_by_id(dbh, args)
    else:
        print("Either relpath, pfw_attempt_id, or a reqnum/unitname/attnum triplet must be specified.")
        sys.exit(1)

    # if relpath is None the just abort gracefully
    if not relpath:
        return None, None, None, None, None, pfwid, None
    # make sure path is at least 3 directories longer than archive_root
    subdirs = relpath.strip('/').split('/')  # remove any trailing / first
    if len(subdirs) < 3:
        print("Suspect relpath is too high up in archive (deleting too much).")
        print("\tCheck relpath is accurate.   If actually want to delete all that,")
        print("\tcall program on smaller chunks")
        print("\tAborting")
        sys.exit(1)

    # check path is path exists on disk, but ignore if this is part of a tag
    path = f"{archive_root}/{relpath}"
    if not os.path.exists(path) and not args.tag:
        print(f" Path does not exist on disk:  {path}")
        return None, None, None, None, None, pfwid, None

    return archive_root, archive_path, relpath, state, operator, pfwid, part

def print_info(comparison_info, pfwids, key):
    """ Method to print(out files for a specific collecion (dbonly, diskonly, both)

        Parameters
        ----------
        comparison_info : dict
            File comparison info on all files keyed by pfw_attempt_id

        pfwids : list
            List of pfw_attempt_ids covered by the comparison_info data. This is passed rather then
            getting it from the keys of comparison_info so that pfw_attempt_id order is maintained
            for each call to this method

        key : str
            The key whose data are being printed

    """
    # only print(out the header if there is more than 1 pfw_attempt_id
    if pfwids[0]:
        print("PFW_ATT_ID     File")
    for pid in pfwids:
        for filename in comparison_info[pid][key]:
            if comparison_info[pid]['pfwid']:
                print(f"  {comparison_info[pid]['pfwid']}     {filename}")
            else:
                print(f"    {filename}")
    print('\n')

def get_counts(comparison_info):
    """ Method to get the number of files only found in the Db and only found on disk

        Parameters
        ----------
        comparison_info : dict
            Comparison information about the files, keyed by pfw_attempt_id

        Returns
        -------
        int, int
            The number of files only found in the BD and the number of files only found on disk

    """
    onlydb = 0
    onlydisk = 0
    for data in comparison_info.values():
        onlydb += len(data['dbonly'])
        onlydisk += len(data['diskonly'])
    return onlydb, onlydisk

def print_files(comparison_info):
    """ Method to print(all found files, separated by where they were found (both disk and DB,
        disk only, DB only)

        Parameters
        ----------
        comparison_info : dict
            File comparison info on all files keyed by pfw_attempt_id
    """

    onlydb, onlydisk = get_counts(comparison_info)
    print("\nFiles in both database and on disk:\n")
    pfwids = list(comparison_info.keys())
    # print(out all files found both on disk and in the DB
    print_info(comparison_info, pfwids, 'both')

    # report any files only found in the DB
    print(" Files only found in database:\n")
    if onlydb == 0:
        print("   None\n\n")
    else:
        print_info(comparison_info, pfwids, 'dbonly')

    # report any files only found on disk
    print(" Files only found on disk:\n")
    if onlydisk == 0:
        print("   None\n\n")
    else:
        print_info(comparison_info, pfwids, 'diskonly')

def diff_files(comparison_info):
    """ Method to print(out the differences between files found on disk and found in the DB

        Parameters
        ----------
        comparison_info : dict
            Comparison info for all files keyed by pfw_attempt_id

    """
    onlydb, onlydisk = get_counts(comparison_info)

    # if there are no files only found on disk or only found in the DB
    if onlydb == onlydisk == 0:
        print("\n No differneces found\n")
        return
    pfwids = list(comparison_info.keys())
    # report any files only found in the DB
    print("\n Files only found in database:\n")
    if onlydb == 0:
        print("None\n\n")
    else:
        print_info(comparison_info, pfwids, 'dbonly')
    print(" Files only found on disk:\n")
    # report any files only found on disk
    if onlydisk == 0:
        print("None\n\n")
    else:
        print_info(comparison_info, pfwids, 'diskonly')

def get_size_unit(filesize):
    """ Method to determine the units of the file size (Gb, Mb, kb, etc) for human readability

        Parameters
        ----------
        filesize : int
            The file size in bytes

        Returns
        -------
        filesize : float
            Scaled filesize to match the units

        unit : str
            The units of the file size

    """
    if filesize >= fmdef.TB:
        filesize /= fmdef.TB
        return filesize, "Tb"
    if filesize >= fmdef.GB:
        filesize /= fmdef.GB
        return filesize, "Gb"
    if filesize >= fmdef.MB:
        filesize /= fmdef.MB
        return filesize, "Mb"
    filesize /= fmdef.KB
    return filesize, "kb"

def main():
    """ Main control """

    args = parse_and_check_cmd_line(sys.argv[1:])
    dbh = desdbi.DesDmDbi(args.des_services, args.section)
    # get all pfw_attempt_ids for the given tag
    if args.tag:
        if not args.filetype:
            print('WARNING, specifying a tag without a filetype will delete all data from the tag.')
            should_continue = input("Please verify you want to do this [yes/no]: ")
            shdelchar = should_continue[0].lower()
            if shdelchar in ['y', 'yes']:
                pass
            else:
                sys.exit(0)
        pfw_ids = dbutils.get_pfw_attempt_id_from_tag(dbh, args.tag)
    elif args.pfwid and ',' in args.pfwid:
        pfw_ids = args.pfwid.split(',')
    elif args.reqnum:
        pfw_ids = dbutils.get_pfw_attempt_ids_from_triplet(dbh, args)
        args.reqnum = None
        args.unitname = None
        args.attnum = None
    else:
        pfw_ids = [args.pfwid]
    pfw_ids.sort() # put them in order
    all_data = {}
    merged_comparison_info = {}
    # go through each pfw_attempt_id and gather the needed data
    for pid in pfw_ids:
        args.pfwid = pid
        archive_root, archive_path, relpath, state, operator, pfwid, part = gather_data(dbh, args)
        if not archive_root and not relpath:
            print(f"    Skipping pfw_attempt_id {pfwid}.")
            continue
        files_from_disk, dup = diskutils.get_files_from_disk(relpath, archive_root)
        files_from_db, dup = dbutils.get_files_from_db(dbh, relpath, args.archive, pfwid, args.filetype)
        # if filetype is set then trim down the disk results
        if args.filetype is not None:
            newfiles = {}
            for filename, val in files_from_db.items():
                if filename in files_from_disk:
                    newfiles[filename] = files_from_disk[filename]
            files_from_disk = newfiles

        comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, dup, False, archive_root=archive_root, pfwid=pid)
        merged_comparison_info[pfwid] = comparison_info
        # add it to the master dictionary
        all_data[pfwid] = fmutils.DataObject(**{'archive_root': archive_root,
                                                'archive_path': archive_path,
                                                'relpath': relpath,
                                                'state': state,
                                                'operator': operator,
                                                'pfwid': pfwid,
                                                'dofiles': args.filetype is not None or part,
                                                'files_from_disk': files_from_disk,
                                                'dup': dup,
                                                'files_from_db': files_from_db,
                                                'comparison_info': comparison_info})

    if not all_data:
        print("Nothing to do")
        sys.exit(0)
    filesize = 0.0
    bad_filesize = 0.0
    bad_pfwids = []
    ffdb = 0
    ffd = 0
    bad_ffdb = 0
    bad_ffd = 0
    # gather the stats for reporting
    empty_pfwids = []
    for data in all_data.values():
        # if the data is not junk and no filetype was specified then it cannot be deleted
        if data.state != 'JUNK' and args.filetype is None:
            for filename, val in data.files_from_disk.items():
                #print(filename,val
                bad_filesize += val['filesize']
            bad_pfwids.append(str(data.pfwid))
            bad_ffdb += len(data.files_from_db)
            bad_ffd += len(data.files_from_disk)
        else:
            for filename, val in data.files_from_disk.items():
                #print(filename,val
                filesize += val['filesize']
            ffdb += len(data.files_from_db)
            ffd += len(data.files_from_disk)
            if not data.files_from_db and not data.files_from_disk:
                empty_pfwids.append(data.pfwid)
    for pid in empty_pfwids:
        del all_data[pid]
        del merged_comparison_info[pid]

    filesize, fend = get_size_unit(filesize)

    bad_filesize, bfend = get_size_unit(bad_filesize)

    # report the results of what was found
    if not files_from_db:
        print("\nNo files in database to delete.")
        sys.exit(0)
    if not files_from_disk:
        print("\nNo files on disk to delete.")
        sys.exit(0)

    if bad_pfwids:
        print("\nThe following data cannot be deleted as the associated attempts have not been marked as 'JUNK' (ATTEMPT_STATE.DATA_STATE):")
        if len(bad_pfwids) == 1:
            pid = list(all_data.keys())[0]
            operator = all_data[pid].operator
            archive_path = all_data[pid].archive_path
        else:
            operator = None
            archive_path = None
        report(operator, archive_path, args.archive, bad_ffdb, bad_ffd, bad_filesize, bfend, bad_pfwids)
        if len(bad_pfwids) == len(all_data):
            print(" No data to delete\n")
            sys.exit(1)
    for bpid in bad_pfwids:
        del all_data[int(bpid)]
        del merged_comparison_info[int(bpid)]

    if len(all_data) == 1:
        pid = list(all_data.keys())[0]
        operator = all_data[pid].operator
        archive_path = all_data[pid].archive_path
    else:
        operator = None
        archive_path = None
    if bad_pfwids:
        print('\nFiles that can be deleted')

    report(operator, archive_path, args.archive, ffdb, ffd, filesize, fend)

    if args.dryrun:
        sys.exit(0)

    shdelchar = 'x'
    while shdelchar not in ['n', 'y']:
        print("")
        # query if we should proceed
        should_delete = input("Do you wish to continue with deletion [yes/no/diff/print]?  ")
        shdelchar = should_delete[0].lower()

        if shdelchar in ['p', 'print']:
            print_files(merged_comparison_info)

        elif shdelchar in ['d', 'diff']:
            diff_files(merged_comparison_info)

        elif shdelchar in ['y', 'yes']:
            # loop over each pfwid
            for data in all_data.values():
                # if deleting specific files
                if data.dofiles:
                    good = diskutils.del_part_files_from_disk(data.files_from_db, data.archive_root)
                    if len(good) != len(data.files_from_db):
                        print("Warning, not all files on disk could be deleted. Only removing the deleted ones from the database.")
                    dbutils.del_part_files_from_db(dbh, good)
                    # check to see if this is the last of the files in the attempt
                    if dbutils.get_file_count_by_pfwid(dbh, data.pfwid) != 0:
                        depth = 'PRUNED'  # there are still some files on disk for this pfw_attempt_id
                    else:
                        depth = 'PURGED'  # these were the last files for the pfw_attempt_id
                else:
                    try:
                        diskutils.del_files_from_disk(data.archive_path)
                    except Exception as exept:
                        print("Error encountered when deleting files: ", str(exept))
                        print("Aborting")
                        raise
                    errfls = {}
                    for (dirpath, _, filenames) in os.walk(os.path.join(data.archive_root, data.relpath)):
                        for filename in filenames:
                            errfls[filename] = dirpath
                    if errfls:
                        delfiles = []
                        depth = 'PRUNED'
                        for filename, val in data.files_from_disk.items():
                            if filename not in errfls:
                                delfiles.append((filename))
                        dbutils.del_part_files_from_db_by_name(dbh, data.relpath, args.archive, delfiles)
                    else: # has to be purged as only an entire attempt can be deleted this way
                        depth = 'PURGED'
                        dbutils.del_files_from_db(dbh, data.relpath, args.archive)
                dbutils.update_attempt_state(dbh, depth, data.pfwid)
        elif shdelchar in ['n', 'no']:
            print("Exiting.")
        else:
            print(f"Unknown input ({shdelchar}).   Ignoring")

if __name__ == "__main__":
    main()
