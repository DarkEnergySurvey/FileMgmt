""" Compare files from local disk and DB location tracking based upon an archive path """
import os
from sets import Set
import sys

import despydmdb.desdmdbi as desdmdbi
import filemgmt.disk_utils_local as diskutils
import filemgmt.db_utils_local as dbutils
import filemgmt.fmutils as fmutils


def gather_data(dbh, args):
    """ Make sure command line arguments have valid values

        Parameters
        ----------
        dbh : database connection
            connection to use for checking the database related argumetns

        args : dict
            dictionary containing the command line arguemtns

        Returns
        -------
        string containing the archive root

    """
    if args.relpath is not None:
        archive_root, archive_path, _, operator, pfwid = dbutils.get_paths_by_path(dbh, args)
        relpath = args.relpath
    elif args.reqnum or args.pfwid:
        archive_root, archive_path, relpath, _, operator, pfwid = dbutils.get_paths_by_id(dbh, args)
    else:
        raise Exception("Either relpath, pfwid, or a reqnum must be specified.")
    if not relpath:
        return None, None, None, None, None
    # check path exists on disk
    if not os.path.exists(archive_path):
        print "Warning: Path does not exist on disk:  %s" % (archive_path)
    if args.verbose:
        args.silent = False
    return archive_root, archive_path, relpath, operator, pfwid

def print_all_files(comparison_info, files_from_db, files_from_disk):
    """ Print both lists of files side by side

        Parameters
        ----------
        comparison_info : dict
            Dictionary containing the results of the comparisons

        files_from_db : dict
            Dicitonary containing the file info from the database

        files_from_disk : dict
            Dictionary containing the file info from disk

        Returns
        -------
        None

    """

    print "db path/name (filesize, md5sum)   F   disk path/name (filesize, md5sum)"
    allfiles = Set(files_from_db).union(Set(files_from_disk))
    fdisk_str = ""
    # loop over all found files
    for fname in allfiles:
        # if the file name is in the DB list
        if fname in files_from_db:
            finfo = files_from_db[fname]
            fullname = "%s/%s" % (finfo['path'], fname)
            filesize = None
            if 'filesize' in finfo:
                filesize = finfo['filesize']
            md5sum = None
            if 'md5sum' in finfo:
                md5sum = finfo['md5sum']

            fdb_str = "%s (%s, %s)" % (fullname, filesize, md5sum)
        else:
            fdb_str = ""
        # if the file name is in the disk list
        if fname in files_from_disk:
            finfo = files_from_disk[fname]
            fullname = "%s/%s" % (finfo['relpath'], fname)
            filesize = None
            if 'filesize' in finfo:
                filesize = finfo['filesize']
            md5sum = None
            if 'md5sum' in finfo:
                md5sum = finfo['md5sum']

            fdisk_str = "%s (%s, %s)" % (fullname, filesize, md5sum)
        else:
            fdisk_str = ""
        # not whether they are the same or not
        comp = 'X'
        if fname in comparison_info['equal']:
            comp = '='

        print "%-140s %s %-140s" % (fdb_str, comp, fdisk_str)

def diff_files(comparison_info, files_from_db, files_from_disk, check_md5sum, check_filesize, duplicates, db_duplicates):
    """ Method to print the differences in the file lists (DB vs disk)

        Parameters
        ----------
        comparision_info : dict
            Dictionary containing the comparisions of disk and db for each file

        file_from_db : dict
            Dicitonary containing the file info from the database

        files_from_disk : dict
            Dictionary containing the file info from disk

        check_md5sum : bool
            Whether or not to report the md5sum comparision

        check_filesize : bool
            Whether or not to report the filesize comparison


        Returns
        -------
        None
    """
    pdup = []
    # Print out files that are only found in the DB
    if len(comparison_info['dbonly']) > 0:
        print "Files only found in the database --------- "
        for fname in sorted(comparison_info['dbonly']):
            fdb = files_from_db[fname]
            print "\t%s/%s" % (fdb['path'], fname)

    # print out files that are only found on disk
    if len(comparison_info['diskonly']) > 0:
        print "\nFiles only found on disk --------- "
        for fname in sorted(comparison_info['diskonly']):
            addon = ""
            if fname in duplicates:
                addon = "  *"
            fdisk = files_from_disk[fname]
            print "\t%s/%s%s" % (fdisk['relpath'], fname, addon)
        if len(comparison_info['pathdup']) > 0:
            print "\n The following files had multiple paths on disk (path  filesize):"
            listing = {}
            for fname in comparison_info['pathdup']:
                pdup.append(fname)
                listing[comparison_info['pathdup']['relpath']] = comparison_info['pathdup']['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if fname in files_from_db and files_from_db[fname]['path'] == pth:
                    addon = "  (DB Match)"
                print "      %s %s/%s   %i%s" % (start, pth, fname, listing[pth], addon)

    # Print files that have different paths on disk and in the DB
    if len(comparison_info['path']) > 0:
        print "\nPath mismatch (file name, db path, disk path) --------- "
        for fname in sorted(comparison_info['path']):
            addon = ""
            if fname in duplicates:
                addon = " *"
            fdb = files_from_db[fname]
            fdisk = files_from_disk[fname]
            print "\t%s\t%s\t%s%s" % (fname, fdb['path'], fdisk['relpath'], addon)
        if len(comparison_info['duplicates']) > 0:
            print "  The following files have multiple disk paths on disk (path  filesize):"
            for fname in comparison_info['duplicates']:
                pdup.append(fname)
                listing[comparison_info['duplicates']['relpath']] = comparison_info['duplicates']['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if fname in files_from_db and files_from_db[fname]['path'] == pth:
                    addon = "  (DB Match)"
                print "      %s %s/%s   %i%s" % (start, pth, fname, listing[pth], addon)

    # Print files that have different file sizes on disk and in the DB
    if check_filesize and len(comparison_info['filesize']) > 0:
        print "\nFilesize mismatch (File name, size in DB, size on disk) --------- "
        for fname in sorted(comparison_info['filesize']):
            fdb = files_from_db[fname]
            fdisk = files_from_disk[fname]
            print "\t%s %s %s" % (fname, fdb['filesize'], fdisk['filesize'])

    # Print files that have different md5sum on disk and in DB
    if check_md5sum and 'md5sum' in comparison_info and len(comparison_info['md5sum']) > 0:
        print "\nmd5sum mismatch (File name, sum in DB, sum on disk) --------- "
        for fname in sorted(comparison_info['md5sum']):
            fdb = files_from_db[fname]
            fdisk = files_from_disk[fname]
            print "\t%s %s %s" % (fname, fdb['md5sum'], fdisk['md5sum'])

    # Print out files that have multiple paths on disk
    if len(duplicates) > len(pdup):
        print "\nThe following files have multiple disk paths on disk (path  filesize):"
    for dup in sorted(duplicates):
        if dup not in pdup:
            listing = {}
            for fls in duplicates[dup]:
                listing[fls['relpath']] = fls['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if dup in files_from_db and files_from_db[dup]['path'] == pth:
                    addon = "  (DB Match)"
                print "      %s %s/%s   %i%s" % (start, pth, dup, listing[pth], addon)

    # Print out files that have multiple endtries in the DB
    if len(db_duplicates) > 0:
        print "\nThe following files have multiple entries in the database (path  filesize):"
    for dup in sorted(db_duplicates):
        listing = {}
        for fls in db_duplicates[dup]:
            listing[fls['relpath']] = fls['filesize']
        first = True
        for pth in sorted(listing):
            start = " "
            if first:
                start = "*"
                first = False
            addon = ""
            if dup in files_from_disk and files_from_disk[dup]['path'] == pth:
                addon = "  (Disk Match)"
            print "      %s %s/%s   %i%s" % (start, pth, dup, listing[pth], addon)

def multi_compare(dbh, pfwids, args):
    """ Method to iterate over pfw_attempt_id's and run the comparison script

        Parameters
        ----------
        dbh : database handle
        pfwids : result of querying a table for pfw_attempt_ids, usually a list of single element tuples
        args : an instance of Args containing the command line arguments

        Returns
        -------
        A sum of the results of do_compare
    """
    count = 0
    length = len(pfwids)
    if args.start_at > length or args.start_at < 1:
        print "Error: Starting index is beyond bounds of list."
        return 1
    offset = int(args.start_at) - 1
    if args.end_at != 0:
        if args.end_at < args.start_at:
            print "Error: Ending index is less than starting index."
            return 1
        if args.end_at > length:
            print "Error: Ending index is beyond bounds of list."
            return 1
        pfwids = pfwids[offset:args.end_at]
    else:
        pfwids = pfwids[offset:]
    for i, pdwi in enumerate(pfwids):
        print "--------------------- Starting %i/%i ---------------------" % (i + 1 + offset, length)
        args.pfwid = pdwi
        count += do_compare(dbh, args)
    return count


def run_compare(args):
    """ Method to determine what data need to be compared

        Parameters
        ----------
        args : list of command line arguments

        Returns
        -------
        the result from do_compare

    """
    # connect to the database
    if args.dbh is None:
        dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
    else:
        dbh = args.dbh
    # do some quick validation
    if args.date_range and args.pfwid:
        print "Date_range was specified, thus pfwid cannot be."
    if args.relpath and (args.reqnum or args.unitname or args.attnum or args.tag or args.pfwid):
        print "Relpath was specified, thus reqnum, unitname, attnum, tag, and pfwid cannot be specified."
        sys.exit(1)
    if args.reqnum and (args.tag or args.pfwid):
        print "Reqnum was specified, thus tag and pfwid cannot be specified."
        sys.exit(1)
    if args.tag and args.pfwid:
        print "Tag was specified, thus pfwid cannot be specified."
        sys.exit(1)
    if (args.unitname or args.attnum) and not args.reqnum:
        print "Unitname and/or attnum were specified, but reqnum was not, please supply a reqnum and run again."
        sys.exit(1)

    # if dealing with a date range then get the relevant pfw_attempt_ids
    if args.date_range:
        dates = args.date_range.split(',')
        whereclause = []
        if len(dates) == 1:
            whereclause.append("submittime>=TO_DATE('%s 00:00:01', 'YYYY-MM-DD HH24:MI:SS') and submittime<=TO_DATE('%s 23:59:59', 'YYYY-MM-DD HH24:MI:SS')" % (dates[0], dates[0]))
        else:
            whereclause.append("submittime>=TO_DATE('%s 00:00:01', 'YYYY-MM-DD HH24:MI:SS') and submittime<=TO_DATE('%s 23:59:59', 'YYYY-MM-DD HH24:MI:SS')" % (dates[0], dates[1]))
        if args.pipeline:
            whereclause.append("subpipeprod='%s'" % args.pipeline)
        if args.reqnum:
            whereclause.append("reqnum=%s" % (args.reqnum))
            if args.unitname:
                whereclause.append("unitname='%s'" % (args.unitname))
            if args.attnum:
                whereclause.append("attnum=%s" % (args.attnum))
        elif args.tag:
            whereclause.append("id in (select pfw_attempt_id from proctag where tag='%s')" % (args.tag))
        pfwids = dbutils.get_pfw_attempt_ids_where(dbh, whereclause, 'id')

        if not args.silent:
            print "Found %i pfw_attempt_id's for the given date range (and any qualifying tag/reqnum)" % (len(pfwids))
        if len(pfwids) == 0:
            return 0
        return multi_compare(dbh, pfwids, args)

    pfwids = []
    # if dealing with a tag then get the relevant pfw_attempt_ids
    if args.tag:
        pfwids = dbutils.get_pfw_attempt_id_from_tag(dbh, args.tag)
    # if dealing with a triplet
    elif args.reqnum:
        pfwids = dbutils.get_pfw_attempt_ids_from_triplet(dbh, args)
        args.reqnum = None
        args.unitname = None
        args.attnum = None
    elif args.pfwid and ',' in args.pfwid:
        pfwids = args.pfwid.split(',')
    # if only a single comparison was requested (single pfw_attempt_id, triplet (reqnum, uniname, attnum), or path)
    if len(pfwids) == 0:
        return do_compare(dbh, args)
    if len(pfwids) == 1:
        args.pfwid = pfwids[0]
        return do_compare(dbh, args)
    pfwids.sort() # put them in order
    return multi_compare(dbh, pfwids, args)

def do_compare(dbh, args):
    """ Main control """
    archive_root, archive_path, relpath, _, pfwid = gather_data(dbh, args)

    if not relpath:
        print '  Connot do comparison for pfw_attempt_id %s' % (pfwid)
        return 1
    #print archive_root
    if args.debug:
        print "From DB"
    files_from_db, db_duplicates = dbutils.get_files_from_db(dbh, relpath, args.archive, pfwid, None, debug=args.debug)
    if args.debug:
        print "From disk"
    files_from_disk, duplicates = diskutils.get_files_from_disk(relpath, archive_root, args.md5sum, args.debug)
    if args.debug:
        print "Compare"
    comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, duplicates, args.md5sum, args.debug, archive_root)
    # print the full results unless requested not to
    if not args.script and not args.silent:
        print "\nPath = %s" % (archive_path)
        print "Archive name = %s" % args.archive
        addon = ""
        dbaddon = ""
        if len(duplicates) > 0:
            addon += "(%i are distinct)" % len(files_from_disk)
        if len(db_duplicates) > 0:
            dbaddon += "(%i are distinct)" % len(files_from_db)
        print "Number of files from db   = %i   %s" % (len(files_from_db) + len(db_duplicates), dbaddon)
        print "Number of files from disk = %i   %s" % (len(files_from_disk) + len(duplicates), addon)
        if len(duplicates) > 0:
            print "Files with multiple paths on disk  = %i" % len(duplicates)
        # print summary of comparison
        print "Comparison Summary"

        print "\tEqual:\t%i" % len(comparison_info['equal'])
        print "\tDB only:\t%i" % len(comparison_info['dbonly'])
        print "\tDisk only:\t%i" % len(comparison_info['diskonly'])
        print "\tMismatched paths:\t%i" % len(comparison_info['path'])
        print "\tMismatched filesize:\t%i" % len(comparison_info['filesize'])
        if 'md5sum' in comparison_info:
            print "\tMismatched md5sum:\t%i" % len(comparison_info['md5sum'])
        print ""

        if args.debug:
            print_all_files(comparison_info, files_from_db, files_from_disk)
        elif args.verbose:
            diff_files(comparison_info, files_from_db, files_from_disk, args.md5sum, True, duplicates, db_duplicates)
        if len(comparison_info['dbonly']) == len(comparison_info['diskonly']) == len(comparison_info['path']) == len(comparison_info['filesize']) == 0:
            if 'md5sum' in comparison_info:
                if len(comparison_info['md5sum']) != 0:
                    print "md5sum  ERROR"
                    return 1
            return 0
        return 1

    else:
        if args.pfwid is not None:
            loc = "%s" % (args.pfwid)
        elif args.relpath is None:
            loc = "%s  %s  %s" % (args.reqnum, args.unitname, args.attnum)
        else:
            loc = args.relpath
        if len(comparison_info['dbonly']) == len(comparison_info['diskonly']) == len(comparison_info['path']) == len(comparison_info['filesize']) == 0:
            if 'md5sum' in comparison_info:
                if len(comparison_info['md5sum']) != 0:
                    if not args.silent:
                        print "%s  ERROR" % loc
                    return 1
            if not args.silent:
                print "%s  OK" % loc
            return 0
        if not args.silent:
            print "%s  ERROR" % loc
        return 1

# pylint: disable=unused-argument
def compare(dbh=None, des_services=None, section=None, archive='desar2home', reqnum=None, unitname=None,
            attnum=None, relpath=None, pfwid=None, date_range=None, pipeline=None,
            md5sum=False, debug=False, script=False, verbose=False, silent=True,
            tag=None, start_at=1, end_at=0, log=None):
    """ Entry point
    """
    return run_compare(fmutils.DataObject(**locals()))
