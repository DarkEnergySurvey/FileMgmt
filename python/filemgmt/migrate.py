#!/usr/bin/env python3
""" Module to migrate files from one file system to another.

"""
import os
import shutil
from pathlib import Path

from despymisc import miscutils
import filemgmt.disk_utils_local as diskutils
import filemgmt.db_utils_local as dbutils
import filemgmt.compare_utils as compare

def migrate(files_from_db, current, destination, archive_root):
    results = {"null": [],
               "comp": [])
    for fname, items in files_from_db.items():
        if current is not None:
            dst = items['path'].replace(current, destination)
        else:
            dst = destination + items['path']
        (_, filename, compress) = miscutils.parse_fullname(fname, miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION)
        path = Path(os.path.join(archive_root, dst))
        path.mkdir(parents=True, exist_ok=True)
        print(f"mkdir {os.path.join(archive_root, dst)}")
        shutil.copy2(os.path.join(archive_root, items['path'], fname), os.path.join(archive_root, dst, fname))
        print(f"moving {os.path.join(archive_root, items['path'], fname)} to {os.path.join(archive_root, dst, fname)}")
        if compress is None:
            results['null'].append({'pth': dst, 'fn':filename})#, 'orig': items['path']})
        else:
            results['comp'].append({'pth': dst, 'fn':filename, 'comp':compress})#, 'orig': items['path']})
    return results

def do_migration(dbh, args):
    """ Method to migrate the data

        Parameters
        ----------
        args : list of command line arguments

        Returns
        -------
        the result
    """
    archive_root, _, relpath, _, pfwid = compare.gather_data(dbh, args)
    if not relpath:
        print(f'  Connot do migration for pfw_attempt_id, no relpath found {pfwid}')
        return 1
    newpath = None
    if args.current is not None:
        newpath = relpath.replace(args.current, args.destination)
    else:
        newpath = os.path.join(args.destination, relpath)
    if newpath == relpath:
        print(f"  ERROR: new path is the same as the original {newpath} == {relpath}")
        return 1
    newarchpath = os.path.join(archive_root, newpath)
    #print archive_root
    if args.debug:
        print("From DB")
    files_from_db, db_duplicates = dbutils.get_files_from_db(dbh, relpath, args.archive, pfwid, None, debug=args.debug)
    # make the root directory
    path = Path(newarchpath)
    path.mkdir(parents=True, exist_ok=True)

    newloc = migrate(files_from_db, args.current, args.destination, archive_root)
    if newloc['comp'] :
        upsql = "update file_archive_info set path=:pth where filename=:fn and compression=:comp"
        print(upsql)
        for item in newloc:
            print(f"    {item}")
        curs = dbh.cursor()
        curs.executemany(upsql, newloc['comp'])
    if newloc['null'] :
        upsql = "update file_archive_info set path=:pth where filename=:fn and compression is NULL"
        print(upsql)
        for item in newloc:
            print(f"    {item}")
        curs = dbh.cursor()
        curs.executemany(upsql, newloc['null'])
    print(f"update pfw_attempt set archive_path={newpath} where id={pfwid}")
    curs.execute(f"update pfw_attempt set archive_path='{newpath}' where id={pfwid}")

    dbh.commit()
    # get new file info from db

    files_from_db, db_duplicates = dbutils.get_files_from_db(dbh, newpath, args.archive, pfwid, None, debug=args.debug)
    files_from_disk, duplicates = diskutils.get_files_from_disk(newpath, archive_root, True, args.debug)

    comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, duplicates, True, args.debug, archive_root)
    error = False
    if len(comparison_info['dbonly']) > 0:
        error = True
        print(f"Error {len(comparison_info['dbonly']):d} files found only in the DB")
    if len(comparison_info['diskonly']) > 0:
        error = True
        print(f"Error {len(comparison_info['diskonly']):d} files only found on disk")
    if len(comparison_info['path']) > 0:
        error = True
        print(f"Error {len(comparison_info['path']):d} files have mismatched paths")
    if len(comparison_info['filesize']) > 0:
        error = True
        print(f"Error {len(comparison_info['filesize']):d} files have mismatched file sizes")
    if len(comparison_info['md5sum']) > 0:
        error = True
        print(f"Error {len(comparison_info['md5sum']):d} files have mismatched md5sums")
    if error:
        print("Error summary")
        compare.diff_files(comparison_info, files_from_db, files_from_disk, True, True, duplicates, db_duplicates)
        #upsql = "update file_archive_info set path=:orig where filename=:fn and compression=:comp"
        #curs = dbh.cursor()
        #curs.executemany(upsql, newloc)
        return 1
    # remove old files

    rml = []
    for item in newloc:
        fname = item['fn']
        if item['comp'] is not None:
            fname += item['comp']
        rml.append(os.path.join(archive_root, item['orig'], fname))
    for r in rml:
        print(r)
    ok = False

    while not ok:
        res = input("Delete the above files[y/n]?")
        if res.lower() == 'y':
            for r in rml:
                os.remove(r)
            return 0
        if res.lower() == 'n':
            return 0

def multi_migrate(dbh, pfwids, args):
    """ Method to iterate over pfw_attempt_id's and run the migration script

        Parameters
        ----------
        dbh : database handle
        pfwids : result of querying a table for pfw_attempt_ids, usually a list of single element tuples
        args : an instance of Args containing the command line arguments

        Returns
        -------
        A summary of the results of do_migration
    """
    count = 0
    length = len(pfwids)
    if args.start_at > length or args.start_at < 1:
        print("Error: Starting index is beyond bounds of list.")
        return 1
    offset = int(args.start_at) - 1
    if args.end_at != 0:
        if args.end_at < args.start_at:
            print("Error: Ending index is less than starting index.")
            return 1
        if args.end_at > length:
            print("Error: Ending index is beyond bounds of list.")
            return 1
        pfwids = pfwids[offset:args.end_at]
    else:
        pfwids = pfwids[offset:]
    for i, pdwi in enumerate(pfwids):
        print(f"--------------------- Starting {i + 1 + offset:d}/{length:d} ---------------------")
        args.pfwid = pdwi
        count += do_migration(dbh, args)
    return count


def run_migration(args):
    """ Method to migrate the files
    """
    (args, pfwids) = compare.determine_ids(args)
    # if dealing with a date range then get the relevant pfw_attempt_ids
    if args.date_range:
        if not pfwids:
            return 0
        return multi_migrate(args.dbh, pfwids, args)

    # if only a single comparison was requested (single pfw_attempt_id, triplet (reqnum, uniname, attnum), or path)
    if not pfwids:
        return do_migration(args.dbh, args)
    if len(pfwids) == 1:
        args.pfwid = pfwids[0]
        return do_migration(args.dbh, args)
    pfwids.sort() # put them in order
    return multi_migrate(args.dbh, pfwids, args)
