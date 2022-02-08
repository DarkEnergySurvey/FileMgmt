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


def removeEmptyFolders(path, removeRoot=True):
    """ Function to remove empty folders
    """
    if not os.path.isdir(path):
        return

    # remove empty subfolders
    files = os.listdir(path)
    if len(files):
        for f in files:
            fullpath = os.path.join(path, f)
            if os.path.isdir(fullpath):
                removeEmptyFolders(fullpath)

    # if folder empty, delete it
    files = os.listdir(path)
    if len(files) == 0 and removeRoot:
        os.rmdir(path)

class Migration:
    """ Class for migrating data

    """
    def __init__(self, args):
        (self.args, self.pfwids) = compare.determine_ids(args)
        self.dbh = self.args.dbh
        self.des_services = self.args.des_services
        self.section = self.args.section
        self.archive = self.args.archive
        self.destination = self.args.destination
        self.current = self.args.current
        self.reqnum = self.args.reqnum
        self.relpath = self.args.relpath
        self.unitname = self.args.unitname
        self.attnum = self.args.attnum
        self.verbose = self.args.verbose
        self.debug = self.args.debug
        self.script = self.args.script
        self.pfwid = self.args.pfwid
        self.silent = self.args.silent
        self.tag = self.args.tag
        self.archive_root = None
        self.copied_files = []
        self.force = args.force
        self.results = {"null": [],
                        "comp": []}
        self.paths = {"null": [],
                      "comp": []}
        self.count = 0

    def __del__(self):
        if self.dbh:
            self.dbh.close()

    def printProgressBar(self, iteration, length = 100, fill = '█', printEnd = "\r"):
        """ Print a progress bar
        """
        percent = (f"{iteration:d}/{self.count:d}")
        filledLength = int(length * iteration // self.count)
        pbar = fill * filledLength + '-' * (length - filledLength)
        print(f'\rProgress: |{pbar}| {percent}', end = printEnd)

    def rollback(self, newpath=None):
        """ Method to undo any changes if something goes wrong
        """
        print("\nRolling back any changes...\n")
        if self.dbh:
            self.dbh.rollback()
        bad_files = []
        self.count = len(self.copied_files)
        self.printProgressBar(0)
        for i, f in enumerate(self.copied_files):
            try:
                os.remove(f)
                self.printProgressBar(i+1)
            except:
                bad_files.append(f)
        if newpath is not None:
            removeEmptyFolders(os.path.join(self.archive_root, newpath))
        if bad_files:
            print(f"Could not remove {len(bad_files)} copied files:")
            for f in bad_files:
                print(f"    {f}")

    def go(self):
        """ Method to migrate the files
        """
        # if dealing with a date range then get the relevant pfw_attempt_ids
        #if args.date_range:
        #    if not pfwids:
        #        return 0
        #    return multi_migrate(args.dbh, pfwids, args)

        # if only a single comparison was requested (single pfw_attempt_id, triplet (reqnum, uniname, attnum), or path)
        if not self.pfwids:
            return self.do_migration()
        if len(self.pfwids) == 1:
            self.pfwid = self.pfwids[0]
            return self.do_migration()
        self.pfwids.sort() # put them in order
        return self.multi_migrate()

    def check_permissions(self, files_from_db):
        """ Check the permissions of the initial files to make sure they can be read and written
        """
        bad_files = []
        print("Checking file permissions")

        self.printProgressBar(0)
        done = 0
        for fname, items in files_from_db.items():
            if not os.access(os.path.join(self.archive_root, items['path'], fname), os.R_OK|os.W_OK):
                bad_files.append(fname)
            done += 1
            self.printProgressBar(done)

        if bad_files:
            print("Some files do not have rw permissions:")
            for f in bad_files:
                print(f"   {f}")
            return False
        return True

    def migrate(self, files_from_db):
        """ Function to copy files from one archive section to another.

            Parameters
            ----------
            files_from_db: dict
                Dictionary of files and descriptors to copy

            current: str
                The current root path of the files

            destination: str
                The destination path for the files

            archive_root: str
                The archive root path
        """
        print(f"\n\nCopying {self.count} files...")
        done = 0
        self.printProgressBar(0)
        for fname, items in files_from_db.items():
            if self.current is not None:
                dst = items['path'].replace(self.current, self.destination)
            else:
                dst = self.destination + items['path']
            (_, filename, compress) = miscutils.parse_fullname(fname, miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION)
            path = Path(os.path.join(self.archive_root, dst))
            try:
                path.mkdir(parents=True, exist_ok=True)
            except:
                print(f"\nError making directory {os.path.join(self.archive_root, dst)}")
                self.rollback()
                raise
            try:
                shutil.copy2(os.path.join(self.archive_root, items['path'], fname), os.path.join(self.archive_root, dst, fname))
                self.copied_files.append(os.path.join(self.archive_root, dst, fname))
            except:
                print(f"\nError copying file from {os.path.join(self.archive_root, items['path'], fname)} to {os.path.join(self.archive_root, dst, fname)}")
                self.rollback()
                raise
            if compress is None:
                self.results['null'].append({'pth': dst, 'fn':filename})#,
                self.paths['null'].append({'orig': items['path']})
            else:
                self.results['comp'].append({'pth': dst, 'fn':filename, 'comp':compress})
                self.paths['comp'].append({'orig': items['path']})
            done += 1
            self.printProgressBar(done)

    def do_migration(self):
        """ Method to migrate the data

            Parameters
            ----------
            args : list of command line arguments

            Returns
            -------
            the result
        """
        print("Gathering file info from DB")
        self.archive_root, _, relpath, _, pfwid = compare.gather_data(self.dbh, self.args)
        if not relpath:
            print(f'  Connot do migration for pfw_attempt_id, no relpath found {pfwid}')
            return 1
        newpath = None
        if self.current is not None:
            newpath = relpath.replace(self.current, self.destination)
        else:
            newpath = os.path.join(self.destination, relpath)
        if newpath == relpath:
            print(f"  ERROR: new path is the same as the original {newpath} == {relpath}")
            return 1
        newarchpath = os.path.join(self.archive_root, newpath)
        #print archive_root
        if self.debug:
            print("From DB")
        files_from_db, db_duplicates = dbutils.get_files_from_db(self.dbh, relpath, self.archive, pfwid, None, debug=self.debug)
        self.count = len(files_from_db)
        # make the root directory
        path = Path(newarchpath)
        path.mkdir(parents=True, exist_ok=True)
        if not self.check_permissions(files_from_db):
            return 0
        self.migrate(files_from_db)
        print("\n\nUpdating database...")
        try:
            if self.results['comp'] :
                upsql = "update file_archive_info set path=:pth where filename=:fn and compression=:comp"
                curs = self.dbh.cursor()
                curs.executemany(upsql, self.results['comp'])
            if self.results['null'] :
                upsql = "update file_archive_info set path=:pth where filename=:fn and compression is NULL"
                curs = self.dbh.cursor()
                curs.executemany(upsql, self.results['null'])
            curs.execute(f"update pfw_attempt set archive_path='{newpath}' where id={pfwid}")
        except:
            print("Error updating the database entries, rolling back any DB changes.")
            self.rollback()
            raise
        # get new file info from db

        print("Running comparison of new files and database...")
        files_from_db, db_duplicates = dbutils.get_files_from_db(self.dbh, newpath, self.archive, pfwid, None, debug=self.debug)
        files_from_disk, duplicates = diskutils.get_files_from_disk(newpath, self.archive_root, True, self.debug)

        comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, duplicates, True, self.debug, self.archive_root)
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
            self.rollback()
            return 1
        # remove old files
        print("     Complete, all files match")
        rml = []
        for i, item in enumerate(self.results['comp']):
            fname = item['fn'] + item['comp']
            rml.append(os.path.join(self.archive_root, self.paths['comp'][i]['orig'], fname))
        for i, item in enumerate(self.results['null']):
            fname = item['fn']
            rml.append(os.path.join(self.archive_root, self.paths['null'][i]['orig'], fname))

        #print('\n\n')
        #for r in rml:
        #    print(r)
        ok = False
        print(f"\n{os.path.join(self.archive_root,relpath)}\n")
        cannot_del = []
        while not ok:
            res = ""
            if not self.force:
                res = input("Delete files in the above directory[y/n]?")
            if res.lower() == 'y' or self.force:
                self.dbh.commit()
                self.printProgressBar(0)
                for i, r in enumerate(rml):
                    try:
                        os.remove(r)
                        self.printProgressBar(i+1)
                    except:
                        cannot_del.append(r)
                removeEmptyFolders(os.path.join(self.archive_root,relpath))
                ok = True
            elif res.lower() == 'n':
                self.rollback(newpath)
                return 0
            print()
        if cannot_del:
            print("Cannot delete the following files:")
            for f in cannot_del:
                print(f"    {f}")

    def multi_migrate(self):
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
        length = len(self.pfwids)

        for i, pdwi in enumerate(self.pfwids):
            print(f"--------------------- Starting {pdwi}    {i + 1:d}/{length:d} ---------------------")
            self.pfwid = pdwi
            self.args.pfwid = pdwi
            self.do_migration()
