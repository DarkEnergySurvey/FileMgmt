#!/usr/bin/env python3
""" Module to migrate files from one file system to another.

"""
import os
import shutil
from pathlib import Path
#import datetime
import time

from despymisc import miscutils
from despydmdb import desdmdbi
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

class Message:
    """ Class for passing messages to main process
    """
    def __init__(self, window, msg, pfwid, iteration=None, count=None, err=False):
        self.win = window
        self.msg = msg
        self.iteration = iteration
        self.count = count
        self.err = err
        self.pfwid = pfwid

class Migration:
    """ Class for migrating data

    """
    def __init__(self, win, args, pfwids, event, que=None):
        self.pfwids = pfwids
        if args.dbh is None:
            args.dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
        self.args = args
        self.win = win
        self.event = event
        self.que = que
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
        self.results = {"null": [],
                        "comp": []}
        self.paths = {"null": [],
                      "comp": []}
        self.count = 0
        self.currnewpath = None
        self.copied_files = []
        self.status = 0
        self.iteration = 0
        self.halt = False
        self.number = 0
        self.length = 1

        if not self.pfwids:
            _ = self.do_migration()
        elif len(self.pfwids) == 1:
            self.pfwid = self.pfwids[0]
            self.args.pfwid = self.pfwid
            _ = self.do_migration()
        else:
            self.pfwids.sort() # put them in order
            self.multi_migrate()


    def __del__(self):
        if self.dbh:
            self.dbh.close()

    def update(self, msg=None, err=False):
        """ Method to report the progress of the job

        """
        if self.silent:
            return
        if self.que is not None:
            if msg is not None:
                self.que.put_nowait(Message(self.win, f"Processing {self.pfwid}  ({self.number+1}/{self.length})\n{msg}", pfwid=self.pfwid, err=err))
            else:
                self.que.put_nowait(Message(self.win, None, self.pfwid, self.iteration, self.count))
        else:
            if msg is not None:
                print(msg)
            else:
                self.printProgressBar()

    def check_status(self):
        """ Method to check whether the processing should continue
        """
        if self.halt:
            return True
        if self.event is not None:
            if self.event.is_set():
                self.rollback(self.currnewpath)
        return self.halt

    def printProgressBar(self, length = 100, fill = '█', printEnd = "\r"):
        """ Print a progress bar
        """
        if self.silent:
            return
        percent = (f"{self.iteration:d}/{self.count:d}")
        filledLength = int(length * self.iteration // self.count)
        pbar = fill * filledLength + '-' * (length - filledLength)
        print(f'\rProgress: |{pbar}| {percent}', end = printEnd)

    def rollback(self, newpath=None):
        """ Method to undo any changes if something goes wrong
        """
        if self.halt:
            return
        self.halt = True
        if self.status != 0:
            self.update("The process cannot be interrupted at this stage")
            return

        self.update("Rolling back any changes...")
        if self.dbh:
            self.dbh.rollback()
        bad_files = []
        self.count = len(self.copied_files)
        if self.count > 0:
            self.iteration = 0
            self.update()
            for i, f in enumerate(self.copied_files):
                try:
                    os.remove(f)
                    self.iteration = i+1
                    self.update()
                except:
                    bad_files.append(f)
            if newpath is not None:
                removeEmptyFolders(os.path.join(self.archive_root, newpath))
            if bad_files:
                with open(f"{self.pfwid}.undel", 'w', encoding="utf-8") as fh:
                    for f in bad_files:
                        fh.write(f"    {f}\n")
                self.update(f"Could not remove {len(bad_files)} copied files. See {self.pfwid}.undel for a list.", True)

    def check_permissions(self, files_from_db):
        """ Check the permissions of the initial files to make sure they can be read and written
        """
        bad_files = []
        self.update("Checking file permissions")
        self.iteration = 0
        self.update()
        for fname, items in files_from_db.items():
            if self.check_status():
                return False
            if not os.access(os.path.join(self.archive_root, items['path'], fname), os.R_OK|os.W_OK):
                bad_files.append(fname)
            self.iteration += 1
            self.update()

        if bad_files:
            with open(f"{self.pfwid}.badperm", 'w', encoding="utf-8") as fh:
                for f in bad_files:
                    fh.write(f"{f}\n")

            self.update(f"Some files do not have rw permissions. See {self.pfwid}.badperm for a list.", True)
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
        self.update(f"Copying {self.count} files...")
        self.iteration = 0
        self.update()
        for fname, items in files_from_db.items():
            if self.check_status():
                return
            if self.current is not None:
                dst = items['path'].replace(self.current, self.destination)
            else:
                dst = self.destination + items['path']
            (_, filename, compress) = miscutils.parse_fullname(fname, miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION)
            path = Path(os.path.join(self.archive_root, dst))
            try:
                path.mkdir(parents=True, exist_ok=True)
            except:
                self.update(f"Error making directory {os.path.join(self.archive_root, dst)}", True)
                time.sleep(2)
                self.rollback()
                raise
            try:
                shutil.copy2(os.path.join(self.archive_root, items['path'], fname), os.path.join(self.archive_root, dst, fname))
                self.copied_files.append(os.path.join(self.archive_root, dst, fname))
            except:
                self.update(f"Error copying file from {os.path.join(self.archive_root, items['path'], fname)} to {os.path.join(self.archive_root, dst, fname)}", True)
                time.sleep(2)
                self.rollback()
                raise
            if compress is None:
                self.results['null'].append({'pth': dst, 'fn':filename})#,
                self.paths['null'].append({'orig': items['path']})
            else:
                self.results['comp'].append({'pth': dst, 'fn':filename, 'comp':compress})
                self.paths['comp'].append({'orig': items['path']})
            self.iteration += 1
            self.update()

    def do_migration(self):
        """ Method to migrate the data

            Parameters
            ----------
            args : list of command line arguments

            Returns
            -------
            the result
        """
        self.update("Gathering file info from DB")
        self.archive_root, _, relpath, _, pfwid = compare.gather_data(self.dbh, self.args)
        if not relpath:
            self.update(f'  Connot do migration for pfw_attempt_id, no relpath found {pfwid}', True)
            return 1
        newpath = None
        if self.current is not None:
            newpath = relpath.replace(self.current, self.destination)
        else:
            newpath = os.path.join(self.destination, relpath)
        if newpath == relpath:
            self.update(f"  ERROR: new path is the same as the original {newpath} == {relpath}", True)
            return 1
        newarchpath = os.path.join(self.archive_root, newpath)
        self.currnewpath = newpath

        files_from_db, _ = dbutils.get_files_from_db(self.dbh, relpath, self.archive, pfwid, None, debug=self.debug)
        self.count = len(files_from_db)

        # make the root directory
        path = Path(newarchpath)
        path.mkdir(parents=True, exist_ok=True)
        if not self.check_permissions(files_from_db):
            return 0
        if self.check_status():
            return 1
        self.migrate(files_from_db)
        if self.check_status():
            return 1
        self.update("Updating database...")
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
            self.update("Error updating the database entries, rolling back any DB changes.", True)
            time.sleep(2)
            self.rollback()
            raise
        # get new file info from db
        if self.check_status():
            return 1
        self.update("Running comparison of new files and database...")
        files_from_db, _ = dbutils.get_files_from_db(self.dbh, newpath, self.archive, pfwid, None, debug=self.debug)
        files_from_disk, duplicates = diskutils.get_files_from_disk(newpath, self.archive_root, True, self.debug)

        comparison_info = diskutils.compare_db_disk(files_from_db, files_from_disk, duplicates, True, self.debug, self.archive_root)
        error = False
        if len(comparison_info['dbonly']) > 0:
            error = True
            self.update(f"Error {len(comparison_info['dbonly']):d} files found only in the DB", True)
        if len(comparison_info['diskonly']) > 0:
            error = True
            self.update(f"Error {len(comparison_info['diskonly']):d} files only found on disk", True)
        if len(comparison_info['path']) > 0:
            error = True
            self.update(f"Error {len(comparison_info['path']):d} files have mismatched paths", True)
        if len(comparison_info['filesize']) > 0:
            error = True
            self.update(f"Error {len(comparison_info['filesize']):d} files have mismatched file sizes", True)
        if len(comparison_info['md5sum']) > 0:
            error = True
            self.update(f"Error {len(comparison_info['md5sum']):d} files have mismatched md5sums", True)
        if error:
            self.rollback()
            return 1

        # remove old files
        self.update("     Complete, all files match")
        rml = []
        for i, item in enumerate(self.results['comp']):
            fname = item['fn'] + item['comp']
            rml.append(os.path.join(self.archive_root, self.paths['comp'][i]['orig'], fname))
        for i, item in enumerate(self.results['null']):
            fname = item['fn']
            rml.append(os.path.join(self.archive_root, self.paths['null'][i]['orig'], fname))
        if self.check_status():
            return 1

        cannot_del = []
        self.status = 1
        self.dbh.commit()
        self.update("Removing original files")
        self.iteration = 0
        self.update()
        for i, r in enumerate(rml):
            try:
                os.remove(r)
                self.iteration = i + 1
                self.update()
            except:
                cannot_del.append(r)
        removeEmptyFolders(os.path.join(self.archive_root,relpath))
        self.status = 0

        if cannot_del:
            with open(f"{self.pfwid}.undel", 'w', encoding="utf-8") as fh:
                for f in cannot_del:
                    fh.write(f"    {f}\n")
            self.update(f"Cannot delete some files. See {self.pfwid}.undel for a list.", True)
        return 0

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
        self.length = len(self.pfwids)

        for i, pdwi in enumerate(self.pfwids):
            self.number = i
            if self.check_status():
                return
            self.copied_files = []
            self.results = {"null": [],
                            "comp": []}
            self.paths = {"null": [],
                          "comp": []}
            self.count = 0

            self.pfwid = pdwi
            self.args.pfwid = pdwi
            self.do_migration()
