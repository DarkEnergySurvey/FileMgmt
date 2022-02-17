#!/usr/bin/env python3
""" Module to migrate files from one file system to another.

"""
import os
import shutil
from pathlib import Path
#import datetime
import time

from despymisc import miscutils
from filemgmt import fmutils


class Migration(fmutils.FileManager):
    def __init__(self, win, args, pfwids, event, que=None):
        fmutils.FileManager.__init__(self, win, args, pfwids, event, que)
        self.destination = args.destination
        self.current = args.current
        self.results = {"null": [],
                        "comp": []}
        self.paths = {"null": [],
                      "comp": []}
        self.copied_files = []
        self.md5sum = True

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
                fmutils.removeEmptyFolders(os.path.join(self.archive_root, newpath))
            if bad_files:
                with open(f"{self.pfwid}.undel", 'w', encoding="utf-8") as fh:
                    for f in bad_files:
                        fh.write(f"    {f}\n")
                self.update(f"Could not remove {len(bad_files)} copied files. See {self.pfwid}.undel for a list.", True)


    def migrate(self):
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
        for fname, items in self.files_from_db.items():
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

    def do_task(self):
        """ Method to migrate the data

            Parameters
            ----------
            args : list of command line arguments

            Returns
            -------
            the result
        """
        self.copied_files = []
        self.results = {"null": [],
                        "comp": []}
        self.paths = {"null": [],
                      "comp": []}

        self.update("Gathering file info from DB")
        self.gather_data()
        if not self.relpath:
            self.update(f'  Connot do migration for pfw_attempt_id, no relpath found {self.pfwid}', True)
            return 1
        newpath = None
        if self.current is not None:
            newpath = self.relpath.replace(self.current, self.destination)
        else:
            newpath = os.path.join(self.destination, self.relpath)
        if newpath == self.relpath:
            self.update(f"  ERROR: new path is the same as the original {newpath} == {self.relpath}", True)
            return 1
        newarchpath = os.path.join(self.archive_root, newpath)
        self.currnewpath = newpath

        self.get_files_from_db()
        self.count = len(self.files_from_db)

        # make the root directory
        path = Path(newarchpath)
        path.mkdir(parents=True, exist_ok=True)
        if not self.check_permissions(self.files_from_db):
            return 0
        if self.check_status():
            return 1
        self.migrate()
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
            curs.execute(f"update pfw_attempt set archive_path='{newpath}' where id={self.pfwid}")
        except:
            self.update("Error updating the database entries, rolling back any DB changes.", True)
            time.sleep(2)
            self.rollback()
            raise
        # get new file info from db
        if self.check_status():
            return 1
        oldpath = self.relpath
        self.relpath = newpath
        self.update("Running comparison of new files and database...")
        self.get_files_from_db()
        self.get_files_from_disk()

        comparison_info = self.compare_db_disk()
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
        fmutils.removeEmptyFolders(os.path.join(self.archive_root, oldpath))
        self.status = 0

        if cannot_del:
            with open(f"{self.pfwid}.undel", 'w', encoding="utf-8") as fh:
                for f in cannot_del:
                    fh.write(f"    {f}\n")
            self.update(f"Cannot delete some files. See {self.pfwid}.undel for a list.", True)
        return 0
