#!/usr/bin/env python3
""" Module to migrate files from one file system to another.

"""
import os
import tarfile

import filemgmt.filemgmt_db as fmdb
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

class CompactLogs:
    """ Class for migrating data

    """
    def __init__(self, win, args, pfwids, event, que=None):
        self.pfwids = pfwids
        #if args.dbh is None:
        args.dbh = fmdb.FileMgmtDB({'des_services': args.des_services,
                                    'section': args.section})
        args.dbh.dynam_load_ftmgmt('logtar')

        self.args = args
        self.win = win
        self.event = event
        self.que = que
        self.dbh = self.args.dbh
        self.des_services = self.args.des_services
        self.section = self.args.section
        self.archive = self.args.archive
        self.verbose = self.args.verbose
        self.debug = self.args.debug
        self.pfwid = self.args.pfwid
        self.silent = self.args.silent
        self.tag = self.args.tag
        self.archive_root = None
        self.results = {"null": [],
                        "comp": []}
        self.paths = {"null": [],
                      "comp": []}
        self.count = 0
        self.status = 0
        self.iteration = 0
        self.halt = False
        self.number = 0
        self.length = 1
        self.tarfile = None

        if not self.pfwids:
            _ = self.doCompact()
        elif len(self.pfwids) == 1:
            self.pfwid = self.pfwids[0]
            self.args.pfwid = self.pfwid
            _ = self.doCompact()
        else:
            self.pfwids.sort() # put them in order
            self.multi_compact()


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
        elif msg is not None:
            print(msg)

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

    def check_status(self):
        """ Method to check whether the processing should continue
        """
        if self.halt:
            return True
        if self.event is not None:
            if self.event.is_set():
                self.rollback()
        return self.halt

    def rollback(self):
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
        try:
            os.remove(self.tarfile)
        except:
            self.update(f"Could not remove {self.archive_root}/log/{self.tarfile}", True)

    def doCompact(self):
        """ Method to migrate the data

            Parameters
            ----------
            args : list of command line arguments

            Returns
            -------
            the result
        """
        owd = os.getcwd()
        try:
            self.update("Gathering file info from DB")
            self.archive_root, _, relpath, _, pfwid = compare.gather_data(self.dbh, self.args)
            curs = self.dbh.cursor()
            curs.execute(f"select reqnum, unitname, attnum from pfw_attempt where id={pfwid}")
            (reqnum, uname, attid) = curs.fetchone()
            if not relpath:
                self.update(f'  Connot compact logs for pfw_attempt_id, no relpath found {pfwid}', True)
                return 1

            logroot = os.path.join(self.archive_root, relpath, 'log')
            files_from_db, _ = dbutils.get_files_from_db(self.dbh, relpath, self.archive, pfwid, 'log', debug=self.debug)
            self.count = len(files_from_db)
            os.chdir(logroot)
            if not self.check_permissions(files_from_db):
                return 0

            self.update("Tar'ing files")
            self.iteration = 0
            self.update()
            fnames = []
            self.tarfile = f"log.{uname}_r{reqnum}p{attid:02d}.tar.gz"
            with tarfile.open(self.tarfile, 'w:gz') as zfh:
                for fname, items in files_from_db.items():
                    fnames.append(os.path.join(self.archive_root, items['path'], fname))
                    name = os.path.join(items['path'].replace(os.getcwd() + '/', ''), fname)
                    zfh.add(name)
                    #fnames.append(fname)
                    self.iteration += 1
                    self.results['null'].append({'fid', items['id']})
                    self.update()


            self.update("Updating database...")
            try:
                if self.results['null'] :
                    upsql = "delete from file_archive_info where desfile_id=:fid"
                    curs = self.dbh.cursor()
                    curs.executemany(upsql, self.results['null'])
                    _ = self.dbh.register_file_data('logtar', [self.tarfile], self.pfwid, 0, False)
                    sql = f"select id from desfile where filename='{self.tarfile.replace('.gz', '')} and compression='.gz'"
                    curs.execute(sql)
                    fid = curs.fetchone()[0]
                    sql = f"insert into file_archive_info (filename, compression, archive_name, path, desfile_id) values ('{self.tarfile.replace('.gz', '')}', '.gz', '{self.archive}', '{os.path.join(relpath, 'log')}', {fid}"
                    curs.execute(sql)

            except:
                self.update("Error updating the database entries, rolling back any DB changes.", True)
                self.rollback()
                raise

            # remove old files
            cannot_del = []
            self.status = 1
            return 0
            """ self.dbh.commit()
            self.update("Removing original files")
            self.iteration = 0
            self.update()
            for i, r in enumerate(fnames):
                try:
                    os.remove(r)
                    self.iteration = i + 1
                    self.update()
                except:
                    cannot_del.append(r)
                    removeEmptyFolders(os.path.join(self.archive_root,relpath,'log'))
            self.status = 0

            if cannot_del:
                with open(f"{self.pfwid}.undel", 'w', encoding="utf-8") as fh:
                    for f in cannot_del:
                        fh.write(f"    {f}\n")
                self.update(f"Cannot delete some files. See {self.pfwid}.undel for a list.", True)
            """
        finally:
            os.chdir(owd)
        return 0

    def multi_compact(self):
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
            self.doCompact()
