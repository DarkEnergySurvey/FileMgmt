#!/usr/bin/env python3
""" Module to migrate files from one file system to another.

"""
import os
import tarfile

from filemgmt import fmutils
import filemgmt.disk_utils_local as dul

class CompactLogs(fmutils.FileManager):
    """ Class for migrating data

    """
    def __init__(self, win, args, pfwids, event, que=None):
        fmutils.FileManager.__init__(self, win, args, pfwids, event, que)
        self.tarfile = args.tarfile
        self.results = []
        self.live = args.live

    def _reset(self):
        self.results = []
        self.tarfile = None

    def rollback(self, x=None):
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

    def do_task(self):
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
            self.gather_data()
            curs = self.dbh.cursor()
            curs.execute(f"select reqnum, unitname, attnum from pfw_attempt where id={self.pfwid}")
            (self.reqnum, self.unitname, self.attnum) = curs.fetchone()
            if not self.relpath:
                self.update(f'  Connot compact logs for pfw_attempt_id, no relpath found {self.pfwid}', True)
                return 1

            logroot = os.path.join(self.archive_root, self.relpath, 'log')
            if self.live:
                logroot = os.path.join(os.getcwd(), 'log')
            self.get_files_from_db('log')
            self.count = len(self.files_from_db)
            if self.count == 0:
                self.update("No log files found.")
                return 0
            os.chdir(logroot)
            if not self.live:
                if not self.check_permissions(self.files_from_db):
                    return 0

            self.update("Tar'ing files")
            self.iteration = 0
            self.update()
            fnames = []
            if self.tarfile is None:
                self.tarfile = f"log.{self.unitname}_r{self.reqnum}p{self.attnum:02d}.tar.gz"
            try:
                with tarfile.open(self.tarfile, 'w:gz') as zfh:
                    for fname, items in self.files_from_db.items():
                        fnames.append(os.path.join(self.archive_root, items['path'], fname))
                        #if self.live:
                        loc = fnames[-1].find('/log/')
                        name = fnames[-1][loc + 5:]
                        #else:
                        #    name = fnames[-1].replace(os.getcwd() + '/', '')
                        zfh.add(name)
                        self.iteration += 1
                        self.results.append({'fid': items['id']})
                        self.update()
            except:
                self.update("Error tarring the files")
                self.rollback()
                raise
            if not self.live:
                self.update("Updating database...")
                try:
                    if self.results:
                        upsql = "delete from file_archive_info where desfile_id=:fid"
                        curs = self.dbh.cursor()
                        curs.executemany(upsql, self.results)
                        #############_ = self.dbh.register_file_data('logtar', [self.tarfile], self.pfwid, 0, False)
                        finfo = {'md5sum': dul.get_md5sum_file(self.tarfile),
                                 'fsize': os.path.getsize(self.tarfile),
                                 'pfwid': self.pfwid,
                                 'fname': self.tarfile.replace('.gz', ''),
                                 'comp': '.gz',
                                 'wgb': 0,
                                 'ftype': 'logtar'
                                 }
                        sql = "insert into desfile (filename, compression, filetype, pfw_attempt_id, wgb_task_id, filesize, md5sum) values (:fname, :comp, :ftype, :pfwid, :wgb, :fsize, :md5sum)"
                        curs.execute(sql, finfo)
                        sql = f"select id from desfile where filename='{self.tarfile.replace('.gz', '')}' and compression='.gz'"
                        curs.execute(sql)
                        fid = curs.fetchone()[0]
                        sql = f"insert into file_archive_info (filename, compression, archive_name, path, desfile_id) values ('{self.tarfile.replace('.gz', '')}', '.gz', '{self.archive}', '{os.path.join(self.relpath, 'log')}', {fid})"
                        curs.execute(sql)

                except:
                    self.update("Error updating the database entries, rolling back any DB changes.", True)
                    self.rollback()
                    raise

                # remove old files
                cannot_del = []
                self.status = 1

                self.dbh.commit()
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
                fmutils.removeEmptyFolders(os.path.join(self.archive_root, self.relpath,'log'))
                self.status = 0

                if cannot_del:
                    with open(os.path.join(self.cwd, f"{self.pfwid}.undel"), 'w', encoding="utf-8") as fh:
                        for f in cannot_del:
                            fh.write(f"    {f}\n")
                    self.update(f"Cannot delete some files. See {self.pfwid}.undel for a list.", True)

        finally:
            os.chdir(owd)
        return 0
