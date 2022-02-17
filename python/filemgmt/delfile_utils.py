import sys
import os

from filemgmt import fmutils
import filemgmt.filemgmt_defs as fmdef
import filemgmt.disk_utils_local as diskutils
import filemgmt.db_utils_local as dbutils

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


class DelFiles(fmutils.FileManager):
    def __init__(self, args, pfwids):
        fmutils.FileManager.__init__(self, 0, args, pfwids, None, None)
        self.filetype = args.filetype
        self.dryrun = args.dryrun
        self.merged_comparison_info = {}
        self.comparison_info = None


    def do_task(self):
        self.pfwids.sort() # put them in order
        all_data = {}
        self.merged_comparison_info = {}
        # go through each pfw_attempt_id and gather the needed data
        for pid in self.pfwids:
            self.pfwid = pid
            if self.relpath:
                # make sure relpath is not an absolute path
                if self.relpath[0] == '/':
                    print(f"Error: relpath is an absolute path  ({self.relpath[0]})")
                    print("\tIt should be the portion of the path after the archive root.")
                    print("\tAborting")
                    sys.exit(1)
            self.gather_data()

            subdirs = self.relpath.strip('/').split('/')  # remove any trailing / first
            if len(subdirs) < 3:
                print("Suspect relpath is too high up in archive (deleting too much).")
                print("\tCheck relpath is accurate.   If actually want to delete all that,")
                print("\tcall program on smaller chunks")
                print("\tAborting")
                sys.exit(1)
            if not self.pfwid:
                part = True
            if not self.archive_root and not self.relpath:
                print(f"    Skipping pfw_attempt_id {self.pfwid}.")
                continue
            self.get_files_from_disk()
            self.get_files_from_db(self.filetype)
            if not self.check_permissions(self.files_from_db):
                raise Exception("Permissions error.")
            # if filetype is set then trim down the disk results
            if self.filetype is not None:
                newfiles = {}
                for filename, val in self.files_from_db.items():
                    if filename in self.files_from_disk:
                        newfiles[filename] = self.files_from_disk[filename]
                self.files_from_disk = newfiles

            self.compare_db_disk()
            self.merged_comparison_info[self.pfwid] = self.comparison_info
            # add it to the master dictionary
            all_data[self.pfwid] = fmutils.DataObject(**{'archive_root': self.archive_root,
                                                         'archive_path': self.archive_path,
                                                         'relpath': self.relpath,
                                                         'state': self.state,
                                                         'operator': self.operator,
                                                         'pfwid': self.pfwid,
                                                         'dofiles': self.filetype is not None or part,
                                                         'files_from_disk': self.files_from_disk,
                                                         'dup': self.duplicates,
                                                         'files_from_db': self.files_from_db,
                                                         'comparison_info': self.comparison_info})

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
            if data.state != 'JUNK' and self.filetype is None:
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
            del self.merged_comparison_info[pid]

        filesize, fend = get_size_unit(filesize)

        bad_filesize, bfend = get_size_unit(bad_filesize)

        # report the results of what was found
        if not self.files_from_db:
            print("\nNo files in database to delete.")
            sys.exit(0)
        if not self.files_from_disk:
            print("\nNo files on disk to delete.")
            sys.exit(0)

        if bad_pfwids:
            print("\nThe following data cannot be deleted as the associated attempts have not been marked as 'JUNK' (ATTEMPT_STATE.DATA_STATE):")
            if len(bad_pfwids) == 1:
                pid = list(all_data.keys())[0]
                self.operator = all_data[pid].operator
                self.archive_path = all_data[pid].archive_path
            else:
                self.operator = None
                self.archive_path = None
            self.report(bad_filesize, bfend, bad_pfwids)
            if len(bad_pfwids) == len(all_data):
                print(" No data to delete\n")
                sys.exit(1)
        for bpid in bad_pfwids:
            del all_data[int(bpid)]
            del self.merged_comparison_info[int(bpid)]

        if len(all_data) == 1:
            pid = list(all_data.keys())[0]
            self.operator = all_data[pid].operator
            self.archive_path = all_data[pid].archive_path
        else:
            self.operator = None
            self.archive_path = None
        if bad_pfwids:
            print('\nFiles that can be deleted')

        self.report(filesize, fend)

        if self.dryrun:
            return

        shdelchar = 'x'
        while shdelchar not in ['n', 'y']:
            print("")
            # query if we should proceed
            should_delete = input("Do you wish to continue with deletion [yes/no/diff/print]?  ")
            shdelchar = should_delete[0].lower()

            if shdelchar in ['p', 'print']:
                self.print_files()

            elif shdelchar in ['d', 'diff']:
                self.diff_files()

            elif shdelchar in ['y', 'yes']:
                # loop over each pfwid
                for data in all_data.values():
                    # if deleting specific files
                    if data.dofiles:
                        good = diskutils.del_part_files_from_disk(data.files_from_db, data.archive_root)
                        if len(good) != len(data.files_from_db):
                            print("Warning, not all files on disk could be deleted. Only removing the deleted ones from the database.")
                        dbutils.del_part_files_from_db(self.dbh, good)
                        # check to see if this is the last of the files in the attempt
                        if dbutils.get_file_count_by_pfwid(self.dbh, data.pfwid) != 0:
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
                            dbutils.del_part_files_from_db_by_name(self.dbh, data.relpath, self.archive, delfiles)
                        else: # has to be purged as only an entire attempt can be deleted this way
                            depth = 'PURGED'
                            dbutils.del_files_from_db(self.dbh, data.relpath, self.archive)
                    dbutils.update_attempt_state(self.dbh, depth, data.pfwid)
            elif shdelchar in ['n', 'no']:
                print("Exiting.")
            else:
                print(f"Unknown input ({shdelchar}).   Ignoring")

    def diff_files(self):
        """ Method to print(out the differences between files found on disk and found in the DB

            Parameters
            ----------
            comparison_info : dict
                Comparison info for all files keyed by pfw_attempt_id

        """
        onlydb, onlydisk = self.get_counts(self.merged_comparison_info)

        # if there are no files only found on disk or only found in the DB
        if onlydb == onlydisk == 0:
            print("\n No differneces found\n")
            return
        pids = list(self.merged_comparison_info.keys())
        # report any files only found in the DB
        print("\n Files only found in database:\n")
        if onlydb == 0:
            print("None\n\n")
        else:
            self.print_info(pids, 'dbonly')
        print(" Files only found on disk:\n")
        # report any files only found on disk
        if onlydisk == 0:
            print("None\n\n")
        else:
            self.print_info(pids, 'diskonly')

    def get_counts(self, compinfo):
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
        for data in compinfo.values():
            onlydb += len(data['dbonly'])
            onlydisk += len(data['diskonly'])
        return onlydb, onlydisk

    def print_info(self, pids, key):
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
        if pids[0]:
            print("PFW_ATT_ID     File")
        for pid in pids:
            for filename in self.merged_comparison_info[pid][key]:
                if self.merged_comparison_info[pid]['pfwid']:
                    print(f"  {self.merged_comparison_info[pid]['pfwid']}     {filename}")
                else:
                    print(f"    {filename}")
        print('\n')

    def report(self, filesize, fend, pids=None):
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
        if pids is None:
            pids = self.pfwids
        # if no operator is given do not report it (usually when there are multiple pfw_attempt_ids being reported)
        if self.operator:
            print(f"  Operator = {self.operator}")
        # if no archive path is given do not report it (usually when there are multiple pfw_attempt_ids being reported)
        if self.archive_path:
            print(f"  Path = {self.archive_path}")
        print(f"  Archive name = {self.archive}")
        print(f"  Number of files from disk = {self.files_from_disk}")
        print(f"  Number of files from db   = {self.files_from_db}")
        print(f"  Total file size on disk = {filesize:.3f} {fend}")
        # if there is no given pfw_attempt_id do not report it (usually if there is only 1 pfw_attempt_id)
        if pids:
            print(f"  pfw_attempt_ids: {', '.join(pids)}")
        print('\n')

    def print_files(self):
        """ Method to print(all found files, separated by where they were found (both disk and DB,
            disk only, DB only)

            Parameters
            ----------
            comparison_info : dict
                File comparison info on all files keyed by pfw_attempt_id
        """

        onlydb, onlydisk = self.get_counts(self.merged_comparison_info)
        print("\nFiles in both database and on disk:\n")
        pfwids = list(self.merged_comparison_info.keys())
        # print(out all files found both on disk and in the DB
        self.print_info(pfwids, 'both')

        # report any files only found in the DB
        print(" Files only found in database:\n")
        if onlydb == 0:
            print("   None\n\n")
        else:
            self.print_info(pfwids, 'dbonly')

        # report any files only found on disk
        print(" Files only found on disk:\n")
        if onlydisk == 0:
            print("   None\n\n")
        else:
            self.print_info(pfwids, 'diskonly')
