""" Compare files from local disk and DB location tracking based upon an archive path """

from filemgmt import fmutils


class FileCompare(fmutils.FileManager):
    def __init__(self, args, pfwids):
        fmutils.FileManager.__init__(self, 0, args, pfwids, None, None)
        self.start_at = args.start_at
        self.end_at = args.end_at
        self.date_range = args.date_range
        self.pipeline = args.pipeline

    def print_all_files(self):
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

        print("db path/name (filesize, md5sum)   F   disk path/name (filesize, md5sum)")
        allfiles = set(self.files_from_db).union(set(self.files_from_disk))
        fdisk_str = ""
        # loop over all found files
        for fname in allfiles:
            # if the file name is in the DB list
            if fname in self.files_from_db:
                finfo = self.files_from_db[fname]
                fullname = f"{finfo['path']}/{fname}"
                filesize = None
                if 'filesize' in finfo:
                    filesize = finfo['filesize']
                md5sum = None
                if 'md5sum' in finfo:
                    md5sum = finfo['md5sum']

                fdb_str = f"{fullname} ({filesize}, {md5sum})"
            else:
                fdb_str = ""
            # if the file name is in the disk list
            if fname in self.files_from_disk:
                finfo = self.files_from_disk[fname]
                fullname = f"{finfo['relpath']}/{fname}"
                filesize = None
                if 'filesize' in finfo:
                    filesize = finfo['filesize']
                md5sum = None
                if 'md5sum' in finfo:
                    md5sum = finfo['md5sum']

                fdisk_str = f"{fullname} ({filesize}, {md5sum})"
            else:
                fdisk_str = ""
            # not whether they are the same or not
            comp = 'X'
            if fname in self.comparison_info['equal']:
                comp = '='

            print(f"{fdb_str:-140s} {comp} {fdisk_str:-140s}")

    def diff_files(self):
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
        if self.comparison_info['dbonly']:
            print("Files only found in the database --------- ")
            for fname in sorted(self.comparison_info['dbonly']):
                fdb = self.files_from_db[fname]
                print(f"\t{fdb['path']}/{fname}")

        # print out files that are only found on disk
        if self.comparison_info['diskonly']:
            print("\nFiles only found on disk --------- ")
            for fname in sorted(self.comparison_info['diskonly']):
                addon = ""
                if fname in self.duplicates:
                    addon = "  *"
                fdisk = self.files_from_disk[fname]
                print(f"\t{fdisk['relpath']}/{fname}{addon}")
            if self.comparison_info['pathdup']:
                print("\n The following files had multiple paths on disk (path  filesize):")
                listing = {}
                for fname in self.comparison_info['pathdup']:
                    pdup.append(fname)
                    listing[self.comparison_info['pathdup']['relpath']] = self.comparison_info['pathdup']['filesize']
                first = True
                for pth in sorted(listing):
                    start = " "
                    if first:
                        start = "*"
                        first = False
                    addon = ""
                    if fname in self.files_from_db and self.files_from_db[fname]['path'] == pth:
                        addon = "  (DB Match)"
                    print(f"      {start} {pth}/{fname}   {listing[pth]:d}{addon}")

        # Print files that have different paths on disk and in the DB
        if self.comparison_info['path']:
            print("\nPath mismatch (file name, db path, disk path) --------- ")
            for fname in sorted(self.comparison_info['path']):
                addon = ""
                if fname in self.duplicates:
                    addon = " *"
                fdb = self.files_from_db[fname]
                fdisk = self.files_from_disk[fname]
                print(f"\t{fname}\t{fdb['path']}\t{fdisk['relpath']}{addon}")
            if self.comparison_info['duplicates']:
                print("  The following files have multiple disk paths on disk (path  filesize):")
                for fname in self.comparison_info['duplicates']:
                    pdup.append(fname)
                    listing[self.comparison_info['duplicates']['relpath']] = self.comparison_info['duplicates']['filesize']
                first = True
                for pth in sorted(listing):
                    start = " "
                    if first:
                        start = "*"
                        first = False
                    addon = ""
                    if fname in self.files_from_db and self.files_from_db[fname]['path'] == pth:
                        addon = "  (DB Match)"
                    print(f"      {start} {pth}/{fname}   {listing[pth]:d}{addon}")

        # Print files that have different file sizes on disk and in the DB
        if self.comparison_info['filesize']:
            print("\nFilesize mismatch (File name, size in DB, size on disk) --------- ")
            for fname in sorted(self.comparison_info['filesize']):
                fdb = self.files_from_db[fname]
                fdisk = self.files_from_disk[fname]
                print(f"\t{fname} {fdb['filesize']} {fdisk['filesize']}")

        # Print files that have different md5sum on disk and in DB
        if self.md5sum and 'md5sum' in self.comparison_info and self.comparison_info['md5sum']:
            print("\nmd5sum mismatch (File name, sum in DB, sum on disk) --------- ")
            for fname in sorted(self.comparison_info['md5sum']):
                fdb = self.files_from_db[fname]
                fdisk = self.files_from_disk[fname]
                print(f"\t{fname} {fdb['md5sum']} {fdisk['md5sum']}")

        # Print out files that have multiple paths on disk
        if len(self.duplicates) > len(pdup):
            print("\nThe following files have multiple disk paths on disk (path  filesize):")
        for dup in sorted(self.duplicates):
            if dup not in pdup:
                listing = {}
                for fls in self.duplicates[dup]:
                    listing[fls['relpath']] = fls['filesize']
                first = True
                for pth in sorted(listing):
                    start = " "
                    if first:
                        start = "*"
                        first = False
                    addon = ""
                    if dup in self.files_from_db and self.files_from_db[dup]['path'] == pth:
                        addon = "  (DB Match)"
                    print(f"      {start} {pth}/{dup}   {listing[pth]:d}{addon}")

        # Print out files that have multiple endtries in the DB
        if self.db_duplicates:
            print("\nThe following files have multiple entries in the database (path  filesize):")
        for dup in sorted(self.db_duplicates):
            listing = {}
            for fls in self.db_duplicates[dup]:
                listing[fls['relpath']] = fls['filesize']
            first = True
            for pth in sorted(listing):
                start = " "
                if first:
                    start = "*"
                    first = False
                addon = ""
                if dup in self.files_from_disk and self.files_from_disk[dup]['path'] == pth:
                    addon = "  (Disk Match)"
                print(f"      {start} {pth}/{dup}   {listing[pth]:d}{addon}")

    def multi_task(self):
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
        length = len(self.pfwids)
        if self.start_at > length or self.start_at < 1:
            print("Error: Starting index is beyond bounds of list.")
            return 1
        offset = int(self.start_at) - 1
        if self.end_at != 0:
            if self.end_at < self.start_at:
                print("Error: Ending index is less than starting index.")
                return 1
            if self.end_at > length:
                print("Error: Ending index is beyond bounds of list.")
                return 1
            self.pfwids = self.pfwids[offset:self.end_at]
        else:
            self.pfwids = self.pfwids[offset:]
        for i, pdwi in enumerate(self.pfwids):
            print(f"--------------------- Starting {i + 1 + offset:d}/{length:d} ---------------------")
            self.pfwid = pdwi
            count += self.do_task()
        return count


    def do_task(self):
        """ Main control """
        self.gather_data()

        if not self.relpath:
            print(f'  Connot do comparison for pfw_attempt_id {self.pfwid}')
            return 1
        #print archive_root
        if self.debug:
            print("From DB")
        self.get_files_from_db()
        if self.debug:
            print("From disk")
        self.get_files_from_disk()
        if self.debug:
            print("Compare")
        self.compare_db_disk()
        # print the full results unless requested not to
        if not self.script and not self.silent:
            print(f"\nPath = {self.archive_path}")
            print(f"Archive name = {self.archive}")
            addon = ""
            dbaddon = ""
            if self.duplicates:
                addon += f"({len(self.files_from_disk):d} are distinct)"
            if self.db_duplicates:
                dbaddon += f"({len(self.files_from_db):d} are distinct)"
            print(f"Number of files from db   = {len(self.files_from_db) + len(self.db_duplicates):d}   {dbaddon}")
            print(f"Number of files from disk = {len(self.files_from_disk) + len(self.duplicates):d}   {addon}")
            if self.duplicates:
                print(f"Files with multiple paths on disk  = {len(self.duplicates):d}")
            # print summary of comparison
            print("Comparison Summary")

            print(f"\tEqual:\t{len(self.comparison_info['equal']):d}")
            print(f"\tDB only:\t{len(self.comparison_info['dbonly']):d}")
            print(f"\tDisk only:\t{len(self.comparison_info['diskonly']):d}")
            print(f"\tMismatched paths:\t{len(self.comparison_info['path']):d}")
            print(f"\tMismatched filesize:\t{len(self.comparison_info['filesize']):d}")
            if 'md5sum' in self.comparison_info:
                print(f"\tMismatched md5sum:\t{len(self.comparison_info['md5sum']):d}")
            print("")

            if self.debug:
                self.print_all_files()
            elif self.verbose:
                self.diff_files()
            if len(self.comparison_info['dbonly']) == len(self.comparison_info['diskonly']) == len(self.comparison_info['path']) == len(self.comparison_info['filesize']) == 0:
                if 'md5sum' in self.comparison_info:
                    if self.comparison_info['md5sum']:
                        print("md5sum  ERROR")
                        return 1
                return 0
            return 1


        if self.pfwid is not None:
            loc = f"{self.pfwid}"
        elif self.relpath is None:
            loc = f"{self.reqnum}  {self.unitname}  {self.attnum}"
        else:
            loc = self.relpath
        if len(self.comparison_info['dbonly']) == len(self.comparison_info['diskonly']) == len(self.comparison_info['path']) == len(self.comparison_info['filesize']) == 0:
            if 'md5sum' in self.comparison_info:
                if self.comparison_info['md5sum']:
                    if not self.silent:
                        print(f"{loc}  ERROR")
                    return 1
            if not self.silent:
                print(f"{loc}  OK")
            return 0
        if not self.silent:
            print(f"{loc}  ERROR")
        return 1

# pylint: disable=unused-argument

def compare(dbh=None, des_services=None, section=None, archive='desar2home', reqnum=None, unitname=None,
            attnum=None, relpath=None, pfwid=None, date_range=None, pipeline=None,
            md5sum=False, debug=False, script=False, verbose=False, silent=True,
            tag=None, start_at=1, end_at=0, log=None):
    """ Entry point
    """
    (args, pfwids) = fmutils.determine_ids(fmutils.DataObject(**locals()))
    cm = FileCompare(args, pfwids)
    return cm.run()
