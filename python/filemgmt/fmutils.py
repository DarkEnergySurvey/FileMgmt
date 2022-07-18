# $Id: fmutils.py 46644 2018-03-12 19:54:58Z friedel $
# $Rev:: 46644                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-03-12 14:54:58 #$:  # Date of last commit.

""" Miscellaneous FileMgmt utils """

import json
import os
import sys
import time
import copy

from despydmdb import desdmdbi
from despymisc import miscutils
from despymisc import misctime
import filemgmt.db_utils_local as dbutils
import filemgmt.disk_utils_local as dkutils

COMPLETE = "Complete"

##################################################################################################
def get_config_vals(archive_info, config, keylist):
    """ Search given dicts for specific values """
    info = {}
    for k, stat in keylist.items():
        if archive_info is not None and k in archive_info:
            info[k] = archive_info[k]
        elif config is not None and k in config:
            info[k] = config[k]
        elif stat.lower() == 'req':
            miscutils.fwdebug_print('******************************')
            miscutils.fwdebug_print(f'keylist = {keylist}')
            miscutils.fwdebug_print(f'archive_info = {archive_info}')
            miscutils.fwdebug_print(f'config = {config}')
            miscutils.fwdie(f'Error: Could not find required key ({k})', 1, 2)
    return info

######################################################################
def read_json_single(json_file, allMandatoryExposureKeys):
    """ Reads json manifest file """

    if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
        miscutils.fwdebug_print(f"reading file {json_file}")

    allExposures = []

    my_header = {}
    all_exposures = dict()
    with open(json_file) as my_json:
        for line in my_json:
            all_data = json.loads(line)

            for key, value in all_data.items():
                if key == 'header':
                    #read the values for the header (date and set_type are here)
                    my_head = value

                    allExposures.append(str(my_head['set_type']))
                    allExposures.append(str(my_head['createdAt']))

                if key == 'exposures':
                    if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
                        miscutils.fwdebug_print(f"line = exposures = {value}")
                    #read all the exposures that were taken for the set_type in header
                    my_header = value

                    #Total Number of exposures in manifest file
                    tot_exposures = len(my_header)

                    if tot_exposures is None or tot_exposures == 0:
                        raise Exception("0 SN exposures parsed from json file")

                    for i in range(tot_exposures):
                        if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
                            miscutils.fwdebug_print(f"Working on exposure {i}")
                            miscutils.fwdebug_print(f"\texpid = {my_header[i]['expid']}")
                            miscutils.fwdebug_print(f"\tdate = {my_header[i]['date']}")
                            miscutils.fwdebug_print(f"\tacttime = {my_header[i]['acttime']}")
                        if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
                            miscutils.fwdebug_print(f"Entire exposure {i} = {my_header[i]}")

                        mytime = my_header[i]['acttime']
                        #if mytime > 10 and numseq['seqnum'] == 2:
                        #    first_expnum = my_header[i]['expid']

                        #Validate if acctime has a meaningful value.
                        #If acttime = 0.0, then it's a bad exposure. Skip it from the manifest.
                        if mytime == 0.0:
                            continue
                        try:
                            for mandatoryExposureKey in allMandatoryExposureKeys:
                                if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
                                    miscutils.fwdebug_print(f"mandatory key {mandatoryExposureKey}")
                                key = str(mandatoryExposureKey)

                                if my_header[i][mandatoryExposureKey]:
                                    if miscutils.fwdebug_check(3, 'FMUTILS_DEBUG'):
                                        miscutils.fwdebug_print(f"mandatory key '{mandatoryExposureKey}' found {my_header[i][mandatoryExposureKey]}")
                                    if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
                                        miscutils.fwdebug_print(f"allExposures in for: {allExposures}")

                                    try:
                                        if key == 'acttime':
                                            key = 'EXPTIME'
                                            all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                        elif key == 'filter':
                                            key = 'BAND'
                                            all_exposures[key].append(str(my_header[i][mandatoryExposureKey]))
                                        elif key == 'expid':
                                            key = 'EXPNUM'
                                            all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                        else:
                                            all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                    except KeyError:
                                        all_exposures[key] = [my_header[i][mandatoryExposureKey]]


                        except KeyError:
                            miscutils.fwdebug_print(f"Error: missing key '{mandatoryExposureKey}' in json entity: {my_header[i]} ")
                            raise

                    if not all_exposures:
                        raise ValueError("Found 0 non-pointing exposures in manifest file")

                    timestamp = all_exposures['date'][0]
                    nite = misctime.convert_utc_str_to_nite(timestamp)

                    # get field by parsing set_type
                    #print 'xxxx', my_head['set_type']
                    myfield = my_head['set_type']
                    if len(myfield) > 5:
                        newfield = myfield[:5]
                    else:
                        newfield = myfield

                    camsym = 'D'   # no way to currently tell CAMSYM/INSTRUME from manifest file

                    if not newfield.startswith('SN-'):
                        raise ValueError(f"Invalid field ({newfield}).  set_type = '{my_head['set_type']}'")

                    #if json_file contains a path or compression extension, then cut it to only the filename
                    jsonfile = miscutils.parse_fullname(json_file, miscutils.CU_PARSE_FILENAME)

                    if tot_exposures is None or tot_exposures == 0:
                        raise Exception("0 SN exposures parsed from json file")

                    for i in range(tot_exposures):
                        if my_header[i]['acttime'] == 0.0:
                            continue
                        if i == 0:
                            #all_exposures['FIELD'] = [str(my_head['set_type'])]
                            all_exposures['FIELD'] = [newfield]
                            all_exposures['CREATEDAT'] = [str(my_head['createdAt'])]
                            all_exposures['MANIFEST_FILENAME'] = [jsonfile]
                            all_exposures['NITE'] = [nite]
                            all_exposures['SEQNUM'] = [1]
                            all_exposures['CAMSYM'] = [camsym]
                        else:
                            #all_exposures['FIELD'].append(str(my_head['set_type']))
                            all_exposures['FIELD'].append(newfield)
                            all_exposures['CREATEDAT'].append(str(my_head['createdAt']))
                            all_exposures['MANIFEST_FILENAME'].append(jsonfile)
                            all_exposures['NITE'].append(nite)
                            all_exposures['SEQNUM'].append(1)
                            all_exposures['CAMSYM'].append(camsym)

    # Add the manifest filename value in the dictionary
    #all_exposures['MANIFEST_FILENAME'] = json_file
    if miscutils.fwdebug_check(6, 'FMUTILS_DEBUG'):
        miscutils.fwdebug_print("allExposures " + all_exposures)

    return all_exposures

##################################################################################################

def build_where_clause(wherevals):
    """ Method to create a where clause from a list of statements

        Parameters
        ----------
        wherevals : list
            List of statements to add to a where clause (e.g. "DATA_STATE='JUNK'")

    """
    sql = ""
    for num, val in enumerate(wherevals):
        if num > 0:
            sql += ' and'
        sql += ' ' + val
    return sql


class DataObject:
    """ Class to turn a dictionary into class elements

    """
    def __init__(self, **kw):
        for item, val in kw.items():
            setattr(self, item, val)

    def get(self, attrib):
        """ Method to get the value of the given attribute

            Parameters
            ----------
            attrib : str
                The name of the attribute to get

            Returns
            -------
            The value of the attribute

        """
        return getattr(self, attrib, None)

    def set(self, attrib, value):
        """ Method to set the value of an attribute

            Parameters
            ----------
            attrib : str
                The name of the attribute to set

            value : vaires
                The value to set the attribute to

        """
        if not hasattr(self, attrib):
            raise Exception(f"{attrib} is not a member of DataObject.")
        setattr(self, attrib, value)

class Print:
    """ Class to capture printed output and write it to a log file

        Parameters
        ----------
        logfile : str
            The log file to write to

    """
    def __init__(self, logfile):
        self.old_stdout = sys.stdout
        self.logfile = open(logfile, 'w')

    def write(self, text):
        """ Method to capture, reformat, and write out the requested text

            Parameters
            ----------
            test : str
                The text to reformat

        """
        self.logfile.write(text)

    def close(self):
        """ Method to return stdout to its original handle

        """
        return self.old_stdout

    def flush(self):
        """ Method to force the buffer to flush

        """
        self.old_stdout.flush()

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


def printProgressBar(win, iteration, count, length=100, fill='█', printEnd="\n"):
    """ Print a progress bar
    """
    if count > 0:
        percent = f"{iteration:d}/{count:d}"
        filledLength = int(length * iteration // count)
    else:
        percent = 0
        filledLength = 0
    pbar = fill * filledLength + '-' * (length - filledLength)
    win.addstr(2, 0, f"Progress: |{pbar}| {percent}{printEnd}")

def run(inputs):
    """ Method to launch a multiprocessing run
    """
    try:
        if len(inputs) == 6:
            (action, wn, args, pfwids, event, que) = inputs
            runner = action(wn, args, pfwids, event, que)
        else:
            (action, wn, args, pfwids, event, rdirs, que) = inputs
            runner = action(wn, args, pfwids, event, rdirs, que)
        return runner.run()
    finally:
        que.put_nowait(Message(wn, COMPLETE, 0))

def results_error(err):
    """ Error handling routine
    """
    print("Exception raised:")
    print(err)
    raise err

def determine_ids(args):
    """ Find the pfw_attempt_id(s) based on tag or triplet
    """
    if args.dbh is None:
        args.dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
    # do some quick validation
    if 'date_range' in args and args.date_range and args.pfwid:
        print("Date_range was specified, thus pfwid cannot be.")
    if 'relpath' in args and args.relpath and (args.reqnum or args.unitname or args.attnum or args.tag or args.pfwid):
        print("Relpath was specified, thus reqnum, unitname, attnum, tag, and pfwid cannot be specified.")
        sys.exit(1)
    if 'reqnum' in args and args.reqnum and (args.tag or args.pfwid):
        print("Reqnum was specified, thus tag and pfwid cannot be specified.")
        sys.exit(1)
    if args.tag and args.pfwid:
        print("Tag was specified, thus pfwid cannot be specified.")
        sys.exit(1)
    if (('unitname' in args and args.unitname) or ('attnum' in args and args.attnum)) and not args.reqnum:
        print("Unitname and/or attnum were specified, but reqnum was not, please supply a reqnum and run again.")
        sys.exit(1)

    # if dealing with a date range then get the relevant pfw_attempt_ids
    if 'date_range' in args and args.date_range:
        dates = args.date_range.split(',')
        whereclause = []
        if len(dates) == 1:
            whereclause.append(f"submittime>=TO_DATE('{dates[0]} 00:00:01', 'YYYY-MM-DD HH24:MI:SS') and submittime<=TO_DATE('{dates[0]} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')")
        else:
            whereclause.append(f"submittime>=TO_DATE('{dates[0]} 00:00:01', 'YYYY-MM-DD HH24:MI:SS') and submittime<=TO_DATE('{dates[1]} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')")
        if args.pipeline:
            whereclause.append(f"subpipeprod='{args.pipeline}'")
        if 'reqnum' in args and args.reqnum:
            whereclause.append(f"reqnum={args.reqnum}")
            if args.unitname:
                whereclause.append(f"unitname='{args.unitname}'")
            if args.attnum:
                whereclause.append(f"attnum={args.attnum}")
        elif args.tag:
            whereclause.append(f"id in (select pfw_attempt_id from proctag where tag='{args.tag}')")
        pfwids = dbutils.get_pfw_attempt_ids_where(args.dbh, whereclause, 'id')

        if not args.silent:
            print(f"Found {len(pfwids):d} pfw_attempt_id's for the given date range (and any qualifying tag/reqnum)")
    else:
        pfwids = []
        # if dealing with a tag then get the relevant pfw_attempt_ids
        if args.tag:
            pfwids = dbutils.get_pfw_attempt_id_from_tag(args.dbh, args.tag)
        # if dealing with a triplet
        elif 'reqnum' in args and args.reqnum:
            pfwids = dbutils.get_pfw_attempt_ids_from_triplet(args.dbh, args)
            args.reqnum = None
            args.unitname = None
            args.attnum = None
        elif args.pfwid:
            if ',' in args.pfwid:
                pfwids = args.pfwid.split(',')
            else:
                pfwids = [args.pfwid]

    return args, pfwids


def get_unique_paths(args):
    """ Determine unique paths based on a fragment of a RAW file path
        args: argparse object
    """
    if 'raw' not in args or args.raw is None:
        return args, []
    if args.dbh is None:
        args.dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
    sql = f"select distinct(path) from file_archive_info where archive_name='{args.archive}' and path like 'RAW/{args.raw}%'"
    curs = args.dbh.cursor()
    curs.execute(sql)
    results = curs.fetchall()
    dirs = []
    for r in results:
        dirs.append(r[0])
    return args, dirs


def check_arg(args, argname):
    if argname in args:
        return args.__dict__[argname]
    return None


class FileManager:
    """ Base class for multiple file management activities

    """
    def __init__(self, win, args, pfwids, event, dirs=[], que=None):
        self.pfwids = pfwids
        self.cwd = os.getcwd()
        if args.dbh is None:
            self.dbh = desdmdbi.DesDmDbi(args.des_services, args.section)
        else:
            self.dbh = args.dbh
        self.win = win
        self.event = event
        self.que = que
        self.des_services = args.des_services
        self.section = args.section
        self.archive = args.archive
        self.operator = None
        self.state = None
        self.archive_path = None
        self.relpath = check_arg(args, 'relpath')
        self.reqnum = check_arg(args, 'reqnum')
        self.unitname = check_arg(args, 'unitname')
        self.attnum = check_arg(args, 'attnum')
        self.md5sum = check_arg(args, 'md5sum')
        self.raw = check_arg(args, 'raw')
        self.user = check_arg(args, 'user')
        self.group = check_arg(args, 'group')

        self.dirs = dirs
        self.verbose = args.verbose
        self.debug = args.debug
        self.script = args.script
        self.pfwid = args.pfwid
        self.rdir = None
        self.silent = args.silent
        self.tag = args.tag
        self.archive_root = None
        self.count = 0
        self.currnewpath = None
        self.status = 0
        self.iteration = 0
        self.halt = False
        self.number = 0
        self.length = 1
        self.files_from_db = None
        self.db_duplicates = None
        self.files_from_disk = None
        self.duplicates = None
        self.comparison_info ={}

    def reset(self):
        self.dbh.close()
        self.dbh = desdmdbi.DesDmDbi(self.des_services, self.section)
        self.relpath = None
        self.reqnum = None
        self.unitname = None
        self.attnum = None
        self.files_from_db = None
        self.db_duplicates = None
        self.files_from_disk = None
        self.duplicates = None
        self.comparison_info ={}
        self._reset()

    def _reset(self):
        pass

    def run(self):
        """ Execute the main task(s)

        """
        try:
            if not self.pfwids and not self.dirs:
                return self.do_task()
            if len(self.pfwids) == 1:
                self.pfwid = self.pfwids[0]
                return self.do_task()
            if len(self.dirs) == 1:
                self.rdir = self.dirs[0]
                return self.do_task()
            self.pfwids.sort()  # put them in order
            self.dirs.sort()
            return self.multi_task()
        except Exception as ex:
            if self.pfwid is not None:
                efname = f"{self.pfwid}.err"
            else:
                efname = f"{os.path.basename(self.rdir)}.err"
            with open(efname, 'w') as fh:
                fh.write(str(ex))


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

    def gather_data(self):
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
        if ('relpath' in self.__dict__ and self.relpath is not None) or \
           ('dirs' in self.__dict__ and self.dirs):
            self.get_paths_by_path()
        elif ('reqnum' in self.__dict__ and self.reqnum) or self.pfwid:
            self.get_paths_by_id()
        else:
            raise Exception("Either relpath, pfwid, or a reqnum must be specified.")
        if not self.relpath:
            return
        # check path exists on disk
        if not os.path.exists(self.archive_path):
            print(f"Warning: Path does not exist on disk:  {self.archive_path}")
        if self.verbose:
            self.silent = False

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
        if self.silent or self.count == 0:
            return
        percent = f"{self.iteration:d}/{self.count:d}"
        filledLength = int(length * self.iteration // self.count)
        pbar = fill * filledLength + '-' * (length - filledLength)
        print(f'\rProgress: |{pbar}| {percent}', end = printEnd)

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
            if self.pfwid is not None:
                fname = f"{self.pfwid}.badperm"
            else:
                fname = f"{os.path.basename(self.rdir)}.badperm"
            with open(os.path.join(self.cwd, fname), 'w', encoding="utf-8") as fh:
                for f in bad_files:
                    fh.write(f"{f}\n")

            self.update(f"Some files do not have rw permissions. See {fname} for a list.", True)
            return False
        return True

    def multi_task(self):
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
        retval = 0
        if self.pfwids:
            self.length = len(self.pfwids)
            for i, pdwi in enumerate(self.pfwids):
                self.number = i
                if self.check_status():
                    return 0
                self.count = 0

                self.pfwid = pdwi
                retval += self.do_task()
                self.reset()
        if self.dirs:
            self.length = len(self.dirs)
            for i, rd in enumerate(self.dirs):
                self.number = i
                if self.check_status():
                    return 0
                self.count = 0

                self.rdir = rd
                retval += self.do_task()
                self.reset()

        return retval

    def get_paths_by_path(self):
        """ Method to get data about files based on path
        """
        # check archive is valid archive name (and get archive root)
        sql = f"select root from ops_archive where name={self.dbh.get_named_bind_string('name')}"

        curs = self.dbh.cursor()
        curs.execute(sql, {'name': self.archive})
        rows = curs.fetchall()
        cnt = len(rows)
        if cnt != 1:
            print(f"Invalid archive name ({self.archive}).   Found {cnt} rows in ops_archive")
            print("\tAborting")
            sys.exit(1)

        self.archive_root = rows[0][0]
        if self.rdir:
            self.archive_path = os.path.join(self.archive_root, self.rdir)
            self.relpath = self.rdir
        else:
            # see if relpath is the root directory for an attempt
            sql = f"select operator, id from pfw_attempt where archive_path={self.dbh.get_named_bind_string('apath')}"
            curs.execute(sql, {'apath': self.relpath})
            rows = curs.fetchall()
            if not rows:
                print(f"\nCould not find an attempt with an archive_path={self.relpath}")
                print("Assuming that this is part of an attempt, continuing...\n")
                self.operator = None
                self.state = ""
                self.pfwid = None
            elif len(rows) > 1:
                print("More than one pfw_attempt_id is assocaited with this path, use tag, or specify by pfw_attempt_id rather than a path")
                print('\nAborting')
                sys.exit(1)
            else:
                self.operator = rows[0][0]
                self.pfwid = rows[0][1]

                sql = f"select data_state from attempt_state where pfw_attempt_id={self.dbh.get_named_bind_string('pfwid')}"
                curs.execute(sql, {'pfwid': self.pfwid})
                rows = curs.fetchall()
                self.state = rows[0][0]

            self.archive_path = os.path.join(self.archive_root, self.relpath)

    def get_paths_by_id(self):
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

        # check archive is valid archive name (and get archive root)
        sql = f"select root from ops_archive where name={self.dbh.get_named_bind_string('name')}"

        curs = self.dbh.cursor()
        curs.execute(sql, {'name': self.archive})
        rows = curs.fetchall()
        cnt = len(rows)
        if cnt != 1:
            print(f"Invalid archive name ({self.archive}).   Found {cnt} rows in ops_archive")
            print("\tAborting")
            sys.exit(1)

        self.archive_root = rows[0][0]

        if self.pfwid:
            sql = f"select pfw.archive_path, ats.data_state, pfw.operator, pfw.reqnum, pfw.unitname, pfw.attnum from pfw_attempt pfw, attempt_state ats where pfw.id={self.dbh.get_named_bind_string('pfwid')} and ats.pfw_attempt_id=pfw.id"
            curs.execute(sql, {'pfwid' : self.pfwid})
            rows = curs.fetchall()

            self.relpath = rows[0][0]
            self.state = rows[0][1]
            self.operator = rows[0][2]
            self.reqnum = rows[0][3]
            self.unitname = rows[0][4]
            self.attnum = rows[0][5]

        else:
        ### sanity check relpath
            sql = f"select archive_path, operator, id from pfw_attempt where reqnum={self.dbh.get_named_bind_string('reqnum')} and unitname={self.dbh.get_named_bind_string('unitname')} and attnum={self.dbh.get_named_bind_string('attnum')}"
            curs.execute(sql, {'reqnum' : self.reqnum,
                               'unitname' : self.unitname,
                               'attnum' : self.attnum})
            rows = curs.fetchall()

            self.relpath = rows[0][0]
            self.operator = rows[0][1]
            self.pfwid = rows[0][2]
            sql = f"select data_state from attempt_state where pfw_attempt_id={self.dbh.get_named_bind_string('pfwid')}"
            curs.execute(sql, {'pfwid' : self.pfwid})
            rows = curs.fetchall()

            self.state = rows[0][0]

        if self.relpath is None:
            raise Exception(f" Path is NULL in database for pfw_attempt_id {self.pfwid}.")
        self.archive_path = os.path.join(self.archive_root, self.relpath)

    def get_files_from_db(self, filetype=None):
        """ Query DB to get list of files within that path inside the archive

            Parameters
            ----------
            dbh : database connection
                The database connection to use

            relpath : str
                The relative path of the directory to gather info for

            archive : str
                The archive name to use

            debug : bool
                Whether or not to report debugging information

            Returns
            -------
            Dictionary containing the file info from the archive (path, name, filesize, md5sum)
        """

        if self.debug:
            start_time = time.time()
            print("Getting file information from db: BEG")
        sql = "select fai.path, art.filename, art.compression, art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where"
        if filetype is not None:
            sql += build_where_clause([f'art.pfw_attempt_id={str(self.pfwid)}',
                                       'fai.desfile_id=art.id',
                                       'art.filetype=\'' + filetype + '\'',
                                       'fai.archive_name=\'' + self.archive + '\''])
        elif self.pfwid is not None:
            sql += build_where_clause([f'art.pfw_attempt_id={str(self.pfwid)}',
                                       'fai.desfile_id=art.id',
                                       'fai.archive_name=\'' + self.archive + '\''])
        elif self.relpath is not None:
            sql += build_where_clause(['fai.desfile_id=art.id',
                                       'fai.archive_name=\'' + self.archive + '\'',
                                       'fai.path like \'' + self.relpath + '%\''])

        if self.debug:
            print(f"\nsql = {sql}\n")

        curs = self.dbh.cursor()
        curs.execute(sql)
        if self.debug:
            print("executed")
        desc = [d[0].lower() for d in curs.description]

        filelist = []

        self.files_from_db = {}
        for row in curs:
            fdict = dict(zip(desc, row))
            fname = fdict['filename']
            if fdict['compression'] is not None:
                fname += fdict['compression']
            filelist.append(fname)
            
            self.files_from_db[fname] = fdict
            if "path" in fdict:
                if fdict["path"][-1] == '/':
                    fdict['path'] = fdict['path'][:-1]
            #    m = re.search("/p(\d\d)",fdict["path"])
            #    if m:
            #        fdict["path"] = fdict["path"][:m.end()]
        self.check_db_duplicates(filelist)
        if self.debug:
            end_time = time.time()
            print(f"Getting file information from db: END ({end_time - start_time} secs)")

    def check_db_duplicates(self, filelist):  #including compression
        """ Method to check for duplicates in DB
        """
        table = self.dbh.load_filename_gtt(filelist)
        sql = f"select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai, {table} gtt where fai.desfile_id=art.id and fai.archive_name='{self.archive}' and gtt.filename=art.filename and coalesce(fai.compression,'x') = coalesce(gtt.compression,'x')"

        curs = self.dbh.cursor()
        curs.execute(sql)
        results = curs.fetchall()
        self.db_duplicates = {}

        if len(results) == len(filelist):
            return
        templist = []
        desc = [d[0].lower() for d in curs.description]

        for row in results:
            fdict = dict(zip(desc, row))
            fname = fdict['filename']
            if fdict['compression'] is not None:
                fname += fdict['compression']

            if fname not in templist:
                templist.append(fname)
            else:
                if fname not in self.db_duplicates:
                    self.db_duplicates[fname] = []
                self.db_duplicates[fname].append(fdict)
            if "path" in fdict:
                if fdict["path"].endswith('/'):
                    fdict['path'] = fdict['path'][:-1]

    def get_files_from_disk(self):
        """ Check disk to get list of files within that path inside the archive

            Parameters
            ----------
            archive_root : str
                The base root of the relpath entry

            check_md5sum : bool
                Whether or not to compare md5sums

            debug : bool
                Whether or not to report debugging info

            Returns
            -------
            A dictionary contianing the info about the files on disk (filesize, md5sum, compression, filename, path)

        """

        start_time = time.time()
        if self.debug:
            print("Getting file information from disk: BEG")

        self.files_from_disk = {}
        self.duplicates = {}
        for (dirpath, _, filenames) in os.walk(os.path.join(self.archive_root, self.relpath)):
            for filename in filenames:
                fullname = f'{dirpath}/{filename}'
                data = dkutils.get_single_file_disk_info(fullname, self.md5sum, self.archive_root)
                if filename in self.files_from_disk:
                    if filename not in self.duplicates:
                        self.duplicates[filename] = [copy.deepcopy(self.files_from_disk[filename])]
                    self.duplicates[filename].append(data)
                    #print "DUP",filename,files_from_disk[filename]['path'],data['path']
                else:
                    self.files_from_disk[filename] = data

        end_time = time.time()
        if self.debug:
            print(f"Getting file information from disk: END ({end_time - start_time} secs)")


    def compare_db_disk(self):
        """ Compare file info from DB to info from disk

            Parameters
            ----------
            file_from_db : dict
                Dicitonary containing the file info from the database

            files_from_disk : dict
                Dictionary containing the file info from disk

            check_md5sum : bool
                Whether or not to report the md5sum comparision

            check_filesize : bool
                Whether or not to report the filesize comparison

            debug : bool
                Whenther or not to report debugging info
                Default: False

            archive_root : str
                The archive root path
                Default : False

            Returns
            -------
            None
        """

        start_time = time.time()
        if self.debug:
            print("Comparing file information: BEG")
        self.comparison_info = {'equal': [],
                           'dbonly': [],
                           'diskonly': [],
                           'both': [],
                           'path': [],
                           'filesize': [],
                           'duplicates': [], # has db entry
                           'pathdup' : [],    # has no db entry
                           'pfwid' : self.pfwid
                          }
        if self.md5sum:
            self.comparison_info['md5sum'] = []

        #if check_filesize:
        #    comparison_info['filesize'] = []

        allfiles = set(self.files_from_db).union(set(self.files_from_disk))

        for fname in allfiles:
            if fname in self.files_from_db:
                if fname in self.files_from_disk:
                    self.comparison_info['both'].append(fname)
                    fdisk = self.files_from_disk[fname]
                    fdb = self.files_from_db[fname]
                    if fname in self.duplicates:
                        self.comparison_info['duplicates'].append(fname)
                    if fdisk['relpath'] == fdb['path']:
                        if fdisk['filesize'] == fdb['filesize']:
                            if self.md5sum:
                                if fdisk['md5sum'] == fdb['md5sum']:
                                    self.comparison_info['equal'].append(fname)
                                else:
                                    self.comparison_info['md5sum'].append(fname)
                            else:
                                self.comparison_info['equal'].append(fname)
                        else:
                            self.comparison_info['filesize'].append(fname)
                    else:
                        try:
                            data = dkutils.get_single_file_disk_info(fdb['path'] + '/' + fname, self.md5sum, self.archive_root)
                            if fname not in self.duplicates:
                                self.duplicates[fname] = []
                            self.duplicates[fname].append(copy.deepcopy(self.files_from_disk[fname]))
                            self.files_from_disk[fname] = data
                            self.comparison_info['duplicates'].append(fname)
                        except:
                            self.comparison_info['path'].append(fname)
                else:
                    self.comparison_info['dbonly'].append(fname)
            else:
                if fname in self.duplicates:
                    self.comparison_info['pathdup'].append(fname)
                self.comparison_info['diskonly'].append(fname)

        end_time = time.time()
        if self.debug:
            print(f"Comparing file information: END ({end_time - start_time} secs)")

    def do_task(self):
        """ Do the main task, must be overloaded by child class
        """
        raise Exception("FimeManager.do_task cannot be directly called. It must be implemented by a child class.")

    def rollback(self, x=None):
        """ Return the data to its original state.
        """
        return
