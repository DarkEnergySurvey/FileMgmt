#!/usr/bin/env python
# $Id: register_files.py 42334 2016-06-07 22:22:16Z mgower $
# $Rev:: 42334                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-06-07 17:22:16 #$:  # Date of last commit.

""" Program to ingest data files that were created external to framework """

import argparse
import os
import re
import sys
import time

import despymisc.miscutils as miscutils
import filemgmt.filemgmt_defs as fmdefs
import filemgmt.errors as fmerrors

__version__ = '$Rev: 42334 $'


###########################################################################
def create_list_of_files(filemgmt, args):
    """ Create list of files to register """

    filelist = None
    starttime = time.time()
    if args['filetype'] is not None:
        if not filemgmt.is_valid_filetype(args['filetype']):
            miscutils.fwdie("Error:  Invalid filetype (%s)" % args['filetype'], 1)
        filelist = get_list_filenames(args['path'], args['filetype'])
    elif args['list'] is not None:
        filelist = parse_provided_list(args['list'])
    endtime = time.time()
    print "DONE (%0.2f secs)" % (endtime - starttime)
    print "\t%s files in list" % sum([len(x) for x in filelist.values()])
    if miscutils.fwdebug_check(6, "REGISTER_FILES_DEBUG"):
        miscutils.fwdebug_print("filelist=%s" % (filelist))
    return filelist


###########################################################################
def save_register_info(filemgmt, task_id, provmsg, do_commit):
    """ Save information into the FILE_REGISTRATION table """
    row = {'task_id': task_id, 'prov_msg': provmsg}
    filemgmt.basic_insert_row('FILE_REGISTRATION', row)
    if do_commit:
        filemgmt.commit()

###########################################################################
def parse_provided_list(listname):
    """ create dictionary of files from list in file """

    #cwd = os.getcwd()
    cwd = os.getenv('PWD')  # don't use getcwd as it canonicallizes path
                            # which is not what we want for links internal to archive

    uniqfiles = {}
    filelist = {}
    try:
        with open(listname, "r") as listfh:
            for line in listfh:
                (fullname, filetype) = miscutils.fwsplit(line, ',')
                if fullname[0] != '/':
                    fullname = cwd + '/' + fullname

                if not os.path.exists(fullname):
                    miscutils.fwdie("Error:   could not find file on disk:  %s" % fullname, 1)

                (_, fname) = os.path.split(fullname)
                if fname in uniqfiles:
                    miscutils.fwdie("Error:   Found duplicate filenames in list:  %s" % fname, 1)

                uniqfiles[fname] = True
                if filetype not in filelist:
                    filelist[filetype] = []
                filelist[filetype].append(fullname)
    except IOError as err:
        miscutils.fwdie("Error: Problems reading file '%s': %s" % (listname, err), 1)

    return filelist


###########################################################################
def get_list_filenames(ingestpath, filetype):
    """ create a dictionary by filetype of files in given path """

    if ingestpath[0] != '/':
        cwd = os.getenv('PWD')  # don't use getcwd as it canonicallizes path
                                # which is not what we want for links internal to archive
        ingestpath = cwd + '/' + ingestpath

    if not os.path.exists(ingestpath):
        miscutils.fwdie("Error:   could not find ingestpath:  %s" % ingestpath, 1)

    filelist = []
    for (dirpath, _, filenames) in os.walk(ingestpath):
        for fname in filenames:
            filelist.append(dirpath+'/'+fname)

    return {filetype: filelist}



###########################################################################
def list_missing_metadata(filemgmt, ftype, filelist):
    """ Return list of files from given set which are missing metadata """
    # filelist = list of file dicts

    if miscutils.fwdebug_check(6, "REGISTER_FILES_DEBUG"):
        miscutils.fwdebug_print("filelist=%s" % (filelist))

    print "\tChecking which files already have metadata registered",
    starttime = time.time()
    results = filemgmt.has_metadata_ingested(ftype, filelist)
    endtime = time.time()
    print "(%0.2f secs)" % (endtime - starttime)

    # no metadata if results[name] == False
    havelist = [fname for fname in results if results[fname]]
    misslist = [fname for fname in results if not results[fname]]

    print "\t\t%0d file(s) already have metadata ingested" % (len(havelist))
    print "\t\t%0d file(s) still to have metadata ingested" % (len(misslist))

    if miscutils.fwdebug_check(6, "REGISTER_FILES_DEBUG"):
        miscutils.fwdebug_print("misslist=%s" % (misslist))

    return misslist



###########################################################################
def list_missing_contents(filemgmt, ftype, filelist):
    """ Return list of files from given set which still need contents ingested """
    # filelist = list of file dicts

    if miscutils.fwdebug_check(6, "REGISTER_FILES_DEBUG"):
        miscutils.fwdebug_print("filelist=%s" % (filelist))

    print "\tChecking which files still need contents ingested",
    starttime = time.time()
    results = filemgmt.has_contents_ingested(ftype, filelist)
    endtime = time.time()
    print "(%0.2f secs)" % (endtime - starttime)

    # no metadata if results[name] == False
    misslist = [fname for fname in results if not results[fname]]

    print "\t\t%0d file(s) already have content ingested" % (len(filelist) - len(misslist))
    print "\t\t%0d file(s) still to have content ingested" % len(misslist)

    if miscutils.fwdebug_check(6, "REGISTER_FILES_DEBUG"):
        miscutils.fwdebug_print("misslist=%s" % (misslist))

    return misslist


###########################################################################
def list_missing_archive(filemgmt, filelist, archive_name):
    """ Return list of files from given list which are not listed in archive """

    print "\tChecking which files are already registered in archive",
    starttime = time.time()
    existing = filemgmt.is_file_in_archive(filelist, archive_name)
    endtime = time.time()
    print "(%0.2f secs)" % (endtime - starttime)

    filenames = {}
    for fullname in filelist:
        fname = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_BASENAME)
        filenames[fname] = fullname

    missing_basenames = set(filenames.keys()) - set(existing)
    misslist = [filenames[f] for f in missing_basenames]

    print "\t\t%0d file(s) already in archive" % len(existing)
    print "\t\t%0d file(s) still to be registered to archive" % len(misslist)
    return misslist


###########################################################################
def save_file_info(filemgmt, task_id, ftype, filelist):
    """ Save file metadata and contents """
    # filelist = list of file dicts

    # check which files already have metadata in database
    #     don't bother with updating existing data, as files should be immutable
    misslist = list_missing_metadata(filemgmt, ftype, filelist)

    if len(misslist) != 0:
        print "\tSaving file metadata/contents on %0d files...." % len(misslist),
        starttime = time.time()
        try:
            filemgmt.register_file_data(ftype, misslist, None, task_id, False, None, None)
        except fmerrors.RequiredMetadataMissingError as err:
            miscutils.fwdie("Error: %s" % err, 1)

        endtime = time.time()
        print "DONE (%0.2f secs)" % (endtime - starttime)

    # check which files already have contents in database
    #     don't bother with updating existing data, as files should be immutable
    misslist = list_missing_contents(filemgmt, ftype, filelist)

    if len(misslist) != 0:
        print "\tSaving file contents on %0d files...." % len(misslist),
        starttime = time.time()
        filemgmt.ingest_contents(ftype, misslist)
        endtime = time.time()
        print "DONE (%0.2f secs)" % (endtime - starttime)

###########################################################################
def save_archive_location(filemgmt, filelist, archive_name):
    """ save location in archive """

    # check which files already are in archive
    missing_files = list_missing_archive(filemgmt, filelist, archive_name)

    # create input list of files that need to be registered in archive
    if len(missing_files) > 0:
        print "\tRegistering %s file(s) in archive..." % len(missing_files),
        starttime = time.time()
        problemfiles = filemgmt.register_file_in_archive(missing_files, archive_name)
        endtime = time.time()
        if problemfiles is not None and len(problemfiles) > 0:
            print "ERROR (%0.2f secs)" % (endtime - starttime)
            print "\n\n\nError: putting %0d files into archive" % len(problemfiles)
            for pfile in problemfiles:
                print pfile, problemfiles[pfile]
                sys.exit(1)
        print "DONE (%0.2f secs)" % (endtime - starttime)



###########################################################################
def process_files(filelist, filemgmt, task_id, archive_name, do_commit):
    """ Ingests file metadata for all files in filelist """
    # filelist[fullname] = {'path': path, 'filetype': filetype, 'fullname':fullname,
    #                       'filename', 'compression'}

    totfilecnt = sum([len(x) for x in filelist.values()])
    print "\nProcessing %0d files" % (totfilecnt)
    if miscutils.fwdebug_check(6, "REGISTER_FILES_DEBUG"):
        miscutils.fwdebug_print("filelist=%s" % (filelist))

    # work in sets defined by filetype
    for ftype in sorted(filelist.keys()):
        print "\n%s:" % ftype
        print "\tTotal: %s file(s) of this type" % len(filelist[ftype])

        save_file_info(filemgmt, task_id, ftype, filelist[ftype])
        save_archive_location(filemgmt, filelist[ftype], archive_name)

        if do_commit:
            filemgmt.commit()

###########################################################################
def parse_cmdline(argv):
    """ Parse the command line """

    parser = argparse.ArgumentParser(description=\
                'Ingest metadata for files generated outside DESDM framework')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', action='store',
                        help='Must be specified if not set in environment')
    parser.add_argument('--provmsg', action='store', required=True)
    parser.add_argument('--wclfile', action='store')
    parser.add_argument('--outcfg', action='store')
    parser.add_argument('--classmgmt', action='store')
    parser.add_argument('--classutils', action='store')

    parser.add_argument('--no-commit', action='store_true', default=False)
    parser.add_argument('--list', action='store', help='format:  fullname, filetype')
    parser.add_argument('--archive', action='store', dest='archive_name',
                        help='archive name, single value', required=True)
    parser.add_argument('--filetype', action='store',
                        help='single value, must also specify search path')
    parser.add_argument('--path', action='store',
                        help='single value, must also specify filetype')
    parser.add_argument('--version', action='store_true', default=False)

    args = vars(parser.parse_args(argv))   # convert to dict

    if args['filetype'] and ',' in args['filetype']:
        print "Error: filetype must be single value\n"
        parser.print_help()
        return 1

    if args['path'] and ',' in args['path']:
        print "Error: path must be single value\n"
        parser.print_help()
        return 1

    if args['filetype'] and args['path'] is None:
        print "Error: must specify path if using filetype\n"
        parser.print_help()
        return 1

    if args['filetype'] is None and args['path']:
        print "Error: must specify filetype if using path\n"
        parser.print_help()
        return 1

    if not args['filetype'] and not args['list']:
        print "Error: must specify either list or filetype+path\n"
        parser.print_help()
        return 1

    return args

###########################################################################
def get_filemgmt_class(args):
    """ Figure out which filemgmt class to use """
    filemgmt_class = None

    archive = args['archive_name']

    if args['classmgmt']:
        filemgmt_class = args['classmgmt']
    elif args['wclfile']:
        if args['wclfile'] is not None:
            from intgutils.wcl import WCL
            config = WCL()
            with open(args['wclfile'], 'r') as configfh:
                config.read(configfh)
        if archive in config['archive']:
            filemgmt_class = config['archive'][archive]['filemgmt']
        else:
            miscutils.fwdie("Invalid archive name (%s)" % archive, 1)
    else:
        import despydmdb.desdmdbi as desdmdbi
        with desdmdbi.DesDmDbi(args['des_services'], args['section']) as dbh:
            curs = dbh.cursor()
            sql = "select filemgmt from ops_archive where name='%s'" % archive
            curs.execute(sql)
            rows = curs.fetchall()
            if len(rows) > 0:
                filemgmt_class = rows[0][0]
            else:
                miscutils.fwdie("Invalid archive name (%s)" % archive, 1)

    if filemgmt_class is None or '.' not in filemgmt_class:
        print "Error: Invalid filemgmt class name (%s)" % filemgmt_class
        print "\tMake sure it contains at least 1 period."
        miscutils.fwdie("Invalid filemgmt class name", 1)

    return filemgmt_class

###########################################################################
def main(argv):
    """ Program entry point """
    starttime = time.time()

    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout
    revmatch = re.search(r'\$Rev:\s+(\d+)\s+\$', __version__)
    print '\nUsing revision %s of %s\n' % (revmatch.group(1), os.path.basename(sys.argv[0]))

    args = parse_cmdline(argv)

    if args['version']:
        return 0

    do_commit = not args['no_commit']

    # tell filemgmt class to get config from DB
    args['get_db_config'] = True


    # figure out which python class to use for filemgmt
    filemgmt_class = get_filemgmt_class(args)

    # dynamically load class for filemgmt
    filemgmt = None
    filemgmt_class = miscutils.dynamically_load_class(filemgmt_class)
    try:
        filemgmt = filemgmt_class(args)
    except Exception as err:
        print "ERROR\nError: creating filemgmt object\n%s" % err
        raise

    archive = args['archive_name']
    if not filemgmt.is_valid_archive(archive):
        miscutils.fwdie("Invalid archive name (%s)" % archive, 1)

    if args['outcfg'] is not None:
        with open(args['outcfg'], 'w') as outcfgfh:
            filemgmt.config.write_wcl(outcfgfh)

    print "Creating list of files to register...",
    filelist = create_list_of_files(filemgmt, args)

    ###
    print "Creating task and entry in file_registration...",
    starttime = time.time()
    task_id = filemgmt.create_task(name='register_files', info_table='file_registration',
                                   parent_task_id=None, root_task_id=None, i_am_root=True,
                                   label=None, do_begin=True, do_commit=do_commit)

    # save provenance message
    save_register_info(filemgmt, task_id, args['provmsg'], do_commit)
    endtime = time.time()
    print "DONE (%0.2f secs)" % (endtime - starttime)


    print """\nReminder:
\tFor purposes of file metadata, uncompressed and compressed
\tfiles are treated as same file (no checking is done).
\tBut when tracking file locations within archive,
\tthey are tracked as 2 independent files.\n"""
    try:
        process_files(filelist, filemgmt, task_id, archive, do_commit)
        filemgmt.end_task(task_id, fmdefs.FM_EXIT_SUCCESS, do_commit)
        if not do_commit:
            print "Skipping commit"
    except:
        filemgmt.end_task(task_id, fmdefs.FM_EXIT_FAILURE, do_commit)
        raise

    endtime = time.time()
    totfilecnt = sum([len(x) for x in filelist.values()])
    print "\n\nTotal time with %s files: %0.2f secs" % (totfilecnt, (endtime - starttime))

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

