#!/usr/bin/env python

# $Id: comparchive.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import time
import argparse
import traceback
import __main__ as main
from collections import OrderedDict
import despymisc.miscutils as miscutils
import despydmdb.desdmdbi as desdmdbi
from filemgmt.filecompressor import FileCompressor

VERSION="1.0.0"
DATE_FORMAT='%Y-%m-%d %H:%M:%S'
failure_threshold=0

def getFilesToCompress(args,dbh):
    cur = dbh.cursor()
    sqlstr = """select art.id artifact_id, fa.archive_name, ar.root archive_root, fa.path, fa.filename, art.filesize
        from genfile g, 
            opm_artifact art, 
            file_archive_info fa, 
            opm_was_generated_by wgb, 
            task t, 
            pfw_attempt att,
            ops_archive ar
        where not exists(
                select * from file_archive_info fa2 
                where fa2.filename=fa.filename and fa2.compression is not null)
            and g.filename=art.name
            and art.name=fa.filename
            and fa.archive_name=ar.name
            and ar.name=:arhome
            and art.compression is null 
            and fa.compression is null
            and art.id=wgb.opm_artifact_id
            and wgb.task_id=t.id
            and t.root_task_id=att.task_id
            """
    params = OrderedDict()
    addlwhere = []
    if args['reqnum']:
        addlwhere.append("and att.reqnum in(%s)" % args["reqnum"])
        if args['unitname']:
            addlwhere.append("and att.unitname like('%s')" % args["unitname"])
            if args['attnum']:
                addlwhere.append("and att.attnum in(%s)" % args["attnum"])

    if args['filetypes']:
        addlwhere.append("and g.filetype in('" + "','".join(args["filetypes"]) + "')")
    fullsql = sqlstr + ' '.join(addlwhere) + " order by fa.path,fa.filename"
    cur.execute(fullsql,{'arhome':args["Archive"]})
    columns = [i[0].lower() for i in cur.description]
    results = [dict(zip(columns, row)) for row in cur]
    cur.close()
    return results


def makeTask(compressor,maintaskid,dbh):
    taskid = dbh.create_task(name=compressor.get_exebase(),
                             info_table='compress_task',
                             parent_task_id=maintaskid,
                             root_task_id=maintaskid,
                             do_commit=False,
                             do_begin=True)
    taskdict = {'task_id':taskid,
                 'name':compressor.get_exebase(),
                 'version':compressor.get_exe_version(),
                 'cmdargs':compressor.get_commandargs()}
    dbh.basic_insert_row("compress_task",taskdict)
    return taskid


def endTasks(successtaskid, failtaskid, dbh):
    if successtaskid:
        dbh.end_task(task_id=successtaskid,status=0)
    if failtaskid:
        dbh.end_task(task_id=failtaskid,status=1)
    if successtaskid or failtaskid:
        dbh.commit()


def reportResults(successcount,failcount,inputfilemb,outputfilemb):
    cr = None
    if outputfilemb > 0:
        cr = float(inputfilemb)/outputfilemb
    else:
        cr = 0
    freedspace = (inputfilemb - outputfilemb)/float(1024)
    print printprefix() + "FINISHED: compressed %s files with %s failures, CR=%.2f:1, freed %.3f GB" % (
            str(successcount), str(failcount), cr, freedspace)

def printprefix(base=None):
    if base:
        return time.strftime(DATE_FORMAT) + " - " + base + ": "
    else:
        return time.strftime(DATE_FORMAT) + " - "

def formaterror(msg,backtrace=False):
    if backtrace:
        return printprefix("ERROR") + msg + "\n" + getbacktrace()
    else:
        return printprefix("ERROR") + msg

def formatmultiline(msg,base=None):
    return printprefix(base) + msg.strip().replace("\n\n","\n").replace("\n","\n" + printprefix(base))

def getbacktrace():
    return formatmultiline(traceback.format_exc())

def compressFiles(artifacts,maintaskid,args,dbh):
    # create a CompressionWorker of the type specified by "class"
    compressor = miscutils.dynamically_load_class(args["class"])(args["cleanup"],args["exeargs"])
    retval = 0
    successtaskid = None
    failtaskid = None
    failcount = 0
    successcount = 0
    inputfilemb = 0.0
    outputfilemb = 0.0

    for dbdictrow in artifacts:
        filecompressor = FileCompressor(infile=None,
                                compressor=compressor,
                                cleanup=args["cleanup"],
                                dbh=dbh,
                                dbargs=dbdictrow
                                )
        retcode = filecompressor.execute()
        if retcode != 0:
            failcount += 1
            print formatmultiline(filecompressor.getErrMsg(), compressor.get_exebase())
            try:
                if failtaskid==None:
                    failtaskid = makeTask(compressor,maintaskid,dbh)
                filecompressor.updatedb(task_id=failtaskid,do_commit=True)
                print formaterror("compression failed for file %s" % filecompressor.getInfileFullpath())
            except:
                print formaterror("after failed compression, could not update database for file %s" 
                        % filecompressor.getInfileFullpath(),backtrace=True)
        else:
            try:
                if successtaskid==None:
                    successtaskid = makeTask(compressor,maintaskid,dbh)
                filecompressor.updatedb(task_id=successtaskid,do_commit=True)
                successcount += 1
                inputfilemb += filecompressor.getInfileSize()/float(1048576)
                outputfilemb += filecompressor.getOutfileSize()/float(1048576)
            except:
                print formaterror("after successful compression, could not update database for input file %s, output file %s"
                        % (filecompressor.getInfileFullpath(),filecompressor.getOutfileFullpath),backtrace=True)
                # clear the cached success_taskid in case is was not committed becasue db update failed
                if successcount == 0:
                    successtaskid = None 
                failcount += 1
        if failcount > int(failure_threshold):
            sys.stderr.write(formaterror("Exceeded failure threshold of %s, aborting\n" % str(failure_threshold)))
            retval = 1
            break
    endTasks(successtaskid,failtaskid,dbh)
    reportResults(successcount,failcount,inputfilemb,outputfilemb)
    return retval

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compress files and update the database')
    parser.add_argument('-A','--Archive',action='store', required=True)
    parser.add_argument('-r','--reqnum',action='store')
    parser.add_argument('-u','--unitname',action='store',
        help='only valid if a reqnum is also provided')
    parser.add_argument('-a','--attnum',action='store',
        help='only valid if a reqnum and unitname are also provided')
    parser.add_argument('-s','--section',action='store',
        help='section of desservices file to use when connecting to DB')
    parser.add_argument('-f','--filetypes',nargs='*',action='store')
    parser.add_argument('-e','--exeargs',action='store',default="",
        help='arguments to pass through to the compression application')
    parser.add_argument('-n','--noop',action='store_true',default=False,
        help='only list files that would be compressed; do not actually compress')
    parser.add_argument('--cleanup',action='store_true',default=False,
        help='after compression, delete the uncompressed version')
    parser.add_argument('--class',action='store',default='filemgmt.fpackcompworker.FpackCompWorker',
        help='the package.class of the compression application wrapper. Default is "filemgmt.fpackcompworker.FpackCompWorker"')
    parser.add_argument('-F','--Failcount',action='store',default=0,
        help='the number of failed compression attempts to tolerate before aborting the batch job. Default is 0.')

    args, unknown_args = parser.parse_known_args()
    args = vars(args)
    failure_threshold = args["Failcount"]

    dbh = None
    artifacts = None
    try:
        dbh = desdmdbi.DesDmDbi(section=args['section'])
    except:
        print formaterror("error connecting to database",backtrace=True)
        exit(1)

    try:
        artifacts = getFilesToCompress(args,dbh)
    except:
        print formaterror("did not get file list from database",backtrace=True)
        exit(1)

    filesizesummb = 0.0
    if args["noop"]:
        print printprefix() + "Files that would be selected for compression with the parameters received:"
    for rowdict in artifacts:
        filesizesummb += float(rowdict['filesize'])/1048576
        if args["noop"]:
            print '    ' + '/'.join([rowdict['archive_root'],rowdict['path'],rowdict['filename']])
    
    print printprefix() + "%s files totalling %.3f GB selected for compression" % (str(len(artifacts)),filesizesummb/float(1024))
    
    if args["cleanup"]:
        print printprefix() + "Space savings at estimated 7.5:1 compression ratio: %.3f GB" % ((13.0/15.0)*(filesizesummb/float(1024)))
    else:
        print printprefix() + "Additional space required at estimated 7.5:1 compression ratio: %.3f GB" % (filesizesummb/(1024*7.5))

    retval = 1
    if not args["noop"]:
        try:
            maintaskid = dbh.create_task(
                    name='comarchive.py', 
                    info_table='compress_task',
                    i_am_root = True,
                    do_begin = True, 
                    do_commit = True)
            taskdict = {'task_id':maintaskid,
                 'name':os.path.basename(main.__file__),
                 'version':VERSION,
                 'cmdargs':' '.join(sys.argv[1:])}
            dbh.basic_insert_row("compress_task",taskdict)
        except:
            print formaterror("could not create main database provenance task",backtrace=True)

        retval = compressFiles(artifacts,maintaskid,args,dbh)
        
        try:
            dbh.end_task(task_id=maintaskid,status=retval,do_commit=True)
        except:
            print formaterror("could not create main database provenance task",backtrace=True)
    else:
        retval = 0

    dbh.close()
    exit(retval)

