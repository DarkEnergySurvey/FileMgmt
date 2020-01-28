#!/usr/bin/env python
# $Id: _clobber_fwrefact_tables.py 38167 2015-05-11 17:35:44Z mgower $
# $Rev:: 38167                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-05-11 12:35:44 #$:  # Date of last commit.

import sys
import os
import argparse
import despydmdb.desdmdbi as desdmdbi



# assumes partitions to drop start with F  (firstcut, finalcut)
def empty_se_objects_table(dbh):
    sql = "select partition_name from all_tab_partitions where table_name='SE_OBJECT' and table_owner=(select user from dual) and partition_name!='PINIT'"
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    linecnt = 0
    for line in curs:
        sql = "alter table SE_OBJECT drop partition (%s)" % line
        print "\t",sql
        curs2 = dbh.cursor()
        curs2.execute(sql)
        linecnt += 1
    print "Dropping %s partitions from SE_OBJECT" % linecnt

    print "You'll need to manually delete any temp tables if they exist"



def delete_from_table(dbh, tname, user=None):
    sql = "delete from %s" % (tname)
    if user is not None:
        sql += " where user_created_by='%s'" % user;
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\tdeleted %s rows" % curs.rowcount


def delete_from_table_by_ftype(dbh, tname, ftype):
    sql = "delete from %s where filetype='%s' and filename like '%%_r%%p%%_%%'" % (tname, ftype)
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\tdeleted %s rows" % curs.rowcount


def delete_from_genfile_table(dbh, filetype, user=None):
    sql = "delete from genfile where filetype='%s'" % (filetype)
    if user is not None:
        sql += " and user_created_by='%s'" % user;
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\tdeleted %s rows" % curs.rowcount


def delete_from_cache_table(dbh):
    sql = "delete from ops_cache_location where filename not in (select filename from genfile)"
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\tdeleted %s rows" % curs.rowcount

    sql = "select count(*) from ops_cache_location"
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\t\t%s rows remaining" % curs.fetchall()[0][0]


def delete_from_file_archive_table(dbh):
    sql = "delete from file_archive_info where filename not in (select filename from genfile)"
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\tdeleted %s rows" % curs.rowcount

    sql = "select count(*) from file_archive_info"
    print sql
    curs = dbh.cursor()
    curs.execute(sql)
    print "\t\t%s rows remaining" % curs.fetchall()[0][0]



def main(args):
    parser = argparse.ArgumentParser(description='delete mass DB records for testing purposes')
    parser.add_argument('--inputs', action='store_true', default=False)
    #parser.add_argument('--filetypes', action='store')

    args = vars(parser.parse_args())

    dbh = desdmdbi.DesDmDbi()

    if dbh.configdict['name'].lower() != 'destest':
        print "This command can only be run against the destest database."
        print "   User = '%s'" % dbh.configdict['user']
        print "   Database Name = '%s'" % dbh.configdict['name']
        print "   Section = %s" % dbh.which_services_section()
        print "   File = %s" % dbh.which_services_file()
        exit(1)

    # delete provenance
    # OPM_ARTIFACT auto deleted when deleting from genfile
    if 'USER' in os.environ:
        for tname in ['OPM_WAS_GENERATED_BY','OPM_WAS_DERIVED_FROM','OPM_USED']:
            delete_from_table(dbh, tname, os.environ['USER'])
    else:
        print "Skipping OPM tables because couldn't determine user"

    # non-file tables
    for tname in [ 'qc_processed_value', 'qc_processed_message', 'pfw_message',
        'pfw_data_query', 'pfw_attempt_label', 'pfw_attempt_val',
        'pfw_exec', 'pfw_wrapper', 'pfw_job', 'pfw_block',
        'pfw_attempt', 'pfw_unit', 'pfw_request',
        'seminfo', 'transfer_file', 'transfer_batch',
        'task']:
        delete_from_table(dbh, tname)

    empty_se_objects_table(dbh)

    for tname in ['catalog', 'image', 'scamp_qa', 'psf_qa']:
        delete_from_table(dbh, tname)


    for ftype in ['cal_biascor', 'cal_dflatcor', 'xtalked_bias', 'xtalked_dflat']:
        delete_from_table_by_ftype(dbh, 'CALIBRATION', ftype)

    # delete output files
    for ftype in ['cat_psfex','cat_satstars','cat_scamp','cat_scamp_full','cat_trailbox','head_scamp','head_scamp_full','psfex_model','qa_scamp','red_bkg','red_check','xml_psfex','xml_scamp','wcl','log','list','junk_tar']:
        delete_from_genfile_table(dbh, ftype)

    if args['inputs']:
        for tname in ['CALIBRATION', 'EXPOSURE']:
            delete_from_table(dbh, tname)
        for ftype in ['config', 'cal_lintable', 'cal_xtalk']:
            delete_from_genfile_table(dbh, ftype)


    # delete entries from file "location" table for files no longer having metadata
    #delete_from_cache_table(dbh)
    delete_from_file_archive_table(dbh)


    print "Are you sure you want to delete all these rows (Y/N)? ",
    ans = sys.stdin.read(1)
    if ans.lower() == 'y':
        print "Committing the deletions"
        dbh.commit()
    else:
        print "Rolling back database.  No rows are deleted"
        dbh.rollback()

if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout
    sys.exit(main(sys.argv))
