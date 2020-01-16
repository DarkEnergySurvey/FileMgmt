#!/usr/bin/env python3

# $Id: delete_db_run.py 38167 2015-05-11 17:35:44Z mgower $
# $Rev:: 38167                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-05-11 12:35:44 #$:  # Date of last commit.


import sys
import re
import despydmdb.desdmdbi as desdmdbi
import despymisc.miscutils as miscutils


def delete_using_run_vals(dbh, tablename, reqnum, unitname, attnum, verbose=0):
    if verbose >= 1:
        print(f"{tablename:25s}")
    sql = f"delete from {tablename} where unitname='{unitname}' and reqnum='{reqnum}' and attnum='{attnum}'"
    if verbose >= 2:
        print(f"\n%{sql}\n")
    curs = dbh.cursor()
    curs.execute(sql)
    if verbose >= 1:
        print(f"{curs.rowcount:3s} deleted rows")


def delete_using_in(dbh, tablename, incol, inlist, verbose=0):
    if verbose >= 1:
        print(f"{tablename:25s}")
    sql = f"delete from {tablename} where {incol} in ('" + "','".join(inlist) + "')"
    if verbose >= 2:
        print(f"\n{sql}\n")
    curs = dbh.cursor()
    curs.execute(sql)
    if verbose >= 1:
        print(f"{curs.rowcount:3s} deleted rows")


# verbose
#   0 - no printing
#   1 - table + number of rows deleted
#   2 - sql
#   3 - more detailed debugging info
def delete_db_run(dbh, unitname, reqnum, attnum, verbose=0):

    files2del = [] # all filenames that are being deleted
    del_by_table = {'genfile':[]}  # make the genfile entry to shorten log/wcl code

    sql = f"select metadata_table, wgb.filename from wgb, ops_filetype where wgb.unitname='{unitname}' and wgb.reqnum='{reqnum}' and wgb.attnum='{attnum}' and wgb.filetype=ops_filetype.filetype"
    if verbose >= 2:
        print(sql)
    curs = dbh.cursor()
    curs.execute(sql)
    for line in curs:
        if line[0].lower() not in del_by_table:
            del_by_table[line[0].lower()] = []
        del_by_table[line[0].lower()].append(line[1])
        files2del.append(line[1])


    sql = f"select log from pfw_wrapper where unitname='{unitname}' and reqnum='{reqnum}' and attnum='{attnum}'"
    if verbose >= 2:
        print(sql)
    curs = dbh.cursor()
    curs.execute(sql)
    for line in curs:
    #    print line
        if line[0] is not None:
            del_by_table['genfile'].append(line[0])
            files2del.append(line[0])

    sql = f"select junktar from pfw_job where unitname='{unitname}' and reqnum='{reqnum}' and attnum='{attnum}'"
    if verbose >= 2:
        print(sql)
    curs = dbh.cursor()
    curs.execute(sql)
    for line in curs:
    #    print line
        if line[0] is not None:
            del_by_table['genfile'].append(line[0])
            files2del.append(line[0])

    if verbose > 3:
        miscutils.pretty_print_dict(del_by_table, None, True, 4)
        print("\n\n\n")

    sql = f"select id from pfw_exec where unitname='{unitname}' and reqnum='{reqnum}' and attnum='{attnum}'"
    curs.execute(sql)
    execids = [str(line[0]) for line in curs]  # save as str so can easily do join
    if verbose >= 3:
        print(execids)

    sql = f"select id from opm_artifact where name in ('" + "','".join(files2del) + "')"
    if verbose >= 2:
        print(sql)
    curs.execute(sql)
    artids = [str(line[0]) for line in curs]  # save as str so can easily do join
    if verbose >= 3:
        print(artids)

    sql = "select id from pfw_wrapper where unitname='%s' and reqnum='%s' and attnum='%s'" % (unitname, reqnum, attnum)
    if verbose >= 2:
        print(sql)
    curs.execute(sql)
    wrapids = [str(line[0]) for line in curs]  # save as str so can easily do join
    if verbose >= 3:
        print(wrapids)



    # deletions
    delete_using_in(dbh, 'opm_was_generated_by', 'opm_process_id', execids, verbose)
    delete_using_in(dbh, 'opm_used', 'opm_process_id', execids, verbose)
    delete_using_in(dbh, 'opm_was_derived_from', 'child_opm_artifact_id', artids, verbose)
    delete_using_in(dbh, 'qc_processed_value', 'pfw_wrapper_id', wrapids, verbose)
    delete_using_in(dbh, 'qc_processed_message', 'pfw_wrapper_id', wrapids, verbose)

    delete_using_run_vals(dbh, 'pfw_job_exec_task', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_exec', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_job_wrapper_task', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_wrapper', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_job_task', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_job', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_block_task', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_block', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_attempt_label', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_attempt_task', reqnum, unitname, attnum, verbose)
    delete_using_run_vals(dbh, 'pfw_attempt', reqnum, unitname, attnum, verbose)

    ## Do we want it to remove pfw_unit and pfw_request if that was the only run for those?

    delete_using_in(dbh, 'scamp_qa', 'filename', files2del, verbose)
    delete_using_in(dbh, 'psf_qa', 'filename', files2del, verbose)
    delete_using_in(dbh, 'se_object', 'filename', files2del, verbose)
    delete_using_in(dbh, 'file_archive_info', 'filename', files2del+del_by_table['genfile'], verbose)

    for table, filelist in del_by_table.items():
        if filelist is not None and filelist:
            delete_using_in(dbh, table, 'filename', filelist, verbose)

    #delete_using_in(dbh, 'opm_artifact', 'id', artids, verbose)
    delete_using_in(dbh, 'opm_process', 'id', execids, verbose)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: delete_db_run.py <run>")
        sys.exit(1)

    run = sys.argv[1]

    m = re.search(r"([^_]+)_r([^p]+)p([^_]+)", run)
    if m is None:
        print("Error:  cannot parse run", run)
        sys.exit(1)

    unitname = m.group(1)
    reqnum = m.group(2)
    attnum = m.group(3)

    print("unitname =", unitname)
    print("reqnum =", reqnum)
    print("attnum =", attnum)
    print("\n")

    dbh = desdmdbi.DesDmDbi()

    try:
        delete_db_run(dbh, unitname, reqnum, attnum, 1)
    except:
        print("Caught exception.  Explicitly rolling back database.  No rows are deleted")
        dbh.rollback()
        raise

    print("Are you sure you want to delete all these rows (Y/N)? ")
    ans = sys.stdin.read(1)
    if ans.lower() == 'y':
        print("Committing the deletions")
        dbh.commit()
    else:
        print("Rolling back database.  No rows are deleted")
        dbh.rollback()
