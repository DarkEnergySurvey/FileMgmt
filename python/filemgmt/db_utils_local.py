"""
    Module for DB utilities
"""
import sys
import os
import time

def get_pfw_attempt_ids_from_triplet(dbh, args):
    """ Method to get a list of pfw_attempt_ids based on part or all of the reqnum, unitname, attnum
        triplet

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        args : object
            Contains the arguments (reqnum, unitname,and attnum)

    """
    # set up the query
    sql = f"select id from pfw_attempt where reqnum={args.reqnum}"
    # if there is a unitname then add it
    if args.unitname:
        sql += f" and unitname='{args.unitname}'"
    # if there is an attnum then add it
    if args.attnum:
        sql += f" and attnum={args.attnum}"
    curs = dbh.cursor()
    curs.execute(sql)
    results = curs.fetchall()
    if not results:
        msg = f"No pfw_attempt_id found for reqnum {args.reqnum}"
        if args.unitname:
            msg += f", unitname {args.unitname}"
        if args.attnum:
            msg += f", attnum {args.attnum}"
        print(msg)
        print("\tAborting")
        sys.exit(1)
    # make a list of the pfw_attempt_ids
    pfwids = []
    for res in results:
        pfwids.append(res[0])
    return pfwids

def del_files_from_db(dbh, relpath, archive):
    """ Method to delete files from file_archive_info table based on relpath

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        relpath : str
            The relative path of the files to delete

        archive : str
            The name of the archive to delete from

    """
    cur = dbh.cursor()
    cur.execute(f"delete from file_archive_info where archive_name='{archive}' and path like '{relpath}%'")
    dbh.commit()

def del_part_files_from_db_by_name(dbh, relpath, archive, delfiles):
    """ Method to delete specific files from file_archive_info table by name

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        relpath : str
            The relative path of the files to deleted

        archive : str
            The name of the archive to delete from

        delfiles : list
            List of file names to delete

    """
    cur = dbh.cursor()
    cur.prepare(f"delete from file_archive_info where archive_name='{archive}' and path like '{relpath}%' and filename=:1")
    cur.executemany(None, delfiles)
    if cur.rowcount != len(delfiles):
        print(f"Inconsistency detected: {cur.rowcount:d} rows removed from db and {len(delfiles):d} files deleted, these should match.")
    cur.execute('commit')

def del_part_files_from_db(dbh, delfileid):
    """ Method to delete specific files from file_archive_info based on id

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        delfileid : list
            List of the desfile ids of the files to be removed from file_archive_info

    """
    # load the desfile_ids into a gtt table
    tid = dbh.load_id_gtt(delfileid)
    cur = dbh.cursor()
    cur.execute(f"delete from file_archive_info fai where fai.desfile_id in (select id from {tid})")
    if len(delfileid) != cur.rowcount:
        print(f"Inconsistency detected: {cur.rowcount:d} rows removed from db and {len(delfileid):d} files deleted, these should match.")
    dbh.commit()

def get_pfw_attempt_id_from_tag(dbh, tag):
    """ Method to find all pfw_attempt_ids associated with a tag

        Parameters
        ----------
        dbh: DB handle
            The database handle to use

        tag: str
            The tag to search for

        Returns
        -------
        list: containing the associated pfw_attempt_ids
    """
    curs = dbh.cursor()
    curs.execute(f'select pfw_attempt_id from proctag where tag=\'{tag}\'')
    pfw_ids = []
    results = curs.fetchall()
    # if no pfw_attempt_ids are found, exit
    if not results:
        print(f'No pfw_attetmps_ids found for tag {tag}')
        sys.exit(1)
    for res in results:
        pfw_ids.append(res[0])
    print(f"Found {len(pfw_ids):d} pfw_attempt_id's associated with tag {tag}")
    return pfw_ids

def update_attempt_state(dbh, state, pfwid):
    """ Method to update the home_archive and db_state columns in the attempt_state table

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        state : str
            The state to change the home_archive_state entry to ('PRUNED' or 'PURGED')

        pfwid : int
            The pfw_attempt_id identifying the row in the table to change

    """
    cur = dbh.cursor()
    cur.execute(f"update attempt_state set home_archive_state='{state}', db_state='PRUNED' where pfw_attempt_id={pfwid:d}")
    cur.execute('commit')

def get_file_count_by_pfwid(dbh, pfwid):
    """ Method to get the number of files in file_archive_info associated with a given pfw_attempt_id

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        pfwid : int
            The pfw_attempt_id to query about

        Returns
        -------
        Int containing the number of files

    """
    curs = dbh.cursor()
    curs.execute(f'select count(fai.filename) from file_archive_info fai, desfile df where df.pfw_attempt_id={pfwid} and fai.desfile_id=df.id')
    return curs.fetchone()[0]

def get_pfw_attempt_ids_where(dbh, whereclause, order=None):
    """ Method to query the database for pfw_attempt_ids with the given where and order by
        qualifiers.

        Parameters
        ----------
        dbh : db handle
            The handle to use for the query

        whereclause : list
            A list of the where clauses to use with the query (e.g. ["DATA_STATE='JUNK'"])

        order : str
            The column to order the results by, default is no ordering.

        Returns
        -------
        List of the pfw_attempt_ids meeting the given qualifiers.

    """
    curs = dbh.cursor()
    sql = "select id from pfw_attempt where"
    for i, val in enumerate(whereclause):
        if i > 0:
            sql += ' and'
        sql += ' ' + val
    if order:
        sql += f' order by {order}'
    curs.execute(sql)
    results = curs.fetchall()
    res = []
    for result in results:
        res.append(result[0])
    return res
