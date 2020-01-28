"""
    Module for DB utilities
"""
import sys
import os
import time

def check_db_duplicates(dbh, filelist, archive):  #including compression
    """ Method to check for duplicates in DB
    """
    table = dbh.load_filename_gtt(filelist)
    sql = f"select fai.path, art.filename, art.compression,art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai, {table} gtt where fai.desfile_id=art.id and fai.archive_name='{archive}' and gtt.filename=art.filename and coalesce(fai.compression,'x') = coalesce(gtt.compression,'x')"

    curs = dbh.cursor()
    curs.execute(sql)
    results = curs.fetchall()

    if len(results) == len(filelist):
        return {}
    duplicates = {}
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
            if fname not in duplicates:
                duplicates[fname] = []
            duplicates[fname].append(fdict)
        if "path" in fdict:
            if fdict["path"].endswith('/'):
                fdict['path'] = fdict['path'][:-1]

    return duplicates

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


def get_paths_by_id(dbh, args):
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
    sql = f"select root from ops_archive where name={dbh.get_named_bind_string('name')}"

    curs = dbh.cursor()
    curs.execute(sql, {'name': args.archive})
    rows = curs.fetchall()
    cnt = len(rows)
    if cnt != 1:
        print(f"Invalid archive name ({args.archive}).   Found {cnt} rows in ops_archive")
        print("\tAborting")
        sys.exit(1)

    archive_root = rows[0][0]

    if args.pfwid:
        sql = f"select pfw.archive_path, ats.data_state, pfw.operator, pfw.reqnum, pfw.unitname, pfw.attnum from pfw_attempt pfw, attempt_state ats where pfw.id={dbh.get_named_bind_string('pfwid')} and ats.pfw_attempt_id=pfw.id"
        curs.execute(sql, {'pfwid' : args.pfwid})
        rows = curs.fetchall()

        relpath = rows[0][0]
        state = rows[0][1]
        operator = rows[0][2]
        args.reqnum = rows[0][3]
        args.unitname = rows[0][4]
        args.attnum = rows[0][5]
        pfwid = int(args.pfwid)

    else:
    ### sanity check relpath
        sql = f"select archive_path, operator, id from pfw_attempt where reqnum={dbh.get_named_bind_string('reqnum')} and unitname={dbh.get_named_bind_string('unitname')} and attnum={dbh.get_named_bind_string('attnum')}"
        curs.execute(sql, {'reqnum' : args.reqnum,
                           'unitname' : args.unitname,
                           'attnum' : args.attnum})
        rows = curs.fetchall()

        relpath = rows[0][0]
        operator = rows[0][1]
        pfwid = rows[0][2]
        sql = f"select data_state from attempt_state where pfw_attempt_id={dbh.get_named_bind_string('pfwid')}"
        curs.execute(sql, {'pfwid' : pfwid})
        rows = curs.fetchall()

        state = rows[0][0]

    if relpath is None:
        print(f" Path is NULL in database for pfw_attempt_id {pfwid}.")
        return None, None, None, None, None, pfwid
    archive_path = os.path.join(archive_root, relpath)

    return archive_root, archive_path, relpath, state, operator, pfwid


def get_paths_by_path(dbh, args):
    """ Method to get data about files based on path
    """
    # check archive is valid archive name (and get archive root)
    sql = f"select root from ops_archive where name={dbh.get_named_bind_string('name')}"

    curs = dbh.cursor()
    curs.execute(sql, {'name': args.archive})
    rows = curs.fetchall()
    cnt = len(rows)
    if cnt != 1:
        print(f"Invalid archive name ({args.archive}).   Found {cnt} rows in ops_archive")
        print("\tAborting")
        sys.exit(1)

    archive_root = rows[0][0]
    # see if relpath is the root directory for an attempt
    sql = f"select operator, id from pfw_attempt where archive_path={dbh.get_named_bind_string('apath')}"
    curs.execute(sql, {'apath' : args.relpath,})
    rows = curs.fetchall()
    if not rows:
        print(f"\nCould not find an attempt with an archive_path={args.relpath}")
        print("Assuming that this is part of an attempt, continuing...\n")
        operator = None
        state = ""
        pfwid = None
    elif len(rows) > 1:
        print("More than one pfw_attempt_id is assocaited with this path, use tag, or specify by pfw_attempt_id rather than a path")
        print('\nAborting')
        sys.exit(1)
    else:
        operator = rows[0][0]
        pfwid = rows[0][1]

        sql = f"select data_state from attempt_state where pfw_attempt_id={dbh.get_named_bind_string('pfwid')}"
        curs.execute(sql, {'pfwid': pfwid})
        rows = curs.fetchall()
        state = rows[0][0]

    archive_path = os.path.join(archive_root, args.relpath)

    return archive_root, archive_path, state, operator, pfwid

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


def get_files_from_db(dbh, relpath, archive, pfwid, filetype=None, debug=False):
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

    if debug:
        start_time = time.time()
        print("Getting file information from db: BEG")
    sql = "select fai.path, art.filename, art.compression, art.id, art.md5sum, art.filesize from desfile art, file_archive_info fai where"
    if filetype is not None:
        sql += build_where_clause([f'art.pfw_attempt_id={str(pfwid)}',
                                   'fai.desfile_id=art.id',
                                   'art.filetype=\'' + filetype + '\'',
                                   'fai.archive_name=\'' + archive + '\''])
    elif pfwid is not None:
        sql += build_where_clause([f'art.pfw_attempt_id={str(pfwid)}',
                                   'fai.desfile_id=art.id',
                                   'fai.archive_name=\'' + archive + '\''])
    elif relpath is not None:
        sql += build_where_clause(['fai.desfile_id=art.id',
                                   'fai.archive_name=\'' + archive + '\'',
                                   'fai.path like \'' + relpath + '%\''])

    if debug:
        print(f"\nsql = {sql}\n")

    curs = dbh.cursor()
    curs.execute(sql)
    if debug:
        print("executed")
    desc = [d[0].lower() for d in curs.description]

    filelist = []

    files_from_db = {}
    for row in curs:
        fdict = dict(zip(desc, row))
        fname = fdict['filename']
        if fdict['compression'] is not None:
            fname += fdict['compression']
        filelist.append(fname)
        files_from_db[fname] = fdict
        if "path" in fdict:
            if fdict["path"][-1] == '/':
                fdict['path'] = fdict['path'][:-1]
        #    m = re.search("/p(\d\d)",fdict["path"])
        #    if m:
        #        fdict["path"] = fdict["path"][:m.end()]
    duplicates = check_db_duplicates(dbh, filelist, archive)
    if debug:
        end_time = time.time()
        print(f"Getting file information from db: END ({end_time - start_time} secs)")
    return files_from_db, duplicates

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
