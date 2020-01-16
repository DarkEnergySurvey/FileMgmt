# $Id: filemgmt_db.py 48550 2019-05-20 19:28:17Z friedel $
# $Rev:: 48550                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:28:17 #$:  # Date of last commit.

# pylint: disable=print-statement

"""
    Extend core DM db class with functionality for managing files
    (metadata ingestion, "location" registering)
"""

__version__ = "$Rev: 48550 $"

import os
import re
import sys
import traceback

import collections

from intgutils.wcl import WCL
import despydmdb.desdmdbi as desdmdbi
import despymisc.miscutils as miscutils
import despymisc.provdefs as provdefs
import filemgmt.disk_utils_local as diskutils
import filemgmt.filemgmt_defs as fmdefs

class FileMgmtDB(desdmdbi.DesDmDbi):
    """
        Extend core DM db class with functionality for managing files
        (metadata ingestion, "location" registering)
    """

    ###########################################################################
    @staticmethod
    def requested_config_vals():
        """ return dictionary describing what values this class uses along with
            whether they are optional or required """
        return {'use_db': 'opt', 'archive': 'req', fmdefs.FILE_HEADER_INFO: 'opt',
                'filetype_metadata': 'req', 'des_services': 'opt', 'des_db_section': 'req',
                'connection': 'opt', 'threaded': 'opt'}

    ###########################################################################
    def __init__(self, initvals=None, fullconfig=None):

        if not miscutils.use_db(initvals):
            miscutils.fwdie("Error:  FileMgmtDB class requires DB but was told not to use DB", 1)

        self.desservices = None
        if 'des_services' in initvals:
            self.desservices = initvals['des_services']

        self.section = None
        if 'des_db_section' in initvals:
            self.section = initvals['des_db_section']
        elif 'section' in initvals:
            self.section = initvals['section']

        if 'threaded' in initvals:
            self.threaded = initvals['threaded']

        have_connect = False
        if 'connection' in initvals:
            try:
                desdmdbi.DesDmDbi.__init__(self, connection=initvals['connection'])
                have_connect = True
            except:
                miscutils.fwdebug_print('Could not connect to DB using transferred connection, falling back to new connection.')
        if not have_connect:
            try:
                desdmdbi.DesDmDbi.__init__(self, self.desservices, self.section)
            except Exception as err:
                miscutils.fwdie((f"Error: problem connecting to database: {err}\n" +
                                 "\tCheck desservices file and environment variables"), 1)

        # precedence - db, file, params
        self.config = WCL()

        if miscutils.checkTrue('get_db_config', initvals, False):
            self._get_config_from_db()

        if 'wclfile' in initvals and initvals['wclfile'] is not None:
            fileconfig = WCL()
            with open(initvals['wclfile'], 'r') as infh:
                fileconfig.read(infh)
                self.config.update(fileconfig)

        if fullconfig is not None:
            self.config.update(fullconfig)
        self.config.update(initvals)

        self.filetype = None
        self.ftmgmt = None
        self.filepat = None


    ###########################################################################
    def _get_config_from_db(self):
        """ reads some configuration values from the database """
        self.config = WCL()
        self.config['archive'] = self.get_archive_info()
        self.config['filetype_metadata'] = self.get_all_filetype_metadata()
        self.config[fmdefs.FILE_HEADER_INFO] = self.query_results_dict('select * from OPS_FILE_HEADER', 'name')


    ###########################################################################
    def register_file_in_archive(self, filelist, archive_name):
        """ Saves filesystem information about file like relative path
            in archive, compression extension, etc """
        # assumes files have already been declared to database (i.e., metadata)
        # caller of program must have already verified given filelist matches given archive
        # if giving fullnames, must include archive root
        # keys to each file dict must be lowercase column names, missing data must be None

        #if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
        #    miscutils.fwdebug_print("filelist = %s" % filelist)


        archivedict = self.config['archive'][archive_name]
        archiveroot = archivedict['root']

        origfilelist = filelist
        if isinstance(origfilelist, str):
            filelist = [origfilelist]

        if filelist:
            # get id from desfile table
            gtt_name = self.load_filename_gtt(filelist)
            idsql = f"""select d.filename, d.compression, d.id
                       from desfile d, {gtt_name} g
                       where d.filename=g.filename and
                       nullcmp(d.compression, g.compression) = 1"""
            ids = {}
            curs = self.cursor()
            curs.execute(idsql)
            for row in curs:
                ids[row[0]] = {row[1]: row[2]}
            #self.empty_gtt(gtt_name)

            # create dict of info to insert into file_archive_info
            insfilelist = []
            for onefile in filelist:
                nfiledict = {}
                nfiledict['archive_name'] = archive_name
                if isinstance(onefile, dict):
                    if 'filename' in onefile and 'path' in onefile and 'compression' in onefile:
                        nfiledict['filename'] = onefile['filename']
                        nfiledict['compression'] = onefile['compression']
                        path = onefile['path']
                    elif 'fullname' in onefile:
                        parsemask = miscutils.CU_PARSE_PATH | \
                                    miscutils.CU_PARSE_FILENAME | \
                                    miscutils.CU_PARSE_COMPRESSION
                        (path, nfiledict['filename'], nfiledict['compression']) = miscutils.parse_fullname(onefile['fullname'], parsemask)
                    else:
                        miscutils.fwdie(f"Error:   Incomplete info for a file to register.   Given {onefile}", 1)
                elif isinstance(onefile, str):  # fullname
                    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION
                    (path, nfiledict['filename'], nfiledict['compression']) = miscutils.parse_fullname(onefile, parsemask)


                # make sure compression starts with .
                if nfiledict['compression'] is not None and not re.match(r'^\.', nfiledict['compression']):
                    nfiledict['compression'] = '.' + nfiledict['compression']

                # get matching desfile id
                if nfiledict['filename'] in ids:
                    if nfiledict['compression'] in ids[nfiledict['filename']]:
                        nfiledict['desfile_id'] = int(ids[nfiledict['filename']][nfiledict['compression']])
                    else:
                        raise ValueError(f'Missing desfile id for file - no matching compression ({onefile})')
                else:
                    raise ValueError(f'Missing desfile id for file - no matching filename ({onefile})')

                if re.match(r'^/', path):   # if path is absolute
                    #if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                    #    miscutils.fwdebug_print("absolute path = %s" % path)
                    #    miscutils.fwdebug_print("archiveroot = %s/" % archiveroot)

                    # get rid of the archive root from the path to store
                    if re.match(fr'^{archiveroot}/', path):
                        nfiledict['path'] = path[len(archiveroot) + 1:]
                    else:
                        canon_archroot = os.path.realpath(archiveroot)
                        canon_path = os.path.realpath(path)

                        # get rid of the archive root from the path to store
                        if re.match(fr'^{canon_archroot}/', canon_path):
                            nfiledict['path'] = canon_path[len(canon_archroot) + 1:]
                        else:
                            miscutils.fwdie((f"Error: file's absolute path ({path}) does not " +
                                             f"contain the archive root ({archiveroot}) (filedict:{nfiledict})"), 1)
                else:
                    #if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                    #    miscutils.fwdebug_print("relative path = %s" % path)
                    nfiledict['path'] = path # assume only contains the relative path within the archive

                insfilelist.append(nfiledict)

            colnames = ['desfile_id', 'filename', 'compression', 'path', 'archive_name']
            try:
                self.insert_many_indiv('FILE_ARCHIVE_INFO', colnames, insfilelist)
            except:
                print("Error from insert_many_indiv in register_file_archive")
                print("colnames =", colnames)
                print("filelist =", insfilelist)
                raise

    ###########################################################################
    def has_metadata_ingested(self, filetype, fullnames):
        """ Check whether metadata has been ingested for given file """

        self.dynam_load_ftmgmt(filetype)

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]

        results = self.ftmgmt.has_metadata_ingested(listfullnames)

        if isinstance(fullnames, str):
            results = results[fullnames]
        return results

    ###########################################################################
    def check_valid(self, filetype, fullnames):
        """ Check whether file is a valid file for the given filetype """

        self.dynam_load_ftmgmt(filetype)

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]

        results = self.ftmgmt.check_valid(listfullnames)

        if isinstance(fullnames, str):
            results = results[fullnames]

        return results

    ###########################################################################
    def has_contents_ingested(self, filetype, fullnames):
        """ Check whether metadata has been ingested for given files """

        self.dynam_load_ftmgmt(filetype)

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]

        results = self.ftmgmt.has_contents_ingested(listfullnames)

        if isinstance(fullnames, str):
            results = results[fullnames]

        return results

    ######################################################################
    def ingest_contents(self, filetype, fullnames):
        """ Call filetype specific function to ingest contents """

        listfullnames = fullnames
        if isinstance(fullnames, str):
            listfullnames = [fullnames]

        results = self.has_contents_ingested(filetype, listfullnames)
        newlist = [fname for fname in results if not results[fname]]

        self.dynam_load_ftmgmt(filetype)
        self.ftmgmt.ingest_contents(newlist)

    ###########################################################################
    def is_file_in_archive(self, filelist, archive_name):
        """ Checks whether given files are in the specified archive according to the DB """
        # TODO change to return count(*) = 0 or 1 which would preserve array
        #      another choice is to return path, but how to make it return null for path that doesn't exist

        gtt_name = self.load_filename_gtt(filelist)

        # join to GTT_FILENAME for query
        sql = (f"select filename||compression from {gtt_name} g where exists " +
               f"(select filename from file_archive_info fai where " +
               f"fai.archive_name={self.get_named_bind_string('archive_name')} and fai.filename=g.filename)")
        #if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
        #    miscutils.fwdebug_print("sql = %s" % sql)

        curs = self.cursor()
        curs.execute(sql, {'archive_name': archive_name})
        existslist = []
        for row in curs:
            existslist.append(row[0])
        return existslist


    ###########################################################################
    @staticmethod
    def _get_required_headers(filetype_dict):
        """
        For use by ingest_file_metadata. Collects the list of required header values.
        """
        REQUIRED = "r"
        all_req_headers = set()
        for hdu_dict in filetype_dict['hdus'].values():
            if REQUIRED in hdu_dict:
                for cat_dict in hdu_dict[REQUIRED].values():
                    all_req_headers = all_req_headers.union(list(cat_dict.keys()))
        return all_req_headers

    ###########################################################################
    @staticmethod
    def _get_optional_metadata(filetype_dict):
        """
        For use by ingest_file_metadata. Collects the list of optional metadata values.
        """
        OPTIONAL = "o"
        all_opt_meta = set()
        for hdu_dict in filetype_dict['hdus'].values():
            if OPTIONAL in hdu_dict:
                for cat_dict in hdu_dict[OPTIONAL].values():
                    all_opt_meta = all_opt_meta.union(list(cat_dict.keys()))
        return all_opt_meta


    ###########################################################################
    @staticmethod
    def _get_column_map(filetype_dict):
        """
        For use by ingest_file_metadata. Creates a lookup from column to header.
        """
        columnMap = collections.OrderedDict()
        for hdu_dict in filetype_dict['hdus'].values():
            for status_dict in hdu_dict.values():
                for cat_dict in status_dict.values():
                    for header, columns in cat_dict.items():
                        collist = columns.split(',')
                        for position, column in enumerate(collist):
                            if len(collist) > 1:
                                columnMap[column] = header + ":" + str(position)
                            else:
                                columnMap[column] = header
        return columnMap


    ###########################################################################
    def ingest_file_metadata(self, filemeta):
        """
            Ingests the file metadata stored in <filemeta> into the database,
            using <dbdict> to determine where each element belongs.
            This wil throw an error and abort if any of the following are missing
            for any file: the filename, filetype, or other required header value.
            It will also throw an error if the filetype given in the input data
            is not found in <dbdict>
            Any exception will abort the entire upload.
        """
        dbdict = self.config[fmdefs.FILETYPE_METADATA]
        FILETYPE = "filetype"
        FILENAME = "filename"
        metatable = "metadata_table"
        COLMAP = "column_map"
        ROWS = "rows"
        metadataTables = collections.OrderedDict()

        try:
            if not isinstance(filemeta, dict):
                raise TypeError(f"Invalid type for filemeta (should be dict): {type(filemeta)}")

            if FILENAME not in filemeta:
                raise KeyError("File metadata missing FILENAME")

            if FILETYPE not in filemeta:
                raise KeyError(f"File metadata missing FILETYPE (file: {filemeta[FILENAME]})")

            if filemeta[FILETYPE] not in dbdict:
                raise ValueError(f"Unknown FILETYPE (file: {filemeta[FILENAME]}, filetype: {filemeta[FILETYPE]})")

            # check that all required are present
            all_req_headers = self._get_required_headers(dbdict[filemeta[FILETYPE]])
            for dbkey in all_req_headers:
                if dbkey not in filemeta or filemeta[dbkey] == "":
                    raise KeyError(f"Missing required data ({dbkey}) (file: {filemeta[FILENAME]})")

            # now load structures needed for upload
            rowdata = collections.OrderedDict()
            mapped_headers = set()
            filemetatable = dbdict[filemeta[FILETYPE]][metatable]

            if filemetatable not in metadataTables:
                metadataTables[filemetatable] = collections.OrderedDict()
                metadataTables[filemetatable][COLMAP] = self._get_column_map(dbdict[filemeta[FILETYPE]])
                metadataTables[filemetatable][ROWS] = []

            colmap = metadataTables[filemetatable][COLMAP]
            for column, header in colmap.items():
                compheader = header.split(':')
                if len(compheader) > 1:
                    hdr = compheader[0]
                    pos = int(compheader[1])
                    if hdr in filemeta:
                        rowdata[column] = filemeta[hdr].split(',')[pos]
                        mapped_headers.add(hdr)
                else:
                    if header in filemeta:
                        rowdata[column] = filemeta[header]
                        mapped_headers.add(header)
                    else:
                        rowdata[column] = None

            # report elements that were in the file that do not map to a DB column
            for notmapped in set(filemeta.keys()) - mapped_headers:
                if notmapped != 'fullname':
                    print("WARN: file " + filemeta[FILENAME] + " header item " \
                        + notmapped + " does not match column for filetype " \
                        + filemeta[FILETYPE])

            # add the new data to the table set of rows
            metadataTables[filemetatable][ROWS].append(rowdata)

            for metatable, metadict in metadataTables.items():
                if metatable.lower() != 'genfile' and metatable.lower() != 'desfile':
                    #self.insert_many(metatable, metadict[COLMAP].keys(), metadict[ROWS])
                    self.insert_many_indiv(metatable, list(metadict[COLMAP].keys()), metadict[ROWS])

        except (KeyError, ValueError, TypeError):
            print("filemeta:", filemeta)
            print("metadataTables = ", metadataTables)
            raise
    # end ingest_file_metadata


    ###########################################################################
    def is_valid_filetype(self, ftype):
        """ Checks filetype definitions to determine if given filetype exists """
        return ftype.lower() in self.config[fmdefs.FILETYPE_METADATA]

    ###########################################################################
    def is_valid_archive(self, arname):
        """ Checks archive definitions to determine if given archive exists """
        return arname.lower() in self.config['archive']

    ###########################################################################
    def get_file_location(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Return relative archive paths and filename including any compression extenstion """

        fileinfo = self.get_file_archive_info(filelist, arname, compress_order)
        rel_filenames = {}
        for fname, finfo in fileinfo.items():
            rel_filenames[fname] = finfo['rel_filename']
        return rel_filenames


    ###########################################################################
    def get_file_archive_info(self, filelist, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Return information about file stored in archive (e.g., filename, size, rel_filename, ...) """

        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie(f'Error: Invalid archive name ({arname})', 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie(f"Error: Missing root in archive def ({self.config['archive'][arname]})", 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie('Error:  Invalid compress_order.  '
                            'It must be a list of compression extensions (including None)', 1)

        # query DB getting all files regardless of compression
        #     Can't just use 'in' expression because could be more than 1000 filenames in list
        #           ORA-01795: maximum number of expressions in a list is 1000

        # insert filenames into filename global temp table to use in join for query
        gtt_name = self.load_filename_gtt(filelist)

        # join to GTT_FILENAME for query
        sql = ("select d.filetype,fai.path,fai.filename,fai.compression, " +
               "d.filesize, d.md5sum from desfile d, file_archive_info fai, " +
               f"{gtt_name} g where fai.archive_name={self.get_named_bind_string('archive_name')} and fai.desfile_id=d.id and " +
               "d.filename=g.filename")
        curs = self.cursor()
        curs.execute(sql, {'archive_name': arname})
        desc = [d[0].lower() for d in curs.description]

        fullnames = {}
        for comp in compress_order:
            fullnames[comp] = {}

        for line in curs:
            ldict = dict(zip(desc, line))

            if ldict['compression'] is None:
                compext = ""
            else:
                compext = ldict['compression']
            ldict['rel_filename'] = f"{ldict['path']}/{ldict['filename']}{compext}"
            fullnames[ldict['compression']][ldict['filename']] = ldict
        curs.close()

        #self.empty_gtt(gtt_name)

        #print "uncompressed:", len(fullnames[None])
        #print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in filelist:
            #print name
            for cmpord in compress_order:    # follow compression preference
                #print "cmpord = ", cmpord
                if name in fullnames[cmpord]:
                    archiveinfo[name] = fullnames[cmpord][name]
                    break

        #print "archiveinfo = ", archiveinfo
        return archiveinfo


    ###########################################################################
    def get_file_archive_info_path(self, path, arname, compress_order=fmdefs.FM_PREFER_COMPRESSED):
        """ Return information about file stored in archive
            (e.g., filename, size, rel_filename, ...) """

        # sanity checks
        if 'archive' not in self.config:
            miscutils.fwdie('Error: Missing archive section in config', 1)

        if arname not in self.config['archive']:
            miscutils.fwdie(f'Error: Invalid archive name ({arname})', 1)

        if 'root' not in self.config['archive'][arname]:
            miscutils.fwdie(f"Error: Missing root in archive def ({self.config['archive'][arname]})", 1)

        if not isinstance(compress_order, list):
            miscutils.fwdie('Error:  Invalid compress_order.  '
                            'It must be a list of compression extensions (including None)', 1)

        likestr = self.get_regex_clause('path', f'{path}/.*')

        # query DB getting all files regardless of compression
        sql = ("select filetype,file_archive_info.* from desfile, file_archive_info " +
               f"where archive_name='{arname}' and desfile.id=file_archive_info.desfile_id " +
               f"and {likestr}")
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        fullnames = {}
        for cmpord in compress_order:
            fullnames[cmpord] = {}

        list_by_name = {}
        for line in curs:
            ldict = dict(zip(desc, line))

            #print "line = ", line
            if ldict['compression'] is None:
                compext = ""
            else:
                compext = ldict['compression']
            ldict['rel_filename'] = f"{ldict['path']}/{ldict['filename']}{compext}"
            fullnames[ldict['compression']][ldict['filename']] = ldict
            list_by_name[ldict['filename']] = True

        #print "uncompressed:", len(fullnames[None])
        #print "compressed:", len(fullnames['.fz'])

        # go through given list of filenames and find archive location and compreesion
        archiveinfo = {}
        for name in list_by_name.keys():
            #print name
            for cmpord in compress_order:    # follow compression preference
                #print "cmpord = ", cmpord
                if name in fullnames[cmpord]:
                    archiveinfo[name] = fullnames[cmpord][name]
                    break

        #print "archiveinfo = ", archiveinfo
        return archiveinfo

    ######################################################################
    def dynam_load_ftmgmt(self, filetype, filepat=None):
        """ Dynamically load a filetype mgmt class """
        #print " REG DYNLOAD"
        #if miscutils.fwdebug_check(1, 'FILEMGMT_DEBUG'):
        #    miscutils.fwdebug_print("LOADING filetype = %s" % self.filetype)

        if self.ftmgmt is None or self.filetype is None or filetype != self.filetype:
            #print "  REG DYNLOAD LOAD %s" % filetype
            classname = 'filemgmt.ftmgmt_generic.FtMgmtGeneric'
            if filetype in self.config['filetype_metadata']:
                if 'filetype_mgmt' in self.config['filetype_metadata'][filetype] and \
                      self.config['filetype_metadata'][filetype]['filetype_mgmt'] is not None:
                    classname = self.config['filetype_metadata'][filetype]['filetype_mgmt']
                else:
                    miscutils.fwdie(f'Error: Invalid filetype ({filetype})', 1)

            # dynamically load class for the filetype
            filetype_mgmt = None
            filetype_mgmt_class = miscutils.dynamically_load_class(classname)
            try:
                filetype_mgmt = filetype_mgmt_class(filetype, self, self.config, filepat)
            except Exception as err:
                print(f"ERROR\nError: creating filemgmt object\n{err}")
                raise

            self.filetype = filetype
            self.filepat = filepat
            self.ftmgmt = filetype_mgmt


    ######################################################################
    def register_file_data(self, ftype, fullnames, pfw_attempt_id, wgb_task_id,
                           do_update, update_info=None, filepat=None):
        """ Save artifact, metadata, wgb provenance, and simple contents for given files """
        self.dynam_load_ftmgmt(ftype, filepat)

        results = {}

        for fname in fullnames:
            metadata = {}
            fileinfo = {}

            try:
                metadata = self.ftmgmt.perform_metadata_tasks(fname, do_update, update_info)
                if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print("INFO: metadata to ingest" + str(metadata))
                fileinfo = diskutils.get_single_file_disk_info(fname,
                                                               save_md5sum=True,
                                                               archive_root=None)
            except IOError:
                miscutils.fwdebug_print(f"\n\nError: Problem gathering data for file {fname}")
                traceback.print_exc(1, sys.stdout)

                results[fname] = None
                continue

            try:
                fileinfo['filetype'] = ftype
                fileinfo['wgb_task_id'] = int(wgb_task_id)
                if pfw_attempt_id is None:
                    fileinfo['pfw_attempt_id'] = None
                else:
                    fileinfo['pfw_attempt_id'] = int(pfw_attempt_id)

                del fileinfo['path']

                has_metadata = self.has_metadata_ingested(ftype, fname)
                if not has_metadata:
                    self.save_file_info(fileinfo, metadata)
                has_contents = self.ftmgmt.has_contents_ingested([fname])

                if not has_contents[fname]:
                    self.ftmgmt.ingest_contents([fname])
                elif miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"INFO: {fname} already has contents ingested")
                results[fname] = {'diskinfo': fileinfo, 'metadata': metadata}
            except:
                miscutils.fwdebug_print(f"\n\nError: Problem gathering metadata for file {fname}")
                traceback.print_exc(1, sys.stdout)

                results[fname] = None
        return results

    ######################################################################
    def basic_register_file_data(self, ftype, fullnames, pfw_attempt_id, wgb_task_id,
                                 do_update, update_info=None, filepat=None):
        """ Save artifact, metadata, wgb provenance, and simple contents for given files """
        self.dynam_load_ftmgmt(ftype, filepat)

        results = {}

        for fname in fullnames:
            metadata = {}
            fileinfo = {}

            try:
                metadata = self.ftmgmt.perform_metadata_tasks(fname, do_update, update_info)
                if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print("INFO: metadata to ingest" + metadata)
                fileinfo = diskutils.get_single_file_disk_info(fname,
                                                               save_md5sum=True,
                                                               archive_root=None)
                fileinfo['filetype'] = ftype
                fileinfo['wgb_task_id'] = int(wgb_task_id)
                if pfw_attempt_id is None:
                    fileinfo['pfw_attempt_id'] = None
                else:
                    fileinfo['pfw_attempt_id'] = int(pfw_attempt_id)

                del fileinfo['path']
                results[fname] = {'diskinfo': fileinfo, 'metadata': metadata}

            except IOError:
                miscutils.fwdebug_print(f"\n\nError: Problem gathering data for file {fname}")
                traceback.print_exc(1, sys.stdout)
                results[fname] = None
                continue
        return results

    ######################################################################
    def mass_register_files(self, ftype, filedata):
        self.dynam_load_ftmgmt(ftype)
        badfiles = set()
        for fname, data in filedata.items():
            try:
                has_metadata = self.has_metadata_ingested(ftype, fname)
                if not has_metadata:
                    self.save_file_info(data['diskinfo'], data['metadata'])
                elif miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"INFO: {fname} already has metadata ingested")
            except:
                miscutils.fwdebug_print(f"\n\nError: Problem gathering metadata for file {fname}")
                traceback.print_exc(1, sys.stdout)
                badfiles.add(fname)
            try:
                has_contents = self.ftmgmt.has_contents_ingested([fname])
                if not has_contents[fname]:
                    self.ftmgmt.ingest_contents([fname])
                elif miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"INFO: {fname} already has contents ingested")
            except:
                miscutils.fwdebug_print(f"\n\nError: Problem gathering metadata for file {fname}")
                traceback.print_exc(1, sys.stdout)
                badfiles.add(fname)

        return list(badfiles)

    ######################################################################
    def save_file_info(self, fileinfo, metadata):
        """ save non-location information about file """

        if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print(f"fileinfo = {fileinfo}")
            miscutils.fwdebug_print(f"metadata = {metadata}")

        self.save_desfile(fileinfo)

        if metadata is not None and metadata:
            self.ingest_file_metadata(metadata)


    ###########################################################################
    def save_desfile(self, fileinfo):
        """ save non-location information about files """
        if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print(f"fileinfo = {fileinfo}")

        colnames = ['pfw_attempt_id', 'filetype', 'filename', 'compression',
                    'filesize', 'md5sum', 'wgb_task_id']
        try:
            self.insert_many_indiv('DESFILE', colnames, [fileinfo])
        except:
            print("Error: problems saving to table desfile")
            print("colnames =", colnames)
            print("fileinfo =", fileinfo)
            raise


    ###########################################################################
    def get_filename_id_map(self, prov):
        """ Return a mapping of filename to desfile id """

        if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print(f"prov = {prov}")

        allfiles = set()
        if provdefs.PROV_USED in prov:
            for filenames in prov[provdefs.PROV_USED].values():
                for fname in filenames.split(provdefs.PROV_DELIM):
                    allfiles.add(fname.strip())
        if provdefs.PROV_WDF in prov:
            for tuples in prov[provdefs.PROV_WDF].values():
                for filenames in tuples.values():
                    for fname in filenames.split(provdefs.PROV_DELIM):
                        allfiles.add(fname.strip())

        result = []
        if allfiles:
            # build a map between filenames (with compression extension) and desfile ID
            gtt_name = self.load_filename_gtt(allfiles)
            sqlstr = f"""SELECT f.filename || f.compression, d.ID
                FROM DESFILE d, {gtt_name} f
                WHERE d.filename=f.filename and
                      nullcmp(d.compression, f.compression) = 1"""
            cursor = self.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            cursor.close()

            return dict(result)
        return result
    # end get_filename_id_map


    ###########################################################################
    def ingest_provenance(self, prov, execids):
        """ Save provenance to OPM tables """
        excepts = []
        insert_sql = """insert into {} ({}) select {},{} {} where not exists(
                    select * from {} n where n.{}={} and n.{}={})"""

        data = []
        bind_str = self.get_positional_bind_string()
        cursor = self.cursor()
        filemap = self.get_filename_id_map(prov)
        if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
            miscutils.fwdebug_print(f"filemap = {filemap}")

        if provdefs.PROV_USED in prov:
            if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print("ingesting used provenance")

            for execname, filenames in prov[provdefs.PROV_USED].items():
                for fname in filenames.split(provdefs.PROV_DELIM):
                    #print ' ==== ',fname
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[fname.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[fname.strip()])
                    data.append(rowdata)
            if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print(f"Number of used records to ingest = {len(data)}")
            exec_sql = insert_sql.format(fmdefs.PROV_USED_TABLE,
                                         fmdefs.PROV_TASK_ID + "," + fmdefs.PROV_FILE_ID,
                                         bind_str, bind_str, self.from_dual(),
                                         fmdefs.PROV_USED_TABLE, fmdefs.PROV_TASK_ID, bind_str,
                                         fmdefs.PROV_FILE_ID, bind_str)
            cursor.executemany(exec_sql, data)
            if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print(f"Number of used rows inserted = {cursor.rowcount}")
            data = []

        if provdefs.PROV_WDF in prov:
            if miscutils.fwdebug_check(3, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print("ingesting wdf provenance")
            for tuples in prov[provdefs.PROV_WDF].values():
                if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"tuples = {tuples}")

                if provdefs.PROV_PARENTS not in tuples:
                    miscutils.fwdie(f"Error: missing {provdefs.PROV_PARENTS} in one of {provdefs.PROV_WDF}",
                                    fmdefs.FM_EXIT_FAILURE)
                elif provdefs.PROV_CHILDREN not in tuples:
                    miscutils.fwdie(f"Error: missing {provdefs.PROV_CHILDREN} in one of {provdefs.PROV_WDF}",
                                    fmdefs.FM_EXIT_FAILURE)
                else:
                    for parentfile in tuples[provdefs.PROV_PARENTS].split(provdefs.PROV_DELIM):
                        for childfile in tuples[provdefs.PROV_CHILDREN].split(provdefs.PROV_DELIM):
                            try:
                                rowdata = []
                                rowdata.append(filemap[parentfile.strip()])
                                rowdata.append(filemap[childfile.strip()])
                                rowdata.append(filemap[parentfile.strip()])
                                rowdata.append(filemap[childfile.strip()])
                                data.append(rowdata)
                            except Exception as ex:
                                miscutils.fwdebug_print(f"Error ingesting provenance for {childfile.strip()} with parent {parentfile.strip()}")
                                (extype, exvalue, trback) = sys.exc_info()
                                traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
                                excepts.append(ex)


            exec_sql = insert_sql.format(fmdefs.PROV_WDF_TABLE,
                                         fmdefs.PROV_PARENT_ID + "," + fmdefs.PROV_CHILD_ID,
                                         bind_str, bind_str, self.from_dual(),
                                         fmdefs.PROV_WDF_TABLE, fmdefs.PROV_PARENT_ID,
                                         bind_str, fmdefs.PROV_CHILD_ID,
                                         bind_str)
            if data:
                if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"Number of wdf rows to insert = {len(data)}")
                cursor.executemany(exec_sql, data)
                if miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"Number of wdf rows inserted = {cursor.rowcount}")
            elif miscutils.fwdebug_check(6, 'FILEMGMT_DEBUG'):
                miscutils.fwdebug_print(f"Warn: {provdefs.PROV_WDF} section given but had 0 valid entries")
        return excepts

    #end_ingest_provenance
