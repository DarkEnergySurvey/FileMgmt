# $Id: ftmgmt_generic.py 46423 2017-12-19 21:07:55Z friedel $
# $Rev:: 46423                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-12-19 15:07:55 #$:  # Date of last commit.

"""
    Generic filetype management class used to do filetype specific tasks
    such as metadata and content ingestion
"""

__version__ = "$Rev: 46423 $"

import collections
import copy
import re
#import time

import despymisc.miscutils as miscutils
import despydmdb.dmdb_defs as dmdbdefs


class FtMgmtGeneric:
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        self.filetype = filetype
        self.dbh = dbh
        self.config = config
        self.filepat = filepat



    ######################################################################
    def has_metadata_ingested(self, listfullnames):
        """ Check if file has row in metadata table """

        assert isinstance(listfullnames, list)

        # assume uncompressed and compressed files have same metadata
        # choosing either doesn't matter
        byfilename = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            byfilename[filename] = fname

        #self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"Loading filename_gtt with: {list(byfilename.keys())}")
        self.dbh.load_filename_gtt(list(byfilename.keys()))

        metadata_table = self.config['filetype_metadata'][self.filetype]['metadata_table']

        if metadata_table.lower() == 'genfile':
            metadata_table = 'desfile'

        dbq = f"select m.filename from {metadata_table} m, {dmdbdefs.DB_GTT_FILENAME} g where m.filename=g.filename"
        curs = self.dbh.cursor()
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"Metadata check query: {dbq}")
        curs.execute(dbq)

        results = {}
        for row in curs:
            results[byfilename[row[0]]] = True

        for fname in listfullnames:
            if fname not in results:
                results[fname] = False

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"Metadata check results: {results}")
        return results

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if file has contents ingested """
        assert isinstance(listfullnames, list)

        # 0 contents to ingest, so true
        results = {}
        for fname in listfullnames:
            results[fname] = True

        return results

    ######################################################################
    def check_valid(self, listfullnames):
        """ Check if a valid file of the filetype """

        assert isinstance(listfullnames, list)

        results = {}
        for fname in listfullnames:
            results[fname] = True

        return results

    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ Ingest certain content into a non-metadata table """
        pass

    ######################################################################
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # read metadata and call any special calc functions
        metadata = self._gather_metadata_file(fullname)

        if do_update:
            miscutils.fwdebug_print(f"WARN ({self.__class__.__name__}): skipping file metadata update.")

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"INFO: beg  file={fullname}")

        metadata = collections.OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"INFO: metadefs={metadefs}")
        for hdname, hddict in metadefs['hdus'].items():
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from filename
                if 'f' in hddict[status_sect]:
                    metakeys = list(hddict[status_sect]['f'].keys())
                    mdata2 = self._gather_metadata_from_filename(fullname, metakeys)
                    metadata.update(mdata2)

                # get value from wcl/config
                if 'w' in hddict[status_sect]:
                    metakeys = list(hddict[status_sect]['w'].keys())
                    mdata2 = self._gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    miscutils.fwdie(f"ERROR ({self.__class__.__name__}): cannot read values from header {hdname} = {list(hddict[status_sect]['h'].keys())}", 1)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    miscutils.fwdie(f"ERROR ({self.__class__.__name__}): cannot calculate values = {list(hddict[status_sect]['c'].keys())}", 1)

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    miscutils.fwdie(f"ERROR ({self.__class__.__name__}): cannot copy values between headers = {list(hddict[status_sect]['p'].keys())}", 1)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_from_config(self, fullname, metakeys):
        """ Get values from config """
        metadata = collections.OrderedDict()

        for wclkey in metakeys:
            metakey = wclkey.split('.')[-1]
            if metakey == 'fullname':
                metadata['fullname'] = fullname
            elif metakey == 'filename':
                metadata['filename'] = miscutils.parse_fullname(fullname,
                                                                miscutils.CU_PARSE_FILENAME)
            elif metakey == 'filetype':
                metadata['filetype'] = self.filetype
            else:
                if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"INFO: wclkey={wclkey}")
                (exists, val) = self.config.search(wclkey)
                if exists:
                    metadata[metakey] = val

        return metadata


    ######################################################################
    def _gather_metadata_from_filename(self, fullname, metakeys):
        """ Parse filename using given filepat """

        if self.filepat is None:
            raise TypeError(f"None filepat for filetype {self.filetype}")

        # change wcl file pattern into a pattern usable by re
        newfilepat = copy.deepcopy(self.filepat)
        varpat = r"\$\{([^$}]+:\d+)\}|\$\{([^$}]+)\}"
        listvar = []
        m = re.search(varpat, newfilepat)
        while m:
            if m.group(1) is not None:
                m2 = re.search(r'([^:]+):(\d+)', m.group(1))
                #print m2.group(1), m2.group(2)
                listvar.append(m2.group(1))

                # create a pattern that will remove the 0-padding
                newfilepat = re.sub(fr"\${{{m.group(1)}}}", fr'(\\d{{{m2.group(2)}}})', newfilepat)
            else:
                newfilepat = re.sub(fr"\${{{m.group(2)}}}", r'(\\S+)', newfilepat)
                listvar.append(m.group(2))

            m = re.search(varpat, newfilepat)


        # now that have re pattern, parse the filename for values
        filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"INFO: newfilepat = {newfilepat}")
            miscutils.fwdebug_print(f"INFO: filename = {filename}")

        m = re.search(newfilepat, filename)
        if m is None:
            miscutils.fwdebug_print(f"INFO: newfilepat = {newfilepat}")
            miscutils.fwdebug_print(f"INFO: filename = {filename}")
            raise ValueError(f"Pattern ({newfilepat}) did not match filename ({filename})")

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"INFO: m.group() = {m.group()}")
            miscutils.fwdebug_print(f"INFO: listvar = {listvar}")

        # only save values parsed from filename that were requested per metakeys
        mddict = {}
        for cnt, val in enumerate(listvar):
            key = val
            if key in metakeys:
                if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print(f"INFO: saving as metadata key = {key}, cnt = {cnt}")
                mddict[key] = m.group(cnt + 1)
            elif miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print(f"INFO: skipping key = {key} because not in metakeys")


        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"INFO: mddict = {mddict}")

        return mddict
