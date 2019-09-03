#!/usr/bin/env python

# $Id: ftmgmt_generic.py 46423 2017-12-19 21:07:55Z friedel $
# $Rev:: 46423                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-12-19 15:07:55 #$:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev: 46423 $"

from collections import OrderedDict
import copy
import re
#import time

import despymisc.miscutils as miscutils
import despydmdb.dmdb_defs as dmdbdefs


class FtMgmtGeneric(object):
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
        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("Loading filename_gtt with: %s" % byfilename.keys())
        self.dbh.load_filename_gtt(byfilename.keys())

        metadata_table = self.config['filetype_metadata'][self.filetype]['metadata_table']

        if metadata_table.lower() == 'genfile':
            metadata_table = 'desfile'

        dbq = "select m.filename from %s m, %s g where m.filename=g.filename" % \
                 (metadata_table, dmdbdefs.DB_GTT_FILENAME)
        curs = self.dbh.cursor()
        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("Metadata check query: %s" % dbq)
        curs.execute(dbq)

        results = {}
        for row in curs:
            results[byfilename[row[0]]] = True

        for fname in listfullnames:
            if fname not in results:
                results[fname] = False

        #self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("Metadata check results: %s" % results)
        return results

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if file has contents ingested """
        #starttime = time.time()
        assert isinstance(listfullnames, list)

        # 0 contents to ingest, so true
        results = {}
        for fname in listfullnames:
            results[fname] = True
        #print "   HCI-gen:  %.3f" % (time.time()-starttime)

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

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: beg")

        # read metadata and call any special calc functions
        metadata = self._gather_metadata_file(fullname)

        #if do_update:
        #    miscutils.fwdebug_print("WARN (%s): skipping file metadata update." % \
                                    self.__class__.__name__)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file """

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: beg  file=%s" % (fullname))

        metadata = OrderedDict()

        metadefs = self.config['filetype_metadata'][self.filetype]
        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: metadefs=%s" % (metadefs))
        for hdname, hddict in metadefs['hdus'].items():
            for status_sect in hddict:  # don't worry about missing here, ingest catches
                # get value from filename
                if 'f' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['f'].keys()
                    mdata2 = self._gather_metadata_from_filename(fullname, metakeys)
                    metadata.update(mdata2)

                # get value from wcl/config
                if 'w' in hddict[status_sect]:
                    metakeys = hddict[status_sect]['w'].keys()
                    mdata2 = self._gather_metadata_from_config(fullname, metakeys)
                    metadata.update(mdata2)

                # get value directly from header
                if 'h' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot read values from header %s = %s" % \
                                    (self.__class__.__name__, hdname,
                                     hddict[status_sect]['h'].keys()), 1)

                # calculate value from different header values(s)
                if 'c' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot calculate values = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['c'].keys()), 1)

                # copy value from 1 hdu to primary
                if 'p' in hddict[status_sect]:
                    miscutils.fwdie("ERROR (%s): cannot copy values between headers = %s" % \
                                    (self.__class__.__name__, hddict[status_sect]['p'].keys()), 1)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def _gather_metadata_from_config(self, fullname, metakeys):
        """ Get values from config """
        metadata = OrderedDict()

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
                #if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                #    miscutils.fwdebug_print("INFO: wclkey=%s" % (wclkey))
                (exists, val) = self.config.search(wclkey)
                if exists:
                    metadata[metakey] = val

        return metadata


    ######################################################################
    def _gather_metadata_from_filename(self, fullname, metakeys):
        """ Parse filename using given filepat """

        if self.filepat is None:
            raise TypeError("None filepat for filetype %s" % self.filetype)

        # change wcl file pattern into a pattern usable by re
        newfilepat = copy.deepcopy(self.filepat)
        varpat = r"\$\{([^$}]+:\d+)\}|\$\{([^$}]+)\}"
        listvar = []
        m = re.search(varpat, newfilepat)
        while m:
            #print m.group(1), m.group(2)
            if m.group(1) is not None:
                m2 = re.search('([^:]+):(\d+)', m.group(1))
                #print m2.group(1), m2.group(2)
                listvar.append(m2.group(1))

                # create a pattern that will remove the 0-padding
                newfilepat = re.sub(r"\${%s}" % (m.group(1)), '(\d{%s})' % m2.group(2), newfilepat)
            else:
                newfilepat = re.sub(r"\${%s}" % (m.group(2)), '(\S+)', newfilepat)
                listvar.append(m.group(2))

            m = re.search(varpat, newfilepat)


        # now that have re pattern, parse the filename for values
        filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: newfilepat = %s" % newfilepat)
        #    miscutils.fwdebug_print("INFO: filename = %s" % filename)

        m = re.search(newfilepat, filename)
        if m is None:
            miscutils.fwdebug_print("INFO: newfilepat = %s" % newfilepat)
            miscutils.fwdebug_print("INFO: filename = %s" % filename)
            raise ValueError("Pattern (%s) did not match filename (%s)" % (newfilepat, filename))

        #if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: m.group() = %s" %  m.group())
        #    miscutils.fwdebug_print("INFO: listvar = %s" % listvar)

        # only save values parsed from filename that were requested per metakeys
        mddict = {}
        for cnt in range(0, len(listvar)):
            key = listvar[cnt]
            if key in metakeys:
                #if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                #    miscutils.fwdebug_print("INFO: saving as metadata key = %s, cnt = %s" % (key, cnt))
                mddict[key] = m.group(cnt+1)
            #elif miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            #    miscutils.fwdebug_print("INFO: skipping key = %s because not in metakeys" % key)


        #if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
        #    miscutils.fwdebug_print("INFO: mddict = %s" % mddict)

        return mddict
