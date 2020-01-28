# $Id: ftmgmt_datafile.py 41700 2016-04-19 19:23:55Z mgower $
# $Rev:: 41700                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-04-19 14:23:55 #$:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev: 41700 $"

from filemgmt.ftmgmt_generic import FtMgmtGeneric

import despymisc.miscutils as miscutils
import databaseapps.datafile_ingest_utils as dfiutils
#import time

class FtMgmtDatafile(FtMgmtGeneric):
    """  Class for managing a filetype whose contents can be read by datafile_ingest """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        super().__init__(filetype, dbh, config, filepat=None)

        [self.tablename, self.didatadefs] = self.dbh.get_datafile_metadata(filetype)

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if file has contents ingested """
        #starttime = time.time()

        assert isinstance(listfullnames, list)

        results = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            results[fname] = dfiutils.is_ingested(filename, self.tablename, self.dbh)
        return results

    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ Ingest certain content into a non-metadata table """
        #starttime = time.time()

        assert isinstance(listfullnames, list)

        for fname in listfullnames:
            #miscutils.fwdebug_print("********************* %s" % fname)
            _ = dfiutils.datafile_ingest_main(self.dbh, self.filetype, fname,
                                              self.tablename, self.didatadefs)
            #if numrows == None or numrows == 0:
            #    miscutils.fwdebug_print("WARN: 0 rows ingested from %s" % fname)
            #elif miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
            #    miscutils.fwdebug_print("INFO: %s rows ingested from %s" % (numrows, fname))
