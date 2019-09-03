#!/usr/bin/env python

# $Id: ftmgmt_coadd_xml_scamp.py 41948 2016-05-23 14:27:22Z mgower $
# $Rev:: 41948                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-05-23 09:27:22 #$:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev: 41948 $"

from filemgmt.ftmgmt_datafile import FtMgmtDatafile

import despymisc.miscutils as miscutils
import databaseapps.datafile_ingest_utils as dfiutils

class FtMgmtCoaddXmlScamp(FtMgmtDatafile):
    """  Class for managing filetype coadd_xml_scamp which needs data stored in 2 tables """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtDatafile.__init__(self, filetype, dbh, config, filepat=None)

        #self.filetype should be 'coadd_xml_scamp'
        self.filetype2 = 'coadd_xml_scamp_2'
        [self.tablename2, self.didatadefs2] = self.dbh.get_datafile_metadata(self.filetype2)


    ######################################################################
    #def has_contents_ingested(self, listfullnames):
    #   For now, assume if ingested into table 1 then also ingested into table 2


    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ Ingest certain content into a non-metadata table """

        assert(isinstance(listfullnames, list))

        for fname in listfullnames:
            miscutils.fwdebug_print("********************* %s" % fname)
            numrows = dfiutils.datafile_ingest_main(self.dbh, self.filetype, fname,
                                                    self.tablename, self.didatadefs)
            if numrows == None or numrows == 0:
                miscutils.fwdebug_print("WARN: 0 rows ingested from %s for table %s" % \
                                        (self.tablename, fname))
            elif miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: %s rows ingested from %s for table %s" % \
                                        (numrows, self.tablename2, fname))

            numrows = dfiutils.datafile_ingest_main(self.dbh, self.filetype2, fname,
                                                    self.tablename2, self.didatadefs2)
            if numrows == None or numrows == 0:
                miscutils.fwdebug_print("WARN: 0 rows ingested from %s for table %s" % \
                                        (self.tablename2, fname))
            elif miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("INFO: %s rows ingested from %s for table %s" % 
                                        (numrows, self.tablename2, fname))
