#!/usr/bin/env python

# $Id: ftmgmt_snmanifest.py 46423 2017-12-19 21:07:55Z friedel $
# $Rev:: 46423                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-12-19 15:07:55 #$:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev: 46423 $"

import cx_Oracle
import json

from collections import defaultdict

from filemgmt.ftmgmt_generic import FtMgmtGeneric
import despydmdb.dmdb_defs as dmdbdefs
import despymisc.miscutils as miscutils
import filemgmt.fmutils as fmutils
import despymisc.misctime as misctime


class FtMgmtSNManifest(FtMgmtGeneric):
    """  Base/generic class for managing a filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata and file_header_info
        FtMgmtGeneric.__init__(self, filetype, dbh, config, filepat)

    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if file has contents ingested """

        assert isinstance(listfullnames, list)

        # assume uncompressed and compressed files have same metadata
        # choosing either doesn't matter
        byfilename = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            byfilename[filename] = fname

        #self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)
        self.dbh.load_filename_gtt(byfilename.keys())

        dbq = "select m.manifest_filename from %s m, %s g where m.manifest_filename=g.filename" % \
                 ("MANIFEST_EXPOSURE", dmdbdefs.DB_GTT_FILENAME)
        curs = self.dbh.cursor()
        curs.execute(dbq)

        results = {}
        for row in curs:
            results[byfilename[row[0]]] = True

        for fname in listfullnames:
            if fname not in results:
                results[fname] = False

        #self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        return results

    ######################################################################
    def _gather_metadata_file(self, fullname, **kwargs):
        """ Gather metadata for a single file """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg  file=%s" % (fullname))

        metadata = FtMgmtGeneric._gather_metadata_file(self, fullname, **kwargs)

        # need nite for the archive path
        with open(fullname, 'r') as jsonfh:
            line = jsonfh.readline()
            linedata = json.loads(line)
            expcnt = 0
            while expcnt < len(linedata['exposures']) and \
                  'date' not in linedata['exposures'][expcnt]:
                expcnt += 1
            if expcnt >= len(linedata['exposures']):
                raise KeyError('Could not find date value for any exposure in manifest')
            datestr = linedata['exposures'][expcnt]['date']
            metadata['nite'] = misctime.convert_utc_str_to_nite(datestr)

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ reads json manifest file and ingest into the DB tables
            EXPOSURES_IN_MANIFEST and SN_SUBMIT_REQUEST values needed to
            determine arrival of exposures taken for a SN field."""

        assert isinstance(listfullnames, list)

        all_mandatory_exposure_keys = ['expid', 'object', 'date', 'acttime', 'filter']

        for fname in listfullnames:
            all_exposures = fmutils.read_json_single(fname, all_mandatory_exposure_keys)
            self.ingestall_exposures(all_exposures)


    ######################################################################
    def insert_dictionary_db(self, query, dictionary):
        """Execute a query and return a cursor to a query
        :param query: string with query statement
        :param dictionary: dictionary to use in query

        """

        try:
            cur = self.dbh.cursor()
            cur.execute(query, dictionary)
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("dictionary into database " % (dictionary))
            success = 1
        #except cx_Oracle.IntegrityError as e:
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if error.code == 955:
                print 'Table already exists'
            elif error.code == 1031:
                print 'Insufficient privileges'
            print error.code
            print error.message
            print error.context
            success = 0
            raise
        return success

    ######################################################################
    def ingestall_exposures(self, all_exposures):
        """
        Ingest all the exposures in EXPOSURES_IN_MANIFEST and SN_SUBMIT_REQUEST

        #If SEQNUM is > 1, then it means the same field was taken again during the same night.
        #This will only happens in rare occasion when the sequence had to be aborted before
        #  it finished.

        :param all_exposures: Dictionary with the following keys:
        [set_type,createdAt,expid,object,date,acttime,filter]

        """


        newdicttionary = {}
        for key in ['CAMSYM', 'EXPNUM', 'MANIFEST_FILENAME', 'FIELD', 'BAND', 'EXPTIME', 'NITE']:
            newdicttionary[key] = all_exposures[key]

        #print "xx", all_exposures
        dict2ingest = {}
        for i in range(len(all_exposures['EXPTIME'])):
            for key in newdicttionary.keys():
                keytoingest = key
                valuetoingest = newdicttionary[key][i]
                dict2ingest[keytoingest] = valuetoingest
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("dict2ingest %s " % (dict2ingest))
            try:
                sql = """insert into MANIFEST_EXPOSURE (CAMSYM,EXPNUM,MANIFEST_FILENAME,NITE,FIELD,BAND,EXPTIME) VALUES
                                    (:CAMSYM, :EXPNUM, :MANIFEST_FILENAME, :NITE, :FIELD, :BAND, :EXPTIME)"""


                if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("sql = %s " % (sql))
                success = self.insert_dictionary_db(sql, dict2ingest)

                if success and miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("Insert into EXPOSURES_IN_MANIFEST was successful..")

            except cx_Oracle.IntegrityError as exc:
                print "error while inserting into EXPOSURES_IN_MANIFEST: ", exc
                raise


        ########################################################################################
        #
        #Fix first expnum. First expnum is the first exposure for each filter set. In case of
        #one a field with one filter exposure, then first_expnum = expnum.
        #For more than one exposure / band/field, then first_expnum = first exposure of set.
        #

        #Determine index of list for exptime = 10. (poiting exposure)
        allexps = all_exposures['EXPTIME']
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("all exptimes %s" % (allexps))
        for i, val in enumerate(allexps):
            if val == 10.0:
                pointing_index = i

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("pointing Exposure index is %s" % pointing_index)

        #find where there are repetead bands, but exclude the band where the exptime = 10
        list_bands = all_exposures['BAND']
        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("listOfaBands...%s" % list_bands)

        band_indexes = defaultdict(list)

        for i, item in enumerate(all_exposures['BAND']):
            band_indexes[item].append(i)

        #I have to loop trorugh the dictionary for all the bands. Cound how many bands. Get the vaues from this dictionary
        #which is the index to the list, and use that to determine the elementes for all the other dictionaries.
        #I need the follwoing elementsl 'FIELD','NITE','BAND','MANIFEST_FILENAME','FIRST_EXPNUM','SEQNUM'
        ind2use = []
        flag_first = 0
        for ind, band in enumerate(list_bands):
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("indexes %s %s" % (band_indexes[band], ind))
            if ind == pointing_index:
                if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("found pointing index %s %s " % (ind, pointing_index))
                continue
            else:
                #for two exposures and one of them is the poiting
                if len(band_indexes[band]) <= 2 and ind == pointing_index+1:
                    ind2use.append((max(band_indexes[band])))
                    #print "the index", ind2use
                #if there are more than 2 exposures (generally for deep fields
                elif len(band_indexes[band]) > 2 and ind == pointing_index+1:
                    ind2use.append(band_indexes[band][ind])
                    flag_first = 1
                elif len(band_indexes[band]) == 1:
                    ind2use.append(band_indexes[band][0])
                elif len(band_indexes[band]) == 2 and ind != pointing_index and flag_first == 0:
                    ind2use.append(min(band_indexes[band]))
                    flag_first = 1
            if flag_first:
                break

        #contruct the dictionary with only the elements that needs to go into the DB
        #To do this use the ind2use extracted from the above list.
        newdict = {}
        for index in ind2use:
            #print index
            newdict['FIELD'] = all_exposures['FIELD'][index]
            newdict['NITE'] = all_exposures['NITE'][index]
            newdict['BAND'] = all_exposures['BAND'][index]
            newdict['MANIFEST_FILENAME'] = all_exposures['MANIFEST_FILENAME'][index]
            newdict['FIRST_EXPNUM'] = all_exposures['EXPNUM'][index]
            newdict['SEQNUM'] = all_exposures['SEQNUM'][index]
            if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
                miscutils.fwdebug_print("index=%s, newdict=%s" % (index, newdict))

            #Ingest into the database each of them
            try:
                sql = """insert into SN_SUBMIT_REQUEST (FIELD,NITE,BAND,MANIFEST_FILENAME,FIRST_EXPNUM,SEQNUM) VALUES
                                            (:FIELD, :NITE, :BAND, :MANIFEST_FILENAME, :FIRST_EXPNUM, :SEQNUM)"""

                if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("sql = %s" % sql)

                success = self.insert_dictionary_db(sql, newdict)
                if success and miscutils.fwdebug_check(1, 'FTMGMT_DEBUG'):
                    miscutils.fwdebug_print("Insert into SN_SUBMIT_REQUEST was successful..")

            except cx_Oracle.IntegrityError as exc:
                print "error while inserting into SN_SUBMIT_REQUEST: ", exc
                raise
