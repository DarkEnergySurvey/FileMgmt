# $Id: ftmgmt_raw.py 46337 2017-10-20 13:42:15Z friedel $
# $Rev:: 46337                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-10-20 08:42:15 #$:  # Date of last commit.

"""
Generic filetype management class used to do filetype specific tasks
     such as metadata and content ingestion
"""

__version__ = "$Rev: 46337 $"

from datetime import datetime
import os

from astropy.io import fits
import despydmdb.dmdb_defs as dmdbdefs
from filemgmt.ftmgmt_genfits import FtMgmtGenFits
import despymisc.miscutils as miscutils
import despymisc.create_special_metadata as spmeta

class FtMgmtRaw(FtMgmtGenFits):
    """  Class for managing a raw filetype (get metadata, update metadata, etc) """

    ######################################################################
    def __init__(self, filetype, dbh, config, filepat=None):
        """ Initialize object """
        # config must have filetype_metadata, file_header_info, keywords_file (OPT)
        super().__init__(self, filetype, dbh, config, filepat)


    ######################################################################
    def has_contents_ingested(self, listfullnames):
        """ Check if exposure has row in rasicam_decam table """

        assert isinstance(listfullnames, list)

        # assume uncompressed and compressed files have same metadata
        # choosing either doesn't matter
        byfilename = {}
        for fname in listfullnames:
            filename = miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME)
            byfilename[filename] = fname

        #self.dbh.empty_gtt(dmdbdefs.DB_GTT_FILENAME)
        self.dbh.load_filename_gtt(list(byfilename.keys()))

        dbq = f"select r.filename from rasicam_decam r, {dmdbdefs.DB_GTT_FILENAME} g where r.filename=g.filename"
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
    def perform_metadata_tasks(self, fullname, do_update, update_info):
        """ Read metadata from file, updating file values """

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: beg")

        # open file
        #hdulist = pyfits.open(fullname, 'update')
        primary_hdr = fits.getheader(fullname, 0)
        prihdu = fits.PrimaryHDU(header=primary_hdr)
        hdulist = fits.HDUList([prihdu])


        # read metadata and call any special calc functions
        metadata, _ = self._gather_metadata_file(fullname, hdulist=hdulist)
        if miscutils.fwdebug_check(6, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print(f"INFO: file={fullname}")

        # call function to update headers
        if do_update:
            miscutils.fwdebug_print("WARN: cannot update a raw file's metadata")

        # close file
        hdulist.close()

        if miscutils.fwdebug_check(3, 'FTMGMT_DEBUG'):
            miscutils.fwdebug_print("INFO: end")
        return metadata


    ######################################################################
    def ingest_contents(self, listfullnames, **kwargs):
        """ Ingest data into non-metadata table - rasicam_decam"""

        assert isinstance(listfullnames, list)

        dbtable = 'rasicam_decam'

        for fullname in listfullnames:
            if not os.path.isfile(fullname):
                raise OSError(f"Exposure file not found: '{fullname}'")

            filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

            primary_hdr = None
            if 'prihdr' in kwargs:
                primary_hdr = kwargs['prihdr']
            elif 'hdulist' in kwargs:
                hdulist = kwargs['hdulist']
                primary_hdr = hdulist[0].header
            else:
                primary_hdr = fits.getheader(fullname, 0)

            row = get_vals_from_header(primary_hdr)
            row['filename'] = filename
            row['source'] = 'HEADER'
            row['analyst'] = 'DTS.ingest'

            if row:
                self.dbh.basic_insert_row(dbtable, row)
            else:
                raise Exception(f"No RASICAM header keywords identified for {filename}")


    ######################################################################
    def check_valid(self, listfullnames):
        """ Check whether the given files are valid raw files """

        assert isinstance(listfullnames, list)

        results = {}
        for fname in listfullnames:
            results[fname] = False

        keyfile = None
        if 'raw_keywords_file' in self.config:
            keyfile = self.config['raw_keywords_file']
        elif 'FILEMGMT_DIR' in os.environ:
            keyfile = f"{os.environ['FILEMGMT_DIR']}/etc/decam_src_keywords.txt"

        miscutils.fwdebug_print(f"keyfile = {keyfile}")
        if keyfile is not None and os.path.exists(keyfile):
            keywords = {'pri':{}, 'ext':{}}
            with open(keyfile, 'r') as keyfh:
                for line in keyfh:
                    line = line.upper()
                    (keyname, pri, ext) = miscutils.fwsplit(line, ',')[0:3]
                    if pri not in ['Y', 'N', 'R']:
                        raise ValueError(f'Invalid primary entry in keyword file ({line})')
                    if ext not in ['Y', 'N', 'R']:
                        raise ValueError(f'Invalid extenstion entry in keyword file ({line})')
                    keywords['pri'][keyname] = pri
                    keywords['ext'][keyname] = ext

            for fname in listfullnames:
                results[fname] = check_single_valid(keywords, fname, 0)
        else:
            raise OSError('Error:  Could not find keywords file')

        return results


######################################################################
def check_single_valid(keywords, fullname, verbose): # should raise exception if not valid
    """ Check whether the given file is a valid raw file """

    # check fits file
    hdulist = fits.open(fullname)
    prihdr = hdulist[0].header

    # check exposure has correct filename (sometimes get NOAO-science-archive renamed exposures)
    correct_filename = prihdr['FILENAME']
    actual_filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)
    if actual_filename != correct_filename:
        raise ValueError(f'Error: invalid filename ({actual_filename})')


    instrume = prihdr['INSTRUME'].lower()

    req_num_hdus = -1
    if instrume == 'decam':
        req_num_hdus = 71
    else:
        raise ValueError(f'Error:  Unknown instrume ({instrume})')

    # check # hdus
    num_hdus = len(hdulist)
    if num_hdus != req_num_hdus:
        raise ValueError(f'Error:  Invalid number of hdus ({num_hdus})')

    # check keywords
    for hdunum in range(0, num_hdus):
        hdr = hdulist[hdunum].header
        (req, want, extra) = check_header_keywords(keywords, hdunum, hdr)

        if verbose > 1:
            if want is not None and want:
                print(f"HDU #{hdunum:02d} Missing requested keywords: {want}")
            if extra is not None and extra:
                print(f"HDU #{hdunum:02d} Extra keywords: {extra}")

        if req is not None and req:
            raise ValueError(f'Error: HDU #{hdunum:02d} Missing required keywords ({req})')

    return True

######################################################################
def check_header_keywords(keywords, hdunum, hdr):
    """ Check for keywords in header """
    # missing has the keywords which are missing in the file and are required for processing
    # extra are the keywords which are not required and are present in the system
    # not required are the ones which are not required and are not present

    req_missing = []
    want_missing = []
    extra = []

    hdutype = 'ext'
    if hdunum == 0:
        hdutype = 'pri'

    for keyw, status in keywords[hdutype].items():
        if keyw not in hdr:
            if status == 'R':
                req_missing.append(keyw)
            elif status == 'Y':
                want_missing.append(keyw)

    # check for extra keywords
    for keyw in hdr:
        if keyw not in keywords[hdutype] or keywords[hdutype][keyw] == 'N':
            extra.append(keyw)

    return (req_missing, want_missing, extra)

######################################################################
def get_vals_from_header(primary_hdr):
    """ Helper function for ingest_contents to get values from primary header
        for insertion into rasicam_DECam table """

    #  Keyword list needed to update the database.
    #     i=int, f=float, b=bool, s=str, date=date
    keylist = {'EXPNUM': 'i',
               'INSTRUME': 's',
               'SKYSTAT': 'b',
               'SKYUPDAT': 'date',
               'GSKYPHOT': 'b',
               'LSKYPHOT': 'b',
               'GSKYVAR': 'f',
               'GSKYHOT': 'f',
               'LSKYVAR': 'f',
               'LSKYHOT': 'f',
               'LSKYPOW': 'f'}

    vals = {}
    for key, ktype in keylist.items():
        key = key.upper()
        if key in primary_hdr:
            value = primary_hdr[key]
            if key == 'SKYUPDAT':  # entry_time is time exposure taken
                vals['ENTRY_TIME'] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
            elif key == 'INSTRUME':
                vals['CAMSYM'] = spmeta.create_camsym(value)
            elif ktype == 'b':
                if value:
                    vals[key] = 'T'
                else:
                    vals[key] = 'F'
            elif ktype == 'i':
                if value != 'NaN':
                    vals[key] = int(value)
            else:
                if value != 'NaN':
                    vals[key] = float(value)

    return vals
