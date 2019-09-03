#!/usr/bin/env python

# $Id: transfer_stats_nodb.py 48052 2019-01-08 19:53:16Z friedel $
# $Rev:: 48052                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-08 13:53:16 #$:  # Date of last commit.

"""
    Define a database utility class for tracking transfer statistics
"""

__version__ = "$Rev: 48052 $"

import ConfigParser
import despymisc.miscutils as miscutils
import despydmdb.desdmdbi as desdmdbi
import time

class TransferStatsDB(desdmdbi.DesDmDbi):
    """
        Class with functionality for tracking transfer statistics in DB
    """

    @staticmethod
    def requested_config_vals():
        """ return dictionary describing what values this class uses along
            with whether they are optional or required """
        return {'transfer_stats_per_file':'opt'}


    def __init__(self, config):

        if 'transfer_stats_per_file' in config:
            self.transfer_stats_per_file = miscutils.convertBool(config['transfer_stats_per_file'])
        else:
            self.transfer_stats_per_file = False

        self.batchvals = {}
        self.filevals = {}
        self.__initialize_values__()

    def __initialize_values__(self):
        self.batchvals = {}
        self.batchvals['transfer_name'] = None
        self.batchvals['transfer_class'] = None
        self.batchvals['totbytes'] = 0
        self.batchvals['numfiles'] = 0
        self.batchvals['src'] = None
        self.batchvals['dst'] = None
        self.batchvals['start_time'] = None
        self.batchvals['end_time'] = None
        self.batchvals['status'] = None

        self.filevals = {}
        self.filevals['filename'] = None
        self.filevals['numbytes'] = None
        self.filevals['start_time'] = None
        self.filevals['end_time'] = None
        self.filevals['status'] = None

    def __str__(self):
        return str(self.batchvals) + " ; " + str(self.filevals)


    ############################################################
    def print_batch_stats(self):
        """ Print stats for transfer batch """

        # current epoch time, numfiles, numbytes, trans secs, status
        print "TRANS_STATS_BATCH: %s %s %s %s %s %s" % \
              (time.time(), self.batchvals['transfer_name'],
               self.batchvals['numfiles'], self.filevals['totbytes'],
               self.filevals['end_time'] - self.filevals['start_time'],
               self.filevals['status'])


    ############################################################
    def print_file_stats(self):
        """ Print stats for transfer file """

        # current epoch time, file number, filename, filesize, trans secs, status
        print "TRANS_STATS_FILE: %s %s %s %s %s %s" % \
              (time.time(), self.batchvals['numfiles'],
               self.filevals['filename'], self.filevals['numbytes'],
               self.filevals['end_time'] - self.filevals['start_time'],
               self.filevals['status'])

    ############################################################
    def stat_beg_batch(self, transfer_name, src, dst, transclass=None):
        """ Starting a batch transfer between src and dst (archive or job scratch) """

        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print("beg %s %s %s %s" % \
                                    (transfer_name, src, dst, transclass))
        self.batchvals['transfer_name'] = transfer_name
        self.batchvals['src'] = src
        self.batchvals['dst'] = dst
        self.batchvals['transfer_class'] = transclass
        self.batchvals['start_time'] = time.time.now()

        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print("end")
        return -1


    ############################################################
    def stat_end_batch(self, status, totbytes=0, numfiles=0, task_id=None):
        """ Update rows for end of a batch transfer and commit """

        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print("beg - %s %s %s %s" % (status, totbytes, numfiles, task_id))

        self.batchvals['status'] = status
        self.batchvals['end_time'] = time.time.now()
        if totbytes != 0:
            self.batchvals['totbytes'] = totbytes
        if numfiles != 0:
            self.batchvals['numfiles'] = numfiles

        print_batch("Batch Copy info:")

        self.__initialize_values__()
        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print("end")


    ############################################################
    def stat_beg_file(self, filename):
        """ save file transfer start info """

        self.batchvals['numfiles'] += 1
        self.filevals['filename'] = filename
        self.filevals['start_time'] = timt.time()

        return -1

    ############################################################
    def stat_end_file(self, status, nbytes=0, task_id=None):
        """ save file transfer end info and print info """

        self.filevals['end_time'] = time.time()
        self.filevals['status'] = status

        if nbytes != 0:
            self.filevals['numbytes'] = nbytes
            self.batchvals['totbytes'] += nbytes

        if self.transfer_stats_per_file:
            self.print_file_stats()
