#!/usr/bin/env python3

# $Id: transfer_stats_db.py 48550 2019-05-20 19:28:17Z friedel $
# $Rev:: 48550                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:28:17 #$:  # Date of last commit.

"""
    Define a database utility class for tracking transfer statistics
"""

__version__ = "$Rev: 48550 $"

import configparser
import despymisc.miscutils as miscutils
import despydmdb.desdmdbi as desdmdbi

class TransferStatsDB(desdmdbi.DesDmDbi):
    """
        Class with functionality for tracking transfer statistics in DB
    """

    @staticmethod
    def requested_config_vals():
        """ return dictionary describing what values this class uses along
            with whether they are optional or required """
        return {'use_db':'opt', 'des_services':'opt', 'des_db_section':'req',
                'parent_task_id':'req', 'root_task_id':'req',
                'transfer_stats_per_file':'opt',
                'connection':'opt', 'threaded':'opt'}


    def __init__(self, config, fullcfg=None):
        if not miscutils.use_db(config):
            miscutils.fwdie("Error:  TransferStatsDB class requires DB "\
                            " but was told not to use DB", 1)

        self.currvals = {}
        self.__initialize_values__()

        self.desservices = None
        if config is not None and 'des_services' in config:
            self.desservices = config['des_services']

        self.section = None
        if config is not None and 'des_db_section' in config:
            self.section = config['des_db_section']
        have_connect = False
        if 'connection' in config:
            try:
                desdmdbi.DesDmDbi.__init__(self, connection=config['connection'])
                have_connect = True
            except:
                miscutils.fwdebug_print('Could not connect to DB using transferred connection, falling back to new connection.')
        if not have_connect:
            try:
                desdmdbi.DesDmDbi.__init__(self, self.desservices, self.section)
            except (configparser.NoSectionError, IOError) as err:
                miscutils.fwdie(f"Error: problem connecting to database: {err}\n" \
                                    "\tCheck desservices file and environment variables", 1)

        self.parent_task_id = config['parent_task_id']
        self.root_task_id = config['root_task_id']

        if 'transfer_stats_per_file' in config:
            self.transfer_stats_per_file = miscutils.convertBool(config['transfer_stats_per_file'])
        else:
            self.transfer_stats_per_file = False

    def __initialize_values__(self):
        self.currvals = {}
        self.currvals['batch_task_id'] = 0
        self.currvals['file_task_id'] = 0
        self.currvals['totbytes'] = 0
        self.currvals['numfiles'] = 0
        self.currvals['transfer_name'] = None
        self.currvals['src'] = None
        self.currvals['dst'] = None

    def __str__(self):
        mydict = {'batch_task_id': self.currvals['batch_task_id'],
                  'transfer_name': self.currvals['transfer_name'],
                  'src': self.currvals['src'],
                  'dst': self.currvals['dst'],
                  'totbytes': self.currvals['totbytes'],
                  'numfiles': self.currvals['numfiles'],
                  'file_task_id': self.currvals['file_task_id']}

        return str(mydict)


    def stat_beg_batch(self, transfer_name, src, dst, transclass=None):
        """ Starting a batch transfer between src and dst (archive or job scratch) """

        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print(f"beg {transfer_name} {src} {dst} {transclass}")
        self.currvals['transfer_name'] = transfer_name
        self.currvals['src'] = src
        self.currvals['dst'] = dst
        self.currvals['batch_task_id'] = self.create_task(name=transfer_name,
                                                          info_table='transfer_batch',
                                                          parent_task_id=self.parent_task_id,
                                                          root_task_id=self.root_task_id,
                                                          label=None,
                                                          do_begin=True,
                                                          do_commit=False)

        row = {'src': src, 'dst': dst,
               'transfer_class': transclass,
               'parent_task_id': self.parent_task_id,
               'task_id': self.currvals['batch_task_id']}

        self.basic_insert_row('transfer_batch', row)
        self.commit()

        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print("end")
        return self.currvals['batch_task_id']


    def stat_end_batch(self, status, totbytes=0, numfiles=0, task_id=None):
        """ Update rows for end of a batch transfer and commit """

        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print(f"beg - {status} {totbytes} {task_id}")
        if task_id is None:
            task_id = self.currvals['batch_task_id']
        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print(f'before end_task, info: {self}')
        self.end_task(task_id, status)
        wherevals = {'task_id': task_id}

        if totbytes == 0:
            totbytes = self.currvals['totbytes']
        if numfiles == 0:
            numfiles = self.currvals['numfiles']
        updatevals = {'total_num_bytes': totbytes,
                      'total_num_files': numfiles}

        self.basic_update_row('transfer_batch', updatevals, wherevals)
        self.commit()
        self.__initialize_values__()
        if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
            miscutils.fwdebug_print("end")

    def stat_beg_file(self, filename):
        """ Insert a row into a file transfer stats table (and task table) and commit """

        self.currvals['numfiles'] += 1
        self.currvals['file_task_id'] = -1

        if self.transfer_stats_per_file:
            if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
                miscutils.fwdebug_print(f"beg - {filename}")
            if self.currvals['batch_task_id'] is None:
                raise Exception('Cannot call this function without prior calling stat_beg_batch')

            row = {'filename': filename}
            row['task_id'] = self.create_task(name='transfer_file',
                                              info_table='transfer_file',
                                              parent_task_id=self.currvals['batch_task_id'],
                                              root_task_id=self.root_task_id,
                                              label=None,
                                              do_begin=True,
                                              do_commit=False)

            row['batch_task_id'] = self.currvals['batch_task_id']
            self.basic_insert_row('transfer_file', row)
            self.commit()

            self.currvals['file_task_id'] = row['task_id']
            if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
                miscutils.fwdebug_print(f"end - file_task_id = {self.currvals['file_task_id']}")
        return self.currvals['file_task_id']



    def stat_end_file(self, status, nbytes=0, task_id=None):
        """ Update rows for end of file transfer and commit """

        if nbytes is not None:
            self.currvals['totbytes'] += nbytes

        if self.transfer_stats_per_file:
            if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
                miscutils.fwdebug_print(f"beg - {status} {nbytes} {task_id}")

            if task_id is None:
                task_id = self.currvals['file_task_id']
            self.end_task(task_id, status)
            wherevals = {'task_id': task_id}
            updatevals = {'bytes': nbytes}

            self.basic_update_row('transfer_file', updatevals, wherevals)
            self.commit()

            if miscutils.fwdebug_check(3, 'TRANSFERSTATS_DEBUG'):
                miscutils.fwdebug_print("end")
