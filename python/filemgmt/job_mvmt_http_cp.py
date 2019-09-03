# $Id: job_mvmt_http_cp.py 45126 2017-03-02 18:30:00Z friedel $
# $Rev:: 45126                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-03-02 12:30:00 #$:  # Date of last commit.

"""
    Module to use when http for transfers between job and home archive, and
    cp between job and target archive
"""

__version__ = "$Rev: 45126 $"

import copy

import despymisc.miscutils as miscutils
import filemgmt.http_utils as http_utils
import filemgmt.disk_utils_local as disk_utils_local

DES_SERVICES = 'des_services'
DES_HTTP_SECTION = 'des_http_section'

class JobArchiveHttpCp(object):
    """
        Use http for transfers between job and home archive, and
        cp between job and target archive
    """

    @staticmethod
    def requested_config_vals():
        """ Tell which values are req/opt for this object """
        return {DES_SERVICES:'REQ', DES_HTTP_SECTION:'REQ'}

    def __init__(self, homeinfo, targetinfo, mvmtinfo, tstats, config=None):
        """ initialize object """
        self.home = homeinfo
        self.target = targetinfo
        self.mvmt = mvmtinfo
        self.config = config
        self.tstats = tstats

        for reqkey in (DES_SERVICES, DES_HTTP_SECTION):
            if reqkey not in self.config:
                miscutils.fwdie('Error:  Missing %s in config' % reqkey, 1)
        self.HU = http_utils.HttpUtils(self.config[DES_SERVICES],
                                       self.config[DES_HTTP_SECTION])

    def home2job(self, filelist):
        """ From inside job, pull files from home archive to job scratch directory """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")

        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.home['root_http'] + '/' + finfo['src']

        if self.tstats is not None:
            self.tstats.stat_beg_batch('home2job', self.home['name'], 'job_scratch',
                                       self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results

    def target2job(self, filelist):
        """ From inside job, pull files from target archive to job scratch directory """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.target['root'] + '/' + finfo['src']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('target2job', self.target['name'], 'job_scratch',
                                       self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2target(self, filelist):
        """ From inside job, push files to target archive from job scratch directory """
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.target['root'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2target', 'job_scratch', self.home['name'],
                                       self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results

    def job2home(self, filelist, verify=False):
        """ From inside job, push files to home archive from job scratch directory """
        # if staging outside job, this function shouldn't be called
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("len(filelist)=%s" % len(filelist))
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print("filelist=%s" % filelist)
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.home['root_http'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2home', 'job_scratch', self.home['name'],
                                       self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats, verify)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results
