# $Id: job_mvmt_local.py 46423 2017-12-19 21:07:55Z friedel $
# $Rev:: 46423                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-12-19 15:07:55 #$:  # Date of last commit.

"""
"""

__version__ = "$Rev: 46423 $"

import os
import copy

import despymisc.miscutils as miscutils
import filemgmt.disk_utils_local as disk_utils_local

class JobArchiveLocal:
    """
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        return {}

    def __init__(self, homeinfo, targetinfo, mvmtinfo, tstats, config=None):
        self.home = homeinfo
        self.target = targetinfo
        self.mvmt = mvmtinfo
        self.config = config
        self.tstats = tstats


    def home2job(self, filelist):
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"len(filelist)={len(filelist)}")
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"filelist={filelist}")

        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")

        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = os.path.join(self.home['root'], finfo['src'])

        if self.tstats is not None:
            self.tstats.stat_beg_batch('home2job', self.home['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def target2job(self, filelist):
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"len(filelist)={len(filelist)}")
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"filelist={filelist}")
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = os.path.join(self.target['root'], finfo['src'])
        if self.tstats is not None:
            self.tstats.stat_beg_batch('target2job', self.target['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2target(self, filelist):
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"len(filelist)={len(filelist)}")
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"filelist={filelist}")
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = os.path.join(self.target['root'], finfo['dst'])
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2target', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2home(self, filelist, verify=False):
        if miscutils.fwdebug_check(3, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"len(filelist)={len(filelist)}")
        if miscutils.fwdebug_check(6, "JOBFILEMVMT_DEBUG"):
            miscutils.fwdebug_print(f"filelist={filelist}")
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = os.path.join(self.home['root'], finfo['dst'])
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2home', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = disk_utils_local.copyfiles(absfilelist, self.tstats, verify)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results
