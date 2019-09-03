# $Id: job_mvmt_easywebdav.py 46423 2017-12-19 21:07:55Z friedel $
# $Rev:: 46423                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2017-12-19 15:07:55 #$:  # Date of last commit.

"""
"""

__version__ = "$Rev: 46423 $"

import copy
import re

import despymisc.miscutils as miscutils
import filemgmt.easywebdav_utils as ewd_utils

DES_SERVICES = 'des_services'
DES_HTTP_SECTION = 'des_http_section'

class JobArchiveEwd(object):
    """
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        return {DES_SERVICES:'REQ', DES_HTTP_SECTION:'REQ'}

    def __init__(self, homeinfo, targetinfo, mvmtinfo, tstats, config=None):
        self.home = homeinfo
        self.target = targetinfo
        self.mvmt = mvmtinfo
        self.config = config
        self.tstats = tstats

        m = re.match("(http://[^/]+)(/.*)", homeinfo['root_http'])
        dest = m.group(1)
        for x in (DES_SERVICES, DES_HTTP_SECTION):
            if x not in self.config:
                miscutils.fwdie('Error:  Missing %s in config' % x, 1)
        self.HU = ewd_utils.EwdUtils(self.config[DES_SERVICES],
                                     self.config[DES_HTTP_SECTION],
                                     dest.replace('http://', ''))


    def home2job(self, filelist):
        # if staging outside job, this function shouldn't be called
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")

        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.home['root_http'] + '/' + finfo['src']

        if self.tstats is not None:
            self.tstats.stat_beg_batch('home2job', self.home['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def target2job(self, filelist):
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['src'] = self.target['root_http'] + '/' + finfo['src']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('target2job', self.target['name'], 'job_scratch', self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2target(self, filelist):
        if self.target is None:
            raise Exception("Target archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.target['root_http'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2target', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results


    def job2home(self, filelist, verify=False):
        # if staging outside job, this function shouldn't be called
        if verify:
            ver = 'F'
        else:
            ver = None
        if self.home is None:
            raise Exception("Home archive info is None.   Should not be calling this function")
        absfilelist = copy.deepcopy(filelist)
        for finfo in absfilelist.values():
            finfo['dst'] = self.home['root_http'] + '/' + finfo['dst']
        if self.tstats is not None:
            self.tstats.stat_beg_batch('job2home', 'job_scratch', self.home['name'], self.__module__ + '.' + self.__class__.__name__)
        (status, results) = self.HU.copyfiles(absfilelist, self.tstats, verify=ver)
        if self.tstats is not None:
            self.tstats.stat_end_batch(status)
        return results
