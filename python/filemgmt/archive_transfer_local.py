# $Id: archive_transfer_local.py 41008 2015-12-11 15:55:43Z mgower $
# $Rev:: 41008                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:55:43 #$:  # Date of last commit.

"""
"""

__version__ = "$Rev: 41008 $"


import copy
import despymisc.miscutils as miscutils
#import filemgmt.filemgmt_defs as fmdefs
import filemgmt.disk_utils_local as disk_utils_local

class ArchiveTransferLocal():
    """
    """
    @staticmethod
    def requested_config_vals():
        return {}    # no extra values needed 

    # assumes home and target are on same machine

    def __init__ (self, src_archive_info, dst_archive_info, archive_transfer_info, config=None):
        self.src_archive_info = src_archive_info 
        self.dst_archive_info = dst_archive_info 
        self.archive_transfer_info = archive_transfer_info 
        self.config = config


    def blocking_transfer(self, filelist):
        miscutils.fwdebug_print("\tNumber files to transfer: %d" % len(filelist))
        if miscutils.fwdebug_check(1, "ARCHIVETRANSFER_DEBUG"):
            miscutils.fwdebug_print("\tfilelist: %s" % filelist)

        srcroot = self.src_archive_info['root']
        dstroot = self.dst_archive_info['root']
        
        files2copy = copy.deepcopy(filelist)
        problems = {}
        srctranslation = {}
        for fname, finfo in files2copy.items():
            finfo['src'] = '%s/%s' % (srcroot, finfo['src'])
            finfo['dst'] = '%s/%s' % (dstroot, finfo['dst'])

        transresults = disk_utils_local.copyfiles(files2copy)

        return transresults 
