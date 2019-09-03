#!/usr/bin/env python

# $Id: fpackcompworker.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import time
import argparse
import traceback
import despymisc.miscutils as miscutils
import filemgmt.disk_utils_local as diskutils
from filemgmt.compworker import CompWorker


class FpackCompWorker(CompWorker):

    def __init__(self,cleanup=False,args=""):
        super(FpackCompWorker,self).__init__(cleanup,args)

    def get_exebase(self):
        return "fpack"

    def get_cleanup(self):
        return ["-D","-Y"]

    def get_extention(self):
        return ".fz"

    def get_exe_version_args(self):
        return ["-V"]

    def execute(self, file):
        newfilename = file + self.get_extention()
        try:
            diskutils.remove_file_if_exists(newfilename)
        except:
            self._errmsg = traceback.format_exc()
            return 1
        return super(FpackCompWorker,self).execute(file)

