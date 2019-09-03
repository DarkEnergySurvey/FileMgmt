#!/usr/bin/env python

# $Id: compworker.py 11430 2013-04-12 21:41:16Z tomashek $
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

__version__ = "$Rev: 11430 $"

import os
import sys
import time
import argparse
import subprocess
import despymisc.miscutils as miscutils
import traceback
from abc import ABCMeta, abstractmethod


class CompWorker:
    __metaclass__ = ABCMeta

    _passthroughargs = None
    _cleanup = None
    _dateFormat = '%Y-%m-%d %H:%M:%S'
    _retcode = None
    _errmsg = None

    def __init__(self,cleanup=False,args=""):
        self._passthroughargs = args.split()
        self._cleanup = cleanup

    @abstractmethod
    def get_cleanup(self):
        return None

    @abstractmethod
    def get_exebase(self):
        return None

    @abstractmethod
    def get_extention(self):
        return None

    @abstractmethod
    def get_exe_version_args(self):
        return None

    def get_errmsg(self):
        return self._errmsg

    def get_exe_version(self):
        cmdlist = filter(None,[self.get_exebase()] + self.get_exe_version_args())
        return (subprocess.check_output(cmdlist)).strip()

    def get_commandargs(self):
        return ' '.join(self.get_commandargs_list())

    def get_commandargs_list(self):
        if self._cleanup:
            return filter(None,self.get_cleanup() + self._passthroughargs)
        else:
            return filter(None,self._passthroughargs)

    def execute(self, file):
        cmdlist = [self.get_exebase()] + self.get_commandargs_list() + [file]
        try:
            self._errmsg = subprocess.check_output(cmdlist, stderr=subprocess.STDOUT,shell=False)
            self._retcode = 0
        except subprocess.CalledProcessError as e:
            self._retcode = e.returncode
            self._errmsg = e.output
        except:
            self._retcode = 1
            self._errmsg = traceback.format_exc()
        return self._retcode



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Calls a subprogram to compress a file')
    parser.add_argument('--file',action='store')
    parser.add_argument('--exeargs',action='store',default="")
    parser.add_argument('--cleanup',action='store_true',default=False)
    parser.add_argument('--class',action='store',default="filemgmt.fpackcompworker.FpackCompWorker")

    args, unknown_args = parser.parse_known_args()
    args = vars(args)

    if "file" not in args or "class" not in args:
        exit(1)

    compressor = miscutils.dynamically_load_class(args["class"])(args["cleanup"],args["exeargs"])
    print "full_commandline=" + compressor.get_exebase() + ' ' + compressor.get_commandargs()
    # compressor.execute(args["file"])
    print "version=" + compressor.get_exe_version()


