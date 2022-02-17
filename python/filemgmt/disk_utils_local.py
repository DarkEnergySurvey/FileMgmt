# $Id: disk_utils_local.py 46644 2018-03-12 19:54:58Z friedel $
# $Rev:: 46644                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2018-03-12 14:54:58 #$:  # Date of last commit.

"""
Generic routines for performing tasks on files that can be seen locally
"""

__version__ = "$Rev: 46644 $"

import os
import sys
import shutil
import hashlib
import errno
import time
import copy

import despymisc.miscutils as miscutils


######################################################################
def get_md5sum_file(fullname, blksize=2**15):
    """ Returns md5 checksum for given file """

    md5 = hashlib.md5()
    with open(fullname, 'rb') as fhandle:
        for chunk in iter(lambda: fhandle.read(blksize), b''):
            md5.update(chunk)
    return md5.hexdigest()

######################################################################
def get_file_disk_info(arg):
    """ Returns information about files on disk from given list or path"""

    if isinstance(arg, list):
        return get_file_disk_info_list(arg)
    if isinstance(arg, str):
        return get_file_disk_info_path(arg)

    miscutils.fwdie(f"Error:  argument to get_file_disk_info isn't a list or a path ({type(arg)})", 1)

######################################################################
def get_single_file_disk_info(fname, save_md5sum=False, archive_root=None):
    """ Method to get disk info for a single file

    """
    if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
        miscutils.fwdebug_print(f"fname={fname}, save_md5sum={save_md5sum}, archive_root={archive_root}")

    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_COMPRESSION

    (path, filename, compress) = miscutils.parse_fullname(fname, parsemask)
    if miscutils.fwdebug_check(3, "DISK_UTILS_LOCAL_DEBUG"):
        miscutils.fwdebug_print(f"path={path}, filename={filename}, compress={compress}")

    fdict = {'filename' : filename,
             'compression': compress,
             'path': path,
             'filesize': os.path.getsize(fname)
             }

    if save_md5sum:
        fdict['md5sum'] = get_md5sum_file(fname)

    if archive_root and path.startswith('/'):
        fdict['relpath'] = path[len(archive_root)+1:]

        if compress is None:
            compext = ""
        else:
            compext = compress

        fdict['rel_filename'] = f"{fdict['relpath']}/{filename}{compext}"

    return fdict


######################################################################
def get_file_disk_info_list(filelist, save_md5sum=False):
    """ Returns information about files on disk from given list """

    fileinfo = {}
    for fname in filelist:
        if os.path.exists(fname):
            fileinfo[fname] = get_single_file_disk_info(fname, save_md5sum)
        else:
            fileinfo[fname] = {'err': "Could not find file"}

    return fileinfo



######################################################################
def get_file_disk_info_path(path, save_md5sum=False):
    """ Returns information about files on disk from given path """
    # if relative path, is treated relative to current directory

    if not os.path.exists(path):
        miscutils.fwdie(f"Error:  path does not exist ({path})", 1)

    fileinfo = {}
    for (dirpath, _, filenames) in os.walk(path):
        for name in filenames:
            fname = os.path.join(dirpath, name)
            fileinfo[fname] = get_single_file_disk_info(fname, save_md5sum)

    return fileinfo

######################################################################
def copyfiles(filelist, tstats, verify=False):
    """ Copies files in given src,dst in filelist """

    status = 0
    for filename, fdict in filelist.items():
        fsize = 0
        try:
            src = fdict['src']
            dst = fdict['dst']

            if 'filesize' in fdict:
                fsize = fdict['filesize']
            elif os.path.exists(src):
                fsize = os.path.getsize(src)

            if not os.path.exists(dst):
                if tstats is not None:
                    tstats.stat_beg_file(filename)
                path = os.path.dirname(dst)
                if path and not os.path.exists(path):
                    miscutils.coremakedirs(path)
                shutil.copy(src, dst)
                if tstats is not None:
                    tstats.stat_end_file(0, fsize)
                if verify:
                    newfsize = os.path.getsize(dst)
                    if newfsize != fsize:
                        raise Exception(f"Incorrect files size for file {filename} ({newfsize:d} vs {fsize:d})")
        except Exception:
            status = 1
            if tstats is not None:
                tstats.stat_end_file(1, fsize)
            (_, value, _) = sys.exc_info()
            filelist[filename]['err'] = str(value)
    return (status, filelist)

######################################################################
def remove_file_if_exists(filename):
    """ Method to remove a single file if it exisits

    """
    try:
        os.remove(filename)
    except OSError as exc:
        if exc.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise
# end remove_file_if_exists

######################################################################

def del_files_from_disk(path):
    """ Delete files from disk """

    shutil.rmtree(path) #,ignore_errors=True)

def del_part_files_from_disk(files, archive_root):
    """ delete specific files from disk """
    good = []
    for key, value in files.items():
        try:
            os.remove(os.path.join(archive_root, value['path'], key))
            good.append(value['id'])
        except:
            pass
    return good
