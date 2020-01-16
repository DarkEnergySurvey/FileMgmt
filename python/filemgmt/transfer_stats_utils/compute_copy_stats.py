#!/usr/bin/env python3

# OSG/FNAL User Support, December 2013

def Usage():
    sys.exit("""Summarize statistics from the output of the instrumented copyfiles()
   in eups/packages/Linux64/FileMgmt/python/filemgmt/http_utils.py

   Typically this is used like this:
     grep -nH -e 'Copy info:' -r ms4_finalcut_xxxx_fringe_20140223114418 | ./compute_copy_stats.py""")


import fileinput
import sys
import getopt
import re
import math


def summarize_copy_stats(fname):
    """>>> summarize_copy_stats('data_for_unit_tests/small_copy_statsB.txt')
(2, 2, 83917440, 4917, 1.0780897140503, {'D00229228_Y_c14_r189p02_detrend.fits': {'fromarchive': [1, 83917440]}, 'D00229228_Y_c14_r189p02_create-scamp-catalog.log': {'toarchive': [1, 4917]}})
"""
    num_copies = 0
    num_files = 0
    num_bytes_from = 0
    num_bytes_to = 0
    time_used = 0
    copy_stats = {}
    #  copy_stats[filename]['toarchive'] and
    #  copy_stats[filename]['fromarchive'] will be lists whose first
    #  element is the number of copies of filename done either to
    #  ('fromarchive') or from ('toarchive') the job, and whose second
    #  element is the total number of bytes spent copying that file in
    #  that direction.

    for line in fileinput.input(fname):
        F = line.split()
        num_copies = max(num_copies, int(F[2])+1)
        num_files += 1
        if F[7] == 'fromarchive':
            num_bytes_from += int(F[4])
        elif F[7] == 'toarchive':
            num_bytes_to += int(F[4])
        else:
            raise Exception, "Field 5 of 'Copy info:' must be either toarchive or fromarchive."
        time_used += float(F[5])
        fname = F[3]
        x = []
        if not copy_stats.has_key(fname):
            copy_stats[fname] = {}
        if not copy_stats[fname].has_key(F[7]):
            copy_stats[fname][F[7]] = [0, 0]
        x = copy_stats[fname][F[7]]
        x[0] += 1
        x[1] += int(F[4])
    return (num_copies, num_files, num_bytes_from, num_bytes_to, time_used, copy_stats)

def record_repeated_copies(per_file_stats):
    """Return a dictionary of length-2 lists.
The length-2 list whose key is i corresponds to the files that were copied i times.
The first element in each length-2 list is how many bytes those files
need, the second element is the list of those file names.

>>> x = {'20130924_scamp_update_head.config': {'fromarchive': [1, 83923590]}, 'D00158966_r_c11_r131p01_scampcat.fits': {'toarchive': [1, 72000], 'fromarchive': [1, 72000]}}
>>> record_repeated_copies(x)
{1: [83923590, ['20130924_scamp_update_head.config']], 2: [144000, ['D00158966_r_c11_r131p01_scampcat.fits']]}
"""
    R = {}
    for fname in per_file_stats.keys():
        def safe_dereference(fname,direction,n):
            if per_file_stats[fname].has_key(direction):
                return per_file_stats[fname][direction][n]
            else:
                return 0
        def sum_stat(n): return safe_dereference(fname,'fromarchive',n) + safe_dereference(fname,'toarchive',n)
        times_copied = sum_stat(0)
        bytes_copied = sum_stat(1)
        if not R.has_key(times_copied):
            R[times_copied] = [0, []]
        R[times_copied][0] += bytes_copied
        R[times_copied][1].append(fname)
    return R

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:],'thL')
    except getopt.GetoptError, x:
        sys.exit('Error: ' + x.msg)
    optsd = dict(opts)
    if optsd.has_key('-h'):
       Usage()
    if optsd.has_key('-t'):
        import doctest
        doctest.testmod()   # verbose=True
        sys.exit()
    cs = summarize_copy_stats('-')
    R = record_repeated_copies(cs[5])
    if not optsd.has_key('-L'):
        print "times_copied   total_bytes_copied  num_files"
        for times_copied in R.keys():
            print "%d  %d  %s" % (times_copied, R[times_copied][0], len(R[times_copied][1]))
        print
        print "num_copies num_files bytes_from_archive  bytes_to_archive time_used(s)"
        print " ".join(map(str,cs[0:5]))
    else:
        for times_copied in R.keys():
            print "times_copied:%d" % times_copied
            print "total_bytes_copied:%d" % R[times_copied][0]
            print "files_copied:\n    " + "\n    ".join(R[times_copied][1])
