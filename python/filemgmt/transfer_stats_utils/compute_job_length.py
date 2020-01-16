#!/usr/bin/env python3

# OSG/FNAL User Support, December 2013

import re
import sys
import getopt
import fileinput

def Usage():
    sys.exit("""   compute_job_length.py -t
   compute_job_length.py -h
   compute_job_length.py [-w  user_log_for_entire_dag ]  [list of other condor user logs to process]

      Compute the total time that the jobs in the listed user logs
      ran. If -w is supplied then the wall time of the whole
      run is computed.

Example of use:
    cd ms4_finalcut_xxxx_fringe_20131209153610/runtime/
    ../../compute_job_length.py -w  uberctrl/D00158966_r90p01_mainmngr.dag.dagman.log  $(find . -name runjob.log)
 """)

def compute_seconds(m):
    return int(m.group(2))*3600 + int(m.group(3))*60 + int(m.group(4))

# Handle the case where the start and end times are on different days.
def safer_time_difference(x,y):
    assert 0 <= x < 3600*24
    assert 0 <= y < 3600*24
    return (y - x if y > x else (y + (3600*24 - x)))

def job_lengths_from_file(fname):
   """>>> job_lengths_from_file('data_for_unit_tests/runjob.log')
(1, 205)
>>> job_lengths_from_file('data_for_unit_tests/runjob_multiple.log')
(2, 55)
"""
   start_times = {}
   total_time = 0
   num_jobs = 0
   time = '^.*\((.*)\) \d\d/\d\d (\d\d):(\d\d):(\d\d) '
   for line in fileinput.input(fname):
       m = re.match(time+'Job executing on host:',line)
       if m:
           if start_times.has_key(m.group(1)):
               print "Warning: start value for %s already seen." % m.group(1)
           start_times[m.group(1)] = compute_seconds(m)
       m = re.match(time+'Job terminated.',line)
       if m:
           if not start_times.has_key(m.group(1)):
               print "Warning: no start value for %s, skipping job termination." % m.group(1)
               continue
           endtime = compute_seconds(m)
           total_time += safer_time_difference(start_times[m.group(1)], endtime)
           num_jobs += 1
   return (num_jobs,total_time)

def multiple_job_lengths(flist):
    N = 0
    sum = 0
    for F in flist:
        (num_jobs,T) = job_lengths_from_file(F)
        assert T >= 0
        sum += T
        N += num_jobs
    return (N,sum)

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:],'htw:')
    except getopt.GetoptError, x:
        sys.exit('Error: ' + x.msg)
    optsd = dict(opts)
    if optsd.has_key('-h') or len(sys.argv) == 1:
       Usage()
    if optsd.has_key('-t'):
        import doctest
        doctest.testmod()
        sys.exit()
    (N,sum) = multiple_job_lengths(args)
    print "num_jobs total_CPU_time(s) total_CPU_time(h)",
    if optsd.has_key('-w'):
        (num_jobs,T) = job_lengths_from_file(optsd['-w'])
        assert num_jobs == 1
        print " wall_time(s) wall_time(h)"
        print " ".join(map(str, (N, sum, sum/3600.0, T, T/3600.0)))
    else:
        print
        print " ".join(map(str, (N, sum, sum/3600.0)))
