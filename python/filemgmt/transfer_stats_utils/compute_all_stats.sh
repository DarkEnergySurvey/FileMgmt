#!/bin/bash

# OSG/FNAL User Support, December 2013

# Usage example:
#     ./compute_all_stats.sh ms4_finalcut_xxxx_fringe_20131210143120
#
# This prints all of the statistics for a particular run
# in a form that is easy to cut-and-paste into the spreadsheet.

cd $1/runtime
JL=$(../../compute_job_length.py -w  uberctrl/D00229228_r*p*_mainmngr.dag.dagman.log  $(find . -name runjob.log) | tail -n 1)
echo "num_jobs total_CPU_time(s) total_CPU_time(h) wall_time(s) wall_time(h)"
# echo $JL
# exit 0
readarray CS < <(grep -nH -e 'Copy info:' -r . | ../../compute_copy_stats.py)

echo "num_jobs total_CPU_time(s) total_CPU_time(h) wall_time(s) wall_time(h)  num_copies num_files num_bytes_from num_bytes_to time_used(s)  directory"
last_line=$(echo "${CS[${#CS[@]}-1]}" | tr '\n' ' ')
echo "$JL  $last_line  $1"
echo
unset CS[${#CS[@]}-1]
unset CS[${#CS[@]}-1]
echo "${CS[@]}"
