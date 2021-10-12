#!/bin/bash

######################################################################

bold=$(tput bold)
normal=$(tput sgr0)

if [ -z "$1" ]; then
    echo "EMPTY FLAG!!! Use ${bold}./manticore.sh'${normal} with one of the flags: ${bold}-f${normal} (for fast preprocessing using all kernels) or ${bold}-s${normal} (for slow preprocessing using one kernel)"
elif [ $1 == -f ]; then
#    max_number_of_processes_is_possible_in_system="$(ulimit -n)"
#    echo "ulimit -n 8192"
    python3 manticore_main_fast.py 1> >(tee .manticore_stdout_fast.log ) 2> >(tee .manticore_stderr_fast.log)
#    echo "ulimit -n $max_number_of_processes_is_possible_in_system"
elif [ $1 == -s ]; then
    python3 manticore_main_slow.py 1> >(tee .manticore_stdout_slow.log ) 2> >(tee .manticore_stderr_slow.log)
else
    echo "WRONG FLAG!!! Use ${bold}./manticore.sh'${normal} with one of the flags: ${bold}-f${normal} (for fast preprocessing using all kernels) or ${bold}-s${normal} (for slow preprocessing using one kernel)"
fi

######################################################################