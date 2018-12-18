#!/bin/bash
#SBATCH -p short # requested parition (normal, short, staging, ...)
#SBATCH -t 1:00:00 # wall clock time

module load python/3.6-intel-2018-u2

python3 -m cProfile -o post_processing_prof.prof "${SIMULATION_DIR}/post_processing/analyse_single_key.py" "${SIMULATION_DIR}/$1"
