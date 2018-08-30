#!/bin/bash
#SBATCH -p staging
#SBATCH -t 1-00:00:00
#SBATCH -J archiving

# NOTES
# https://userinfo.surfsara.nl/systems/cartesius/usage/batch-usage#heading16

# change directory to the temporary directory of the computation job
JOB_ID=$1
TMP_DIR=$2
JOB_DIR=$3

# Concatenate output CSV files and move to project space
mkdir -p $JOB_DIR

mv $TMP_DIR/* $JOB_DIR

# Move the slurm file
mv "${SIMULATION_DIR}/slurm-${JOB_ID}.out" $JOB_DIR

# Cleanup
rmdir $TMP_DIR
