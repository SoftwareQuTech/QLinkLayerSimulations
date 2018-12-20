#!/bin/bash
#SBATCH -p staging
#SBATCH -t 1-00:00:00
#SBATCH -J archiving

# NOTES
# https://userinfo.surfsara.nl/systems/cartesius/usage/batch-usage#heading16

# change directory to the temporary directory of the computation job
SIMULATION_DIR=$1
JOB_ID=$2
TMP_DIR=$3
RESULT_DIR=$4
POST_PROC=$5
timestamp=$6
OUTPUTDIRNAME=$7
paramsetfile=$8
PARTITION=$9

# Concatenate output CSV files and move to project space
mkdir -p $RESULT_DIR

mv $TMP_DIR/* $RESULT_DIR

# Move the slurm file
mv "${SIMULATION_DIR}/slurm-${JOB_ID}"*".out" $RESULT_DIR


# Cleanup
rmdir $TMP_DIR

# Run post-processing
if [ "$POST_PROC" == "y" ]; then
    post_proc_file="${SIMULATION_DIR}/readonly/post_processing.sh"

    if [ "$PARTITION" == "short" ]; then
        MAXTIME=1:00:00
    else
        MAXTIME=24:00:00
    fi

    # Note that this script will only be called if we are on the cluster
    sbatch -p $PARTITION -t $MAXTIME --out="${RESULT_DIR}/post_processing_log.out" $post_proc_file $SIMULATION_DIR $RESULT_DIR $timestamp $paramsetfile y $OUTPUTDIRNAME
else
    # Move to folder to not include absolute
    cd $SIMULATION_DIR

    # Zip the results directory
    zip -r "${timestamp}_${OUTPUTDIRNAME}.zip" "${timestamp}_${OUTPUTDIRNAME}"
fi
