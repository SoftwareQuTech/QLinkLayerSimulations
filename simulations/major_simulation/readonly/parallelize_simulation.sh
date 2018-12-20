#!/bin/bash
# This is a script sets up things for running in parallel in cluster
#
# Author: Axel Dahlberg

##################
# Read arguments #
##################

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

#logfiledestination

case $key in
    -rd|--resultsdir)
    resultsdir="$2"
    shift
    shift
    ;;
    -sd|--simulation_dir)
    SIMULATION_DIR="$2"
    shift
    shift
    ;;
    -ts|--timestamp)
    timestamp="$2"
    shift
    shift
    ;;
	-lf|--logfiledestination)
	logfiledestination="$2"
	shift
	shift
	;;
	-ol|--outputlogfile)
	OUTPUTLOGFILE="$2"
	shift
	shift
	;;
	-lc|--logtoconsole)
	LOGTOCONSOLE="$2"
	shift
	shift
	;;
    -pr|--profiling)
	PROFILING="$2"
	shift
	shift
	;;
	-pp|--postprocessing)
	POST_PROC="$2"
	shift
	shift
	;;
    -p)
    PARTITION="$2"
    shift
    shift
    ;;
	*)
	echo "Unknown argument $key"
	exit 1
esac
done

OUTPUTLOGFILE=${OUTPUTLOGFILE:-'y'}
LOGTOCONSOLE=${LOGTOCONSOLE:-'y'}
PROFILING=${PROFILING:-'n'}
POST_PROC=${POST_PROC:-'n'}

# relevant files
runsimulation="$SIMULATION_DIR"/setupsim/perform_single_simulation_run.py
simdetailsfile="$resultsdir"/simdetails.ini
paramcombinationsfile="$resultsdir"/paramcombinations.json
paramsetfile="$resultsdir"/paramset.csv
archivejobfile="$SIMULATION_DIR"/readonly/archivejob.sh

# Get simulation details
# TODO Check that requiered arguments are set
. "$simdetailsfile"

# load modules
module load stopos
module load python/3.6-intel-2018-u2

# Setup stopos data base
module load stopos
stopos_pool=pool_simulation_${SLURM_JOB_ID}
echo "creating stopos pool ${stopos_pool}"
stopos create -p $stopos_pool
echo "adding file $paramsetfile to stopos"
stopos add -p $stopos_pool $paramsetfile

# Make a temporary folder
TMP_DIR=`mktemp -d`
TMP_DIR=$(readlink -f $TMP_DIR)

# Schedule moving results files after simulation (and possibly post-processing

sbatch --out="${resultsdir}/archiving_log.out" --dependency=afterany:${SLURM_JOB_ID} $archivejobfile $SIMULATION_DIR ${SLURM_JOB_ID} $TMP_DIR $resultsdir $POST_PROC $timestamp $OUTPUTDIRNAME $paramsetfile $PARTITION


srun sh $SIMULATION_DIR/readonly/run_simulation.sh -rd $resultsdir -td $TMP_DIR -sd $SIMULATION_DIR -ts $timestamp -lf $logfiledestination -ol $OUTPUTLOGFILE -lc $LOGTOCONSOLE -pr $PROFILING -rc y -pp $POST_PROC
