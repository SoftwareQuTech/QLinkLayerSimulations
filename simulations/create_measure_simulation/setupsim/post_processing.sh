#!/bin/bash
#SBATCH -p short
#SBATCH -t 1:00:00

RESULT_DIR=$1

# Run post processing
python3 "${SIMULATION_DIR}/../analysis_sql_data.py" --results-path $RESULT_DIR --no-plot
