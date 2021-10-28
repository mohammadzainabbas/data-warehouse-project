#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 28th Oct, 2021
#====================================================================================
# This script is used to benchmark via TPC-DS using some scale.
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

# import helper functions from 'utils.sh'
source $(dirname $0)/utils.sh

#Function that shows usage for this script
function usage()
{
cat << HEREDOC

Benchmark for a given scale using TPC-DS benchmark

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -s, --scale             Scale in Gb. (by default 1)
    -p, --path              Path for tpcds directory. (by default uses '../tpcds-kit')
    -h, --help              Show usage

Examples:

    $ $progname -s 1
    ⚐ → Benchmark for 1 Gb scale.

    $ $progname -s 5 -p ../tpcds-kit/
    ⚐ → Benchmark for 5 Gb scale with tpcds dir path as '../tpcds-kit'.

HEREDOC
}

progname=$(basename $0)
scale=1
path=../tpcds-kit

#Get all the arguments and update accordingly
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -s|--scale) scale="$2"; shift ;;
        -p|--path) path="$2"; shift ;;
        -h|--help)
        usage
        exit 1
        ;;
        *) printf "\n$progname: invalid option → '$1'\n\n⚐ Try '$progname -h' for more information\n\n"; exit 1 ;;
    esac
    shift
done


benchmark() {

    local scale=$1
    local path=$2
    local conda_env=pyspark_env # change this to your conda env

    # sanity checks
    check_dir $path
    check_dir $path/tools
    check_dir $path/query_templates

    line_separator

    log "Scale factor: $scale Gb"

    # 1. Generate data
    log "Generating data for $scale"
    sh scripts/generate_data.sh -s $scale -p $path

    # 2. Generate queries
    log "Generating queries for $scale"
    sh scripts/generate_queries.sh -s $scale -p $path

    # 3. Modify queries
    conda activate $conda_env || error "Unable to activate conda env '$conda_env' "
    log "Modifying queries for $scale"
    python scripts/modify_queires.py -queries_dir queries_${scale}gb -save_dir queries_${scale}gb
    
    # 4. Benchmark queries
    spark-submit scripts/run_queries.py -scale $scale &> spark_${scale}gb.log #  redirect stdout and stderr to the spark_<scale>gb.log

    # @todo: make adjustment for modified queries' path -> save in same locations (and save the old versions somewhere else)
}

log "Starting Benchmarking Service"

benchmark $scale $path

log "All done !!"
