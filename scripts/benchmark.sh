#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 28th Oct, 2021
#====================================================================================
# This script is used to benchmark via TPC-DS using some scale.
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

log () {
    echo "[[ log ]] $1"
}

line_separator() {
    echo "\n========================================\n"
}

#Function that shows usage for this script
function usage()
{
cat << HEREDOC

Benchmark for a given scale using TPC-DS benchmark

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -s, --scale             Scale in Gb. (by default 1)
    -p, --path              Path for tpcds directory. (required)
    -h, --help              Show usage

Examples:

    $ $progname -s 1
    ⚐ → Benchmark for 1 Gb scale.

    $ $progname -s 5 -p ../tpcds-kit/
    ⚐ → Benchmark for 5 Gb scale with tpcds dir path as '../tpcds-kit'ß.

HEREDOC
}

scale=1

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
    local scale_factor=$1

    line_separator

    log "Scale factor: $scale_factor Gb"

    log "Generating data for $scale_factor"
    sh scripts/generate_data.sh -s $scale_factor -p ../tpcds-kit
    
    log "Generating queries for $scale_factor"
    sh scripts/generate_queries.sh -s $scale_factor -p ../tpcds-kit

    # @todo: make adjustment for modified queries' path -> save in same locations (and save the old versions somewhere else)

    log "Benchmarking for $scale_factor"
    spark-submit --jars ~/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar scripts/run_queries.py -scale $scale_factor
}

#--------------------
# Configurations
#--------------------

log "Starting Benchmarking Service"

for i in "${benchmark_scales[@]}"; do benchmark "$i"; done

log "All done !!"
