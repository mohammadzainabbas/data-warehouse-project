#!/bin/bash

set -e -u -o pipefail

say() {
    echo $1
}

log() {
    say "[[ log ]] $1"
}

error() {
    say "[[ error ]] $1" 
}

line_separator() {
    say "\n========================================\n"
}

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
benchmark_scales=(1 5 10 25 50)

log "Starting Benchmarking Service"

for i in "${benchmark_scales[@]}"; do benchmark "$i"; done

log "All done !!"
