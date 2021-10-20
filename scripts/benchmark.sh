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

    log "Changing directory"
    cd ~/Masters/ULB/Data\ Warehouse/tpcds-kit/tools


    cd - > /dev/null
}

#--------------------
# Configurations
#--------------------
benchmark_scales=(1 5 10 25 50)

log "Starting Benchmarking Service"

for i in "${benchmark_scales[@]}"; do benchmark "$i"; done

log "All done !!"
