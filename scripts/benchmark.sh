#!/bin/bash

set -e -u

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
    line_separator

    log "Scale factor: $1 Gb"
}

#--------------------
# Configurations
#--------------------
benchmark_scales=(1 5 10 25 50)

log "Starting Benchmarking Service"

for i in "${benchmark_scales[@]}"; do benchmark "$i"; done

log "All done !!"
