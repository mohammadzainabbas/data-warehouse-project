#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 28th Oct, 2021
#====================================================================================
#  This script is used to benchmark via TPC-DS for all scales.
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

# import helper functions from 'scripts/utils.sh'
source $(dirname $0)/utils.sh

# scale factors to be used
benchmark_scales=(1 2 5 10)

for i in "${benchmark_scales[@]}"; do sh benchmark.sh -s "$i"; done
