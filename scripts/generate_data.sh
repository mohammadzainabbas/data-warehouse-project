#!/usr/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 12th Oct, 2021
#====================================================================================
# This script is used to generate data for TPC-DS benchmarking.
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

# import helper functions from 'utils.sh'
source utils.sh

#Function that shows usage for this script
function usage()
{
cat << HEREDOC

Generate data for TPC-DS benchmark (via 'dsdgen' binary)

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -s, --scale             Scale in Gb. (required)
    -p, --path              Path for tools 'dsdgen' binary. (by default consider 'dsdgen' binary in your current directory)
    -h, --help              Show usage

Examples:

    $ $progname -s 1
    ⚐ → Generates data for 1 Gb scale.

    $ $progname -s 5 -p ../tpcds-kit/
    ⚐ → Generates data for 5 Gb scale by running '../tpcds-kit/tools/dsdgen' binary.

HEREDOC
}

# Clear the screen
clear

#Get program name
progname=$(basename $0)

#Path for 'tpcds-kit'
path=$(pwd)

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

# some sanity checks
check_dir $path
check_dir $path/tools
check_bin $path/dsdgen

parent_dir="$(basename $(pwd))"
output_dir="data_${scale}gb"
output_path="$parent_dir/$output_dir"

# If output_dir directory doesn't exist
create_dir_if_not_exists $output_dir

# Delete everything in the output directory
rm -rf $output_dir/* || error "Something went wrong while deleting"
cd $path/tools > /dev/null

start=$(date +%s)

./dsdgen -scale $scale -dir ../../$output_path -suffix ".csv" -delimiter "|" > /dev/null 2>> ${progname}_error.log || fatal_error "Unable to generate data via '$progname' for '$scale' Gb ..."

end=$(date +%s)
time_took=$((end-start))

cd - > /dev/null
total_queries=$(ls $output_dir | wc -l)
log "⚐ → Generated data containing $total_queries '.csv' files for $scale Gb"
log "⚑ Data Generation time for $scale Gb → $time_took seconds..."
