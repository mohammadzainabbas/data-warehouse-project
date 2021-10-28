#!/usr/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 12th Oct, 2021
#====================================================================================
# This script is used to generate queires for TPC-DS benchmarking.
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

# import helper functions from 'scripts/utils.sh'
source $(dirname $0)/utils.sh

#Function that shows usage for this script
function usage()
{
cat << HEREDOC

Generate queries for TPC-DS benchmark (via 'dsqgen' binary)

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -s, --scale             Scale in Gb. (required)
    -p, --path              Path for tools 'dsqgen' binary. (by default consider 'dsqgen' binary in your current directory)
    -d, --dialect           Dialect to use. (by default uses 'netezza')
    -h, --help              Show usage

Examples:

    $ $progname -s 1
    ⚐ → Generates queries for 1 Gb scale.

    $ $progname -s 5 -d oracle -p ../tpcds-kit/
    ⚐ → Generates queries for 5 Gb scale using 'oracle' as dialect and by running '../tpcds-kit/tools/dsqgen' binary.

HEREDOC
}

# Clear the screen
clear

#Get program name
progname=$(basename $0)

#Path for 'tpcds-kit'
path=$(pwd)

#Dialect for 'tpcds-kit'
dialect='netezza'

#Get all the arguments and update accordingly
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -s|--scale) scale="$2"; shift ;;
        -p|--path) path="$2"; shift ;;
        -d|--dialect) dialect="$2"; shift ;;
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
check_bin $path/dsqgen

parent_dir="$(basename $(pwd))"
output_dir="queries_${scale}gb"
output_path="$parent_dir/$output_dir"

# If output_dir directory doesn't exist
create_dir_if_not_exists $output_dir

# Delete everything in the output directory
rm -rf $output_dir/*

# template file do we have
template_files=$path/query_templates/templates.lst

# check if template file exists
check_file $template_files

log "Generating queries for scale $scale Gb ..."

start=$(date +%s)

for template in `less $template_files`
do
    cd $path/tools > /dev/null
    ./dsqgen -directory ../query_templates -template $template -scale $scale -dialect $dialect -output_dir ../../$output_path > /dev/null 2>> ${progname}_error_${scale}gb.log
    cd - > /dev/null
    query_no=$(echo $template | sed 's/[a-z]*//g' | sed 's/\.//g')
    mv $output_dir/query_0.sql $output_dir/query_$query_no.sql || error "Unable to rename '$output_dir/query_0.sql' file to '$output_dir/query_$query_no.sql' for scale $scale Gb ..."
    log "⚐ → query_$query_no.sql done."
done

end=$(date +%s)
time_took=$((end-start))
log "⚑ Queries generation time for $scale Gb → $time_took seconds ..."
