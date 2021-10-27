#!/usr/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 12th Oct, 2021
#====================================================================================
# This script is used to generate queires for TPC-DS benchmarking.
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

log () {
    echo "[[ log ]] $1"
}

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

    $ $progname -s 5 -d oracle -p tpcds-kit
    ⚐ → Generates queries for 5 Gb scale using 'oracle' as dialect and by running 'tpcds-kit/tools/dsqgen' binary.

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

# If path directory specified doesn't exist
if [ ! -d $path ]; then
    printf "\nError: Directory '$path' not found.\n"
    exit 1
fi

# If path directory specified doesn't exist
if [ ! -d $path/tools ]; then
    printf "\nError: Directory '$path/tools' not found.\n"
    exit 1
fi

# @todo: Check if 'dsqgen' binary exists

parent_dir="$(basename $(pwd))"
output_dir="queries_${scale}gb"
output_path="$parent_dir/$output_dir"

# If output_dir directory doesn't exist
if [ ! -d $output_dir ]; then
    echo "Directory '$output_dir' not found. Creating $output_dir now"
    mkdir -p $output_dir
fi

# Delete everything in the output directory
rm -rf $output_dir/*

# template file do we have
template_files=$path/query_templates/templates.lst

# If $path/query_templates/templates.lst file doesn't exist
if [ ! -f $template_files ]; then
    echo "Error: File '$template_files' not found."
    exit 1
fi

for template in `less $template_files`
do
    cd $path/tools > /dev/null
    ./dsqgen -directory ../query_templates -template $template -scale $scale -dialect $dialect -output_dir ../../$output_path > /dev/null 2>> ${progname}_error.log
    cd - > /dev/null
    query_no=$(echo $template | sed 's/[a-z]*//g' | sed 's/\.//g')
    mv $output_dir/query_0.sql $output_dir/query_$query_no.sql
    log "⚐ → query_$query_no.sql done."
done

# #Sub-path for alphas (in your sampled directory)
# path_sufix=term_samples/alphas
# #Current path for your alpha
# path_alpha=$(pwd)
# #Sampled directory (which was created)
# directory=0
# #First Alpha file for comparison
# alpha_1=0
# #Second Alpha file for comparison
# alpha_2=0
# #Check to show header file
# header=0
# #Check to show source file
# code=0


# echo "Bash version ${BASH_VERSION}..."


# cd bm-tpcds

# set -e  # Exit script on error

# for i in {1..99}
# do
#     $path/dsqgen -directory query_templates -template query$i.tpl -VERBOSE Y -SCALE $DATA_SCALE -DIALECT netezza -OUTPUT_DIR $OUTPUT_DIR/
#     mv "$OUTPUT_DIR/query_0.sql" "$OUTPUT_DIR/query$i.sql"
#     echo "Done Query: $i"
# done

