#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 28th Oct, 2021
#====================================================================================
# This file contains helper methods. If you want to use them in your script, simply add '. utils.sh' or 'source utils.sh'
#====================================================================================

log () {
    echo "[[ log ]] $1"
}

error () {
    echo "[[ error ]] $1"
}

fatal_error () {
    error "$1"
    exit 1
}

check_dir() {
    if [ ! -d $1 ]; then
        fatal_error "Directory '$1' not found."
    fi
}

line_separator() {
    echo "\n========================================\n"
}

