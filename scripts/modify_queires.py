from os import getcwd, listdir, makedirs
from os.path import join, isfile, exists
from argparse import ArgumentParser
from sys import argv
import re

def fatal_error(text):
    print("\nError: {}\n".format(text))
    exit(0)

def needs_to_split(query):

    # checks whether to split the query or not
    split_regex = re.compile(r"(;)\s*(with)", re.S)
    return len(split_regex.findall(query))

def modify_line(line):

    # +/- <Digit> days) -> INTERVAL <Digit> day)
    interval_regex = re.compile(r"(\+|\-)\s*(?P<num>\d+)\s*(?P<day>days\))", re.S)
    line = interval_regex.sub(lambda m: m.group().replace(m.group("num"), "INTERVAL {}".format(m.group("num")), 1).replace(m.group("day"), "day)", 1), line)
    
    # as "<some text>" -> as some_text
    as_regex = re.compile(r"(as)\s*(?P<p1>\")\s*(?P<text>[^]]+)\s*(?P<p2>\")", re.S)
    for m in re.finditer(as_regex, line): # not a good approach but it works
        x, y = m.span()
        line = "{}{}{}".format(line[0:x], line[x:y].replace("-", "_"), line[y:len(line)-1])
    line = as_regex.sub(lambda m: m.group().replace(m.group("text"), "{}".format("_".join(m.group("text").split(" "))), 1).replace(m.group("p1"), "", 1).replace(m.group("p2"), "", 1), line)

    return line

def modify_query(src_file, dest_file):

    def save_file(dest_file, text):
        with open(dest_file, "w") as f:
            f.write(text)

    if needs_to_split(src_file):


    with open(src_file, "rt") as file:
        lines = file.readlines()
        query = ""
            
        for index, line in enumerate(lines):
            text = line.strip()
            if not len(text) and index == 0: continue
            query += "\n{}".format(modify_line(text))
        
        save_file(dest_file=dest_file, text=query)

def main(queries_dir, save_dir, debug=False):
    sql_queries = [x for x in listdir(queries_dir) if isfile(join(queries_dir, x)) and x.endswith(".sql")]

    if not len(sql_queries): fatal_error("No .sql file in '{}'".format(queries_dir))
    if not exists(save_dir): makedirs(save_dir)
    
    for query in sql_queries:
        src_file = join(queries_dir, query)
        dest_file = join(save_dir, query)
        modify_query(src_file=src_file, dest_file=dest_file)

        if debug:
            with open(join(getcwd(), 'compare_files.sh'), 'a') as f:
                f.write("code -d {} {}\n".format( src_file, dest_file ))

if __name__ == "__main__":
    basename = argv[0] if len(argv) else "modified_queries.py"
    parser = ArgumentParser("{} modifies your TPC-DS queries for Spark SQL".format(basename))
    parser.add_argument("-queries_dir", help="Directory for queries to be modified.", type=str)
    parser.add_argument("-save_dir", help="Directory for modified queries to be saved.", type=str, default="modified_queries")
    parser.add_argument("--debug", help="Produce a comparsion script to show the modification(s) done to each file.", action="store_true")
    args = parser.parse_args()

    queries_dir = args.queries_dir
    save_dir = args.save_dir
    debug = args.debug

    # If no queries_dir is specified
    if not queries_dir or not len(queries_dir):
        fatal_error("No 'queries_dir' specified. Run '{} -h' for some help.".format(basename))
    # If queries_dir is invalid
    elif not exists(queries_dir):
        fatal_error("Invalid 'queries_dir' provided. '{}' is not a directory. Run '{} -h' for some help.".format(queries_dir, basename))
    
    main(queries_dir=queries_dir, save_dir=save_dir, debug=debug)
