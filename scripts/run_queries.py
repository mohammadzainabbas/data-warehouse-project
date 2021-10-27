from os import getcwd, listdir, makedirs
from os.path import join, isfile, abspath, pardir, exists
from time import time
from shutil import copyfile
from sys import stdout, stderr, __stdout__, __stderr__
from contextlib import redirect_stdout, redirect_stderr
from pickle import load
from argparse import ArgumentParser
from sys import argv, stdout, stderr, __stdout__, __stderr__
import pandas as pd
import re
from pyspark.sql import SparkSession

#spark-submit --jars ~/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar scripts/run_queries.py

#------------------------
# Helper functions
#------------------------
def print_log(log_file, *args, **kwargs):
    # redirect stdout to a file
    with open(log_file, 'a') as f:
        with redirect_stdout(f):
            print(*args, **kwargs)

def print_error(error_file, *args, **kwargs):
    # redirect stderr to a file
    with open(error_file, 'a') as f:
        with redirect_stderr(f):
            print(*args, **kwargs)

def fatal_error(text):
    print("\nError: {}\n".format(text))
    exit(0)

def create_if_not_exist(dir):
    if not exists(dir): makedirs(dir)
    return dir
#------------------------
#------------------------
def load_schema(schema_file):
    '''
    Loads the schema and return the schema dict
    '''
    schema_dict = {}
    with open(schema_file, 'rb') as pickle_file:
        schema_dict = load(pickle_file)
    return schema_dict

def load_data(spark, schema_dict, data_dir):
    '''
    Load the data and create a temp view for each data 
    '''
    # check if data_dir has .csv files -> if not fatal error
    if not exists(data_dir): fatal_error("Data directory '{}' not found".format(data_dir))

    # get all *.csv files in data_dir
    data_files = [str(x).replace(".csv", "") for x in listdir(data_dir) if x.endswith(".csv") and isfile(join(data_dir, x))]
    if not len(data_files): fatal_error("No .csv file found in data directory '{}'".format(data_dir))
    
    # get common tables names b/w *.csv and schema
    tables = list(set(data_files).intersection(set(list(schema_dict.keys()))))
    if not len(tables): fatal_error("No schema was found for any .csv file in data directory '{}'".format(data_dir))

    for table in tables:
        df = spark.read.load( join(data_dir, "{}.csv".format( table )), format="csv", delimiter="|", header="true", schema=schema_dict[table])
        df.createOrReplaceTempView("{}".format( table ))

def run_benchmark(spark, queries_dir, result_dir, log_file, error_file):
    '''
    Runs the benchmark and return the timings df
    '''
    #--------- Configuration(s) -------------
    take_avg_run, skip_run = 5, 1
    total_runs = take_avg_run + skip_run
    queries = [x for x in listdir(queries_dir) if str(x).endswith(".sql") and isfile(join(queries_dir, x))]

    digit_regex = re.compile(r"(?P<num>\d+)")
    timings_dict = {}

    log_message = lambda x: print_log(log_file, x)
    log_error = lambda x: print_error(error_file, x)
    #----------------------------------------
    def get_execute_query_time(file, is_first=False):
        with open(file, "rt") as f:
            try:
                if is_first:
                    start_time = time()
                    result = spark.sql(f.read())
                    return time() - start_time, result
                else:
                    start_time = time()
                    spark.sql(f.read())
                    return time() - start_time, None
            except Exception as e:
                log_error("[[ error ]]: Failed to execute '{}' query due to:\n{}".format( file, e ))
                return None, None
    
    for index, query in enumerate(queries):
        log_message("="*50)
        log_message("{} - Running benchmark for query: {}".format(index + 1, query))

        timings = list()
        query_file = join(queries_dir, query)
        query_no = int(digit_regex.findall(query)[0]) if len(digit_regex.findall(query)) else ""

        for run in list(range(0, total_runs)):
            is_first = run == 0
            executed_time, result = get_execute_query_time(query_file, is_first=is_first)
            if executed_time is None and result is None: continue # in case of some error
            elif result is not None: result.toPandas().to_csv(join(result_dir, "result_{}.csv".format(query_no)), sep="|") # save the result after 1st run
            else: # add to benchmark's timings
                timings.append(executed_time)
                log_message("{} run took {} secs".format(run, executed_time))
        timings_dict[query] = timings
    return timings_dict

def save_benchmark(benchmark_dict, benchmark_file):
    '''
    Save the benchmark result as 'benchmark_timings_1gb.csv'
    '''
    benchmark_df = pd.DataFrame(benchmark_dict)
    benchmark_df.to_csv(benchmark_file)

def main(scale):
    
    if not isinstance(scale, int): fatal_error("'scale' should be an int.")
#=======
    #------------------------
    # Configuration(s)
    #------------------------
    # tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "dbgen_version", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
    # data_dir = join(abspath(join(getcwd(), pardir)), "tpcds-kit", "data_correct") # ../tpcds-kit/data_correct
    project_dir = join(getcwd())
    data_dir = join(project_dir, "data_{}gb".format(scale)) # data_1gb -> path for data for e.g: data_1gb/*.csv 
    result_dir = join(create_if_not_exist(join(project_dir, "results_dir", "results_{}gb".format(scale)))) # results_dir/results_1gb
    queries_dir = join(project_dir, "queries_{}gb".format(scale)) # queries_1gb

    log_file = join(create_if_not_exist(join(project_dir, "tpcds_logs")), "tpcds_{}gb.log".format(scale)) # tpcds_logs/tpcds_1gb.log
    error_file = join(create_if_not_exist(join(project_dir, "tpcds_errors")), "tpcds_{}gb.err".format(scale)) # tpcds_errors/tpcds_1gb.err
    schema_file = join(create_if_not_exist(join(project_dir, "schema")), "tpcds_schema.pickle")
    benchmark_file = join(create_if_not_exist(join(project_dir, "benchmark")), "benchmark_timings_{}gb.csv".format(scale))
#=======

    spark = SparkSession.builder.master("local[1]").appName("TPC DS").enableHiveSupport().getOrCreate()
    schema_dict = load_schema(schema_file)
    print("Schema Dict: {}".format(schema_dict.keys()))
    load_data(spark, schema_dict, data_dir)
    benchmark_dict = run_benchmark(spark, queries_dir, result_dir, log_file, error_file)
    save_benchmark(benchmark_dict, benchmark_file)

    spark.stop()

if __name__ == "__main__":
    basename = argv[0] if len(argv) else "run_queries.py"
    parser = ArgumentParser("{} runs your TPC-DS queries for Spark SQL".format(basename))
    parser.add_argument("-scale", help="Scale factor for your tpc queries.", type=int)
    args = parser.parse_args()
    main(scale=args.scale)

#--------------------------------------------------------------------
'''

- 1. Generate the data for scale <s> 
- 2. Generate the queries for scale <s>
- 3. Benchmark for scale <s> by running queries on data


python run_queries.py -data_dir <path to data dir> -queries_dir <path to queries dir>

python run_queries.py -data_dir data_5gb -queries_dir sql_queries_10gb

python run_queries.py -scale 1

- load the data
- run queries for that data
- save the benchmarking stuff (or )
- saving a csv file with timings.

python run_queries.py -s 1

-> pwd/queries_1gb - Queries directory for your 1 gb  
-> pwd/data_1gb - Data directory for your <scale> gb

-> pwd/queries_<scale>gb - Queries directory for your <scale> gb  
-> pwd/data_<scale>gb - Data directory for your <scale> gb

'''
#--------------------------------------------------------------------

