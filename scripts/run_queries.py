from os import getcwd, listdir, environ
from os.path import join, isfile, abspath, pardir
from time import time
from shutil import copyfile
from sys import stdout, stderr, __stdout__, __stderr__
from contextlib import redirect_stdout
from pickle import load
from argparse import ArgumentParser
from sys import argv
from pyspark.sql import SparkSession
import pandas as pd 

#spark-submit --jars ~/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar scripts/run_queries.py

#------------------------
# Helper functions
#------------------------
def log_message(*args, **kwargs):
    # redirect stdout to a file
    with open(kwargs.log_file, 'a') as f:
        with redirect_stdout(f):
            print(*args, **kwargs)

def fatal_error(text):
    print("\nError: {}\n".format(text))
    exit(0)
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
    # @todo: check if data_dir has .csv files -> if not fatal error

    df_dict = {}
    for table in list(schema_dict.keys()):
        df = spark.read.load( join(data_dir, "{}.csv".format( table )), format="csv", delimiter="|", header="true", schema=schema_dict[table])
        df_dict[table] = df
        df.createOrReplaceTempView("{}".format( table ))
    return df_dict

def run_benchmark(spark, df_dict):
    '''
    Runs the benchmark and return the timings df
    '''
    # @todo: Save the queries result in results_1gb/result_1.txt -> Query No. 1's result with 1 Gb scale.

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
    result_dir = join(project_dir, "results_dir", "results_{}gb".format(scale)) # results_dir/results_1gb
    queries_dir = join(project_dir, "queries_{}gb".format(scale)) # queries_1gb

    log_file = join(project_dir, "tpcds_logs", "tpcds_{}gb.log".format(scale)) # tpcds_logs/tpcds_1gb.log
    error_file = join(project_dir, "tpcds_errors", "tpcds_{}gb.err".format(scale)) # tpcds_errors/tpcds_1gb.err
    schema_file = join(project_dir, "schema", "tpcds_schema.pickle")
    benchmark_file = join(project_dir, "benchmark", "benchmark_timings_{}gb.csv".format(scale))
#=======

    spark = SparkSession.builder.master("local[1]").appName("TPC DS").enableHiveSupport().getOrCreate()
    schema_dict = load_schema(schema_file)
    df_dict = load_data(spark, schema_dict, data_dir)
    benchmark_dict = run_benchmark(spark, df_dict, queries_dir, result_dir, log_file, error_file)
    save_benchmark(benchmark_dict, benchmark_file)

    spark.stop()

if __name__ == "__main__":
    basename = argv[0] if len(argv) else "run_queries.py"
    parser = ArgumentParser("{} runs your TPC-DS queries for Spark SQL".format(basename))
    parser.add_argument("-s", "-scale", help="Scale factor for your tpc queries.", type=int)
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

