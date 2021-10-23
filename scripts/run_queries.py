from os import getcwd, listdir, environ
from os.path import join, isfile, abspath, pardir
from time import time
from shutil import copyfile
from sys import stdout, stderr, __stdout__, __stderr__
from contextlib import redirect_stdout
from pyspark.sql import SparkSession

#spark-submit --jars ~/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar scripts/run_queries.py

#------------------------
# Configuration(s)
#------------------------
connection_string = "jdbc:mysql://localhost/test_tpcds"
spark = SparkSession.builder.master("local[1]").appName("TPC DS").enableHiveSupport().getOrCreate()
tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "dbgen_version", "household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
data_dir = join(abspath(join(getcwd(), pardir)), "tpcds-kit", "data_correct") # ../tpcds-kit/data_correct
log_file = join(getcwd(), "tpcds.out")
error_file = join(getcwd(), "tpcds.err")

#------------------------
# Helper functions
#------------------------
def log_message(*args, **kwargs):
    # redirect stdout to a file
    with open(log_file, 'a') as f:
        with redirect_stdout(f):
            print(*args, **kwargs)

#------------------------
# 1. Get schema & tables
#------------------------
schema_dict = {}
df_dict = {}

start_time = time()
for index, table in enumerate(tables):
    table_df = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", connection_string).option("dbtable", table).option("user", "root").load()
    schema_dict[table] = table_df.schema
    df = spark.read.load( join(data_dir, "{}.csv".format( table )), format="csv", delimiter="|", header="true", schema=table_df.schema)
    df_dict[table] = df
    df.createOrReplaceTempView("{}".format( table ))

log_message("Took {} seconds to load {} tables with schemas.".format(time() - start_time, len(list(df_dict.keys())) ))
start_time = time()
spark.stop()
log_message("Took {} seconds to stop spark.".format(time() - start_time))

