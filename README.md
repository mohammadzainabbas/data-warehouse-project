### INFO-H-419: Data Warehouse' Project ✨👨🏻‍💻

[![Open in VS Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/mohammadzainabbas/data-warehouse-project)

## Table of contents

- [Introduction](#introduction)
  * [TPC-DS](#tpc-ds)
  * [Spark SQL](#spark)
- [Setup](#setup)
  * [via Setup script](#script-setup)
  * [Apache Spark](#apache-spark-setup)
  * [PySpark](#pyspark-setup)
  * [TPC-DS](#tpc-ds-setup)
- [Benchmark](#benchmark)
- [Result](#result)

---

<a id="introduction" />

### 1. Introduction

<a id="tpc-ds" />

#### 1.1 TPC-DS

TPC DS is a standard used for benchmarking relational databases with a power test. The official TPC-DS tools can be found at [tpc.org](http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp). We have used v2.10.0 with some modifications done by [gregrahn/tpcds-kit](https://github.com/gregrahn/tpcds-kit).

<a id="spark" />

#### 1.2 Spark SQL

For TPC DS benchmarking, we have used Apache spark's module (_Spark SQL_) for structured data processing. It provides a programming abstraction called DataFrames and can also act as a distributed SQL query engine. You can find out more from their [official doc](https://spark.apache.org/sql/).

<a id="setup" />

### 2. Setup

Before starting, you need to setup your machine first. Please follow the below mentioned guide to setup Spark on Mac machine. 

> Note: For Linux, you might need to manually install apache-spark and setup your python enviornment

<a id="script-setup" />

#### 2.1 via Setup script

We have created a setup script which will setup brew, apache-spark and conda enviornment. If you are on Mac machine, you can run the following commands:

```bash
git clone https://github.com/mohammadzainabbas/data-warehouse-project.git
cd data-warehouse-project && sh scripts/setup.sh
```

After running the above mentioned commands, your file structure should look something like this. (you need to go to parent directory first `cd ..`)

<a id="file-structure" />

```
├── data-warehouse-project
│   ├── schema
│   ├── scripts
└── tpcds-kit
    ├── query_templates
    ├── query_variants
    ├── specification
    └── tools
```

For Mac users, that's all you needed to do. Now, you can skip to benchmarking part.

For Linux users, please follow below mentioned steps for setup.

<a id="apache-spark-setup" />

#### 2.2 Apache Spark

If you are on Linux, you need to install [Apache Spark](https://spark.apache.org) by yourself. You can follow this [helpful guide](https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/) to install apache spark.

<a id="pyspark-setup" />

#### 2.3 PySpark

We recommend you to install _conda_ on your machine. You can setup conda from [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)

After you have conda, create new enviornment via:

```bash
conda create -n spark_env python=3.8 pandas
```

> Note: We are using Python3.8 because spark doesn't support Python3.9 and above (at the time of writing this)

Activate your enviornment:

```bash
conda activate spark_env
```

Now, you need to install _pyspark_:

```bash
pip install pyspark
```

If you are using bash:

```bash

echo "export PYSPARK_DRIVER_PYTHON=$(which python)" >> ~/.bashrc
echo "export PYSPARK_DRIVER_PYTHON_OPTS=''" >> ~/.bashrc
. ~/.bashrc

```

And if you are using zsh:

```zsh

echo "export PYSPARK_DRIVER_PYTHON=$(which python)" >> ~/.zshrc
echo "export PYSPARK_DRIVER_PYTHON_OPTS=''" >> ~/.zshrc
. ~/.zshrc

```

<a id="tpc-ds-setup" />

#### 2.4 TPC DS

For Linux users, you need to manually install _tpcds-kit_ from [gregrahn/tpcds-kit](https://github.com/gregrahn/tpcds-kit)

Make sure you have the same file structure as [mentioned above](#file-structure)

<a id="benchmark" />

### Benchmark

Run the following command to benchmark for scale N

```bash
sh scripts/benchmark.sh -scale N
```

Here, `N` is the scale factor against which you want to benchmark.

If you want to run for multiple benchmark scales

1. Modify the `benchmark_scales` variable to your scale values
2. Run the below command:

```bash
sh scripts/benchmark_all.sh
```

This will run for all the scales.

<a id="result" />

### Result

The benchmarking results can be found under `benchmark` folder. For example, when you run `sh benchmark.sh -scale 1` (_i.e: benchmarking for 1 GB scale_), you will find two files in `benchmark` directory:

1. `benchmark_timings_1gb.csv` contains the average time (_in seconds_) for all queries.
2. `benchmark_timings_1gb.pickle` contains all the timings for each query (usually 5 per query).

