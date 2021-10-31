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

```
git clone https://github.com/mohammadzainabbas/data-warehouse-project.git
cd data-warehouse-project && sh scripts/setup.sh
```

After running the above mentioned commands, your file structure should look something like this. (you need to go to parent directory first `cd ..`)

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
