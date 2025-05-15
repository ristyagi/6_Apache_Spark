#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 13 18:17:11 2025

@author: Rishabh_Tyagi
"""

from pyspark.sql import SparkSession

# https://stackoverflow.com/questions/56089894/issue-while-creating-sparksession-object-using-sparkconf

 # .config is passed to enable hive catalog on local setup

spark = SparkSession \
    .builder \
    .appName("loadcsv") \
    .config("spark.sql.catalogImplementation", "hive") \
    .master('local[2]') \
    .getOrCreate()


fire_df = spark.read.format("csv")\
            .option("header", True)\
            .option("inferschema", True)\
            .load("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/fire_service_call_dataset/Fire_Department_Calls_for_Service.csv")

fire_df.show()

fire_df.createGlobalTempView("gv_fire_df")
spark.sql("select * from global_temp.gv_fire_df").show()

# https://stackoverflow.com/questions/50914102/why-do-i-get-a-hive-support-is-required-to-create-hive-table-as-select-error

spark.sql("CREATE DATABASE IF NOT EXISTS mydatabase") # to create a new databse, but where is it created? Its created in local HOME directory

spark.sql("create table if not exists spark_catalog.mydatabase.fire_service_tbl as select * from global_temp.gv_fire_df")

spark.sql("select * from mydatabase.fire_service_tbl").show()


fire_df.createOrReplaceTempView("v_fire_df")
spark.sql("select * from v_fire_df").show()

spark.sql("SHOW TABLES IN mydatabase").show()
spark.sql("SHOW SCHEMAS").show()


spark.sql("SHOW TABLE EXTENDED IN mydatabase LIKE 'fire_service_tbl%'").show()

