#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 14 18:32:41 2025

@author: Rishabh_Tyagi
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("loadcsv") \
    .config("spark.sql.catalogImplementation", "hive") \
    .master('local[2]') \
    .getOrCreate()
    
# Read Parquet
df_parquet = spark.read.format("parquet") \
            .load("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/Parquet datasets/2010-summary/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

df_parquet.show()

# We don't have to pass any 'options'  while reading parquet because the file itself has all the metadata.

df_parquet.printSchema()


# Write Parquet
df_parquet.write.format("parquet") \
                .option("header", True) \
                .option("mode", "overwrite") \
                .save("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/ops/parquet_op/df_parquet2.parquet")






import os

os.listdir("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/ops/parquet_op/df_parquet2.parquet")
