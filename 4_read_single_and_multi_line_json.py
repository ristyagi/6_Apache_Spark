#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 17 23:57:54 2025

@author: Rishabh_Tyagi
"""

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("read_single_line_json") \
        .master('local[2]') \
        .getOrCreate()
        

# Read single line json        
df_json = spark.read.format("json") \
            .option("inferschema", True) \
            .load("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/json_datasets/1_single_line.json")
            
df_json.show()


# Read multi-line json
df_multi_line_json = spark.read.format("json") \
    .option("inferschema", True) \
        .load("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/json_datasets/2_multi_line.json")
# Errors out as we have to pass multiline option to read multi-line jsons

        
df_multi_line_json2 = spark.read.format("json") \
    .option("inferschema", True) \
        .option("multiline", True) \
        .load("/Users/Rishabh_Tyagi/1_RT_docs/2_My_py_projects/1_Spark_RT/0_datasets/json_datasets/2_multi_line.json")

df_multi_line_json2.show()
