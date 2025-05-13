#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 29 12:30:56 2025

@author: Rishabh_Tyagi
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Hello Spark').master('local[2]').getOrCreate()


data_list = [("Ravi", 28),
             ("David", 45),
             ("Abdul", 37)]

df = spark.createDataFrame(data_list).toDF("Name", "Age")

df.show()
