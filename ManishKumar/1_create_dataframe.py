#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 19:01:05 2025

@author: Rishabh_Tyagi
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('createdf').master('local[2]').getOrCreate()

raw_data = [(1,1),
            (2,1),
            (3,1),
            (4,1),
            (5,1),
            (6,1),
            (7,1)
    ]

my_schema = ['id', 'num']

df = spark.createDataFrame(data = raw_data, schema= my_schema)
df.show()
