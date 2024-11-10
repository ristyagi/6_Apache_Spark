Two types of storages in Databricks
DBFS and external mounted storage

### ------------
DBUTILS LS command

%python
dbutils.fs.ls("dbfs:/mnt/rt_test/")

### ------------
df = spark.read.format("csv")\
            .option("header", "true")\
            .option("sep", ",")\
            .option("inferschema", "true")\
            .option("mode", "FAILFAST")\
            .load("dbfs:/mnt/rt_test/2010-summary.csv")
display(df)
