Two types of storages in Databricks
DBFS and external mounted storage

### ===========================
DBUTILS LS command

%python
dbutils.fs.ls("dbfs:/mnt/rt_test/")


### ===========================
df = spark.read.format("csv")\
            .option("header", "true")\
            .option("sep", ",")\
            .option("inferschema", "true")\
            .option("mode", "FAILFAST")\
            .load("dbfs:/mnt/rt_test/2010-summary.csv")
display(df)


### ============ Another method of creating df and table from csv
# File location and type
file_location = "/FileStore/tables/2_employees.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Create a view or table
temp_table_name = "2_employees_csv"
df.createOrReplaceTempView(temp_table_name)


%sql
/* Query the created temp table in a SQL cell */
select * from `2_employees_csv`

            
# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
permanent_table_name = "2_employees_csv"
df.write.format("parquet").saveAsTable(permanent_table_name)


### ===========================
fire_df = spark.read.format(") \
            .option("headers", True) \
            .option("inferschema", True) \
            .load("fire_df_.csv")
display(fire_df)
