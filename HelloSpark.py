from pyspark.sql import *

# this is python code not spark code, this is to check if python is working
# if __name__ == '__main__':
#     print('Hello Spark!')

# Simple spark application to create spark df from data list
# This super simple spark application will run on local computer
if __name__ == '__main__':

    spark = SparkSession.builder \
            .appName('Hello Saprk') \
            .master('local[2]') \
            .getOrCreate()

    data_list = [("Ravi", 28),
                 ("David", 45),
                 ("Abdul", 37)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()