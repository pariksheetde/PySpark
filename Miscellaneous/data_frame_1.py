import pandas as pd
from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : Miscellaneous, Script : Data Frame 1")
    
    
# create a sample Python dictionary
data = {'F_Name': ['Monica', 'Kate', 'Bob', 'Pamela'], 
        'L_Name' : ['Bellucci', 'Upton', 'Marley', 'Anderson'],
        'Age': [25, 30, 35, 37]}

# convert the dictionary to a Pandas DataFrame
pandas_df = pd.DataFrame.from_dict(data)

# create a SparkSession
spark = SparkSession.builder.appName("Data Frame 1").getOrCreate()

# create a Spark DataFrame from the Pandas DataFrame
df = spark.createDataFrame(pandas_df)

cln_df = df.selectExpr("f_name", "l_name", "age") \
    .withColumn("full_name", concat("f_name", lit(" "),"l_name")) \
    .show()

spark.stop()