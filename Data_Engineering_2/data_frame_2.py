from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : Data Frame 2")

spark = SparkSession.builder.appName("Data_Frame_2").master("local[3]").getOrCreate()

schema = StructType([
    StructField("Company", StringType(), True),
    StructField("Model", StringType(), True),
    StructField("OS", StringType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Launch_DT", StringType(), True)
])

data = [["Samsung", "Galaxy S8", "Android" ,65000, "15-10-2021"],
        ["Apple", "IPhone 10 MAX", "iOS", 75000, "12-11-2020"],
        ["Apple", "IPhone X", "iOS", 125000, "12-10-2017"],
        ["Redmi", "Redmi 9", "Android", 10900,"12-12-2015"],
        ["Apple", "MacBook Pro 16", "IOS", 310000, "12-12-2021"]
        ]

mobile_df = spark.createDataFrame(data, schema)

clean_mobile_df = mobile_df.withColumn("Date", substring("Launch_DT", 1, 2).cast("Int")) \
    .withColumn("Month", substring("Launch_DT", 4,2).cast("Int")) \
    .withColumn("Year", substring("Launch_DT", 7, 4).cast("Int"))

clean_mobile_df.printSchema()
clean_mobile_df.show(10, False)

spark.stop()