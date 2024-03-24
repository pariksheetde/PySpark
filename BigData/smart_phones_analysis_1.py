from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Smart_Phones_Analysis_1")

spark = SparkSession.builder.appName("Smart_Phones_Analysis_1").master("local[3]").getOrCreate()


columns = ["Maker", "Model", "OS", "Price", "Release_Date"]
data = [("Samsung", "Galaxy S8", "Android" ,65000, "15-01-2021"),
        ("Apple", "IPhone 10 MAX", "iOS", 75000, "12-09-2020"),
        ("Apple", "IPhone X", "iOS", 125000, "12-09-2020"),
        ("Redmi", "Redmi 9", "Android", 10900,"12-09-2020")
        ]

df = spark.createDataFrame(data).toDF(*columns)
df.show(10, False)

spark.stop()