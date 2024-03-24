from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : Data_Engineering_1, Script : Data_Frame_1")

spark = SparkSession.builder.appName("Data_Frame_1").master("local[3]").getOrCreate()

schema = StructType([
    StructField("F_Name", StringType(), True),
    StructField("L_Name", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Nationality", StringType(), True),
])

data = [["Monica", "Bellucci", 2500000, "Italy"], ["Kate", "Upton", 2250000, "USA"],
        ["Gigi", "Hadid", None, "USA"], ["Barbara", "Palvin", 5414782, "USA"]]

cust_df = spark.createDataFrame(data, schema)
cust_df.show(10, False)
spark.stop()
