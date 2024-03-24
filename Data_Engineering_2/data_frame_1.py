from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : Data Frame 1")

spark = SparkSession.builder.appName("Data_Frame_1").master("local[3]").getOrCreate()

schema = StructType([
    StructField("F_Name", StringType(), True),
    StructField("L_Name", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

data = [["Monica", "Bellucci", 2500000], ["Kate", "Upton", 2250000], ["Kate", "Beckinsale", 1700000],
        ["Audry", "Hepburn", 1750000],["Jennifer", "Lawrence", 1957000], ["Carmen", "Electra", 1550000],
        ["Jennifer", "Garner", 3225000], ["Michelle", "Obama", 1400500], ["Kirsten", "Steward", 2150000],
        ["Sophia", "Turner", 2945000]
        ]

cust_df = spark.createDataFrame(data, schema)

filtered_cust_df = cust_df.where("Salary > 2500000") \
    .show(10, False)
    
spark.stop()