from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Data_Frame_2")

spark = SparkSession.builder.appName("Data_Frame_2").master("local[3]").getOrCreate()

data = [(100, "Monica", "Bellucci", "Italy")]
columns = ["ID", "F_Name", "L_Name", "Origin"]

df = spark.createDataFrame(data, columns)
df.show()