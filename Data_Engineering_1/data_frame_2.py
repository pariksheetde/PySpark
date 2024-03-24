from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_1, Script : Data_Frame_2")

spark = SparkSession.builder.appName("Data_Frame_2").master("local[3]").getOrCreate()

data = [
    (100, "Monica", "Bellucci", "Italy", "Modeling"),
    (110, "Kate", "Upton", "USA", "Modeling"),
    (120, "Kate", "Winslet", "USA", "Acting"),
    (130, "Pierce ", "Brosnan", "USA", "Acting"),
    (140, "Tom", "Cruise", "USA", "Acting"),
    (150, "Chris", "Patt", "USA", "Acting"),
    (160, "Daniel", "Craig", "UK", "Acting")]

columns = ["ID", "F_Name", "L_Name", "Origin", "Occupation"]

actors_df = spark.createDataFrame(data, columns)
actors_df.show()
spark.stop()