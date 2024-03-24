from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == "__main__":
    print("Package : Bank, Script : Churn Modeling 2")

spark = SparkSession.builder.appName("Churn Modeling 2").master("local[3]").getOrCreate()

churn_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(r"D:\DataSet\DataSet\SparkDataSet\ChurnModeling.csv")

analysis_churn_df = churn_df.select(churn_df["CustomerId"], churn_df["Surname"], churn_df["CreditScore"],
                                    churn_df["Geography"], churn_df["Gender"], churn_df["Age"],
                                    churn_df["Tenure"], churn_df["Balance"], churn_df["NumOfProducts"],
                                    churn_df["HasCrCard"], churn_df["IsActiveMember"],
                                    churn_df["EstimatedSalary"], churn_df["Exited"])

analysis_churn_df.show(10, truncate=False)
print(f"Records Effected {analysis_churn_df.count()}")
spark.stop()