from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/
def main():
    spark = SparkSession.builder.appName("MyApp").getOrCreate()

    df_transactions = spark.read \
         .option("header", True) \
         .option('inferSchema', True) \
        .csv("data/transactions.csv")

    df_users = spark.read \
         .option("header", True) \
         .option('inferSchema', True) \
        .csv("data/users.csv")

    print("  - Two topmost spenders")
    df_transactions \
        .filter(df_transactions.Amount < 0) \
        .groupBy('UserId') \
        .agg(func.sum('Amount').alias('AmountSum')) \
        .orderBy(func.col('AmountSum').asc()) \
        .limit(2) \
        .join(df_users, df_transactions.UserId == df_users.UserId) \
        .show()
