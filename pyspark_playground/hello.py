from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    print(f"Spark version: {spark.version}")

