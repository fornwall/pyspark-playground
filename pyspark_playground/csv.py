from pyspark.sql import SparkSession

# https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/
def main():
    spark = SparkSession.builder.appName("MyApp").getOrCreate()

    print("Reading CSV file..")
    df = spark.read.csv("data/zipcodes.csv")
    print("  - Printing schema")
    df.printSchema()
    
    print("\nReading CSV file - using header records for column names..")
    df = spark.read \
         .option("header", True) \
        .csv("data/zipcodes.csv")
    print("  - Printing schema")
    df.printSchema()
    print("  - Calling show()")
    df.show()
