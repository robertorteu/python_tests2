# Importing the required libraries
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Creating a SparkSession
spark = SparkSession.builder.getOrCreate()

# Set Spark configuration properties
spark.conf.set("fs.azure.sas.landing.landingtest.blob.core.windows.net",
               "sp=r&st=2024-04-03T10:54:31Z&se=2024-04-03T18:54:31Z&spr=https&sv=2022-11-02&sr=c&sig=ERTYPA0W6vEeo8oolruqLHbtAl%2B3JYU%2F9ImMBFRv%2B7g%3D")  # Replace <sas_token> with your generated SAS token

# Loading the CSV file into a dataframe
df = spark.read.csv("wasbs://landing@landingtest.blob.core.windows.net/dataset1.csv",
                    header=True,
                    inferSchema=True)
df.show()

# Rename the columns
df = df.withColumnRenamed("Client ID", "ClientID").withColumnRenamed("Client Name", "ClientName")

# Remove non-numeric characters from Amount column
df = df.withColumn("Amount", regexp_replace(col("Amount"), "[^0-9.]", ""))
df.show()

# Register the dataframe as temporary view
df.createOrReplaceTempView("temp_df")

# Check if the table already exists and drop it if necessary
if spark.catalog.tableExists("db_testing.bronze.dataset_1"):
    spark.sql("DROP TABLE db_testing.bronze.dataset_1")

# Create the table if it does not exist
spark.sql("""
   CREATE TABLE IF NOT EXISTS db_testing.bronze.dataset_1
   USING DELTA
   AS
   SELECT *
   FROM temp_df
""")

# Query the created table
spark.sql("SELECT * FROM db_testing.bronze.dataset_1").show()