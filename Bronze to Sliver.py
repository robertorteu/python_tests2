# Importing the required libraries
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Creating a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the dataset
dataset = spark.read.format("delta").table("db_testing.bronze.dataset_1")
dataset.show()
# Filter the dataset by ClientName starting with "A"
filtered_dataset = dataset.filter(dataset["ClientName"].startswith("A"))
filtered_dataset.show()
# Save the filtered dataset as a Delta table
filtered_dataset.write.format("delta").mode("overwrite").saveAsTable("db_testing.silver.dataset_1_subset")

# Query the created table
spark.sql("SELECT * FROM db_testing.silver.dataset_1_subset").show()

