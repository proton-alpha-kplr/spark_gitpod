import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import input_file_name

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Get the underlying SparkContext
sc = spark.sparkContext

# Specify the directory path
directory_path = "/tmp/check"

# Create the directory using os module
# os.makedirs(directory_path)


data = [("John", "25"), ("Jane", "30"), ("Bob", "35")]

schema = StructType([
    StructField("Name", StringType(), nullable=False),
    StructField("Age", StringType(), nullable=False)
])



# content = "je suis un fichier texte"

df = spark.createDataFrame(data, schema)

# Specify the output file path
output_file_path = "/tmp/output.csv"

df.write.mode("overwrite").csv(output_file_path) 

output_file_path = "/tmp/output.csv"

df.write.mode("overwrite").csv(output_file_path) 



# # Verify if the file was created
# file_exists = spark._jvm.java.nio.file.Files.exists(spark._jvm.java.nio.file.Paths.get(output_file_path))
# if file_exists:
#     print(f"File '{output_file_path}' has been created successfully.")
# else:
#     print(f"Failed to create file '{output_file_path}'.")

df = spark.read.csv("/tmp/output.csv")\
    .select(input_file_name().alias("File Path"))

# Afficher les chemins des fichiers
df.show(truncate=False)