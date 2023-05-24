import requests
import time
import datetime
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os





# os.makedirs('/tmp/alpha')
# os.makedirs('/tmp/alpha/in')
# os.makedirs('/tmp/alpha/out')

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Get the underlying SparkContext
sc = spark.sparkContext


url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=71DTB3BBJVRIPACV&datatype=csv"


response = requests.get(url)

print(type(response))
print(type(response.content))
print(type(response.content.decode("utf-8")))

file_path = '/tmp/alpha/in/ma_data_.csv'

dbutils.fs.put(file_path, response.content.decode("utf-8"), overwrite=True)






# tickers = ['AAPL', 'AMZN', 'MSFT', 'GOOGL', 'NFLX'] 


# def get_hrtime():
#     return (datetime.datetime.now() + datetime.timedelta(hours=2)).strftime('%Y-%m-%d_%H%M%S')

# def dl_stocks(ticker:str):
#   url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey=VYMPBNXERFQNJ3CL&datatype=csv"
#   response = requests.get(url)
#   return response.content.decode("utf-8")

# schema = StructType([
#     StructField("date", StringType()),
#     StructField("open", StringType()),
#     StructField("high",  StringType()),
#     StructField("low",  StringType()),
#     StructField("close",  StringType()),
#     StructField("volume",  StringType())
# ])

# def download_data():
#   for ticker in tickers:
#     base_directory = f'/tmp/alpha/in/{ticker}/'
#     timestamp = get_hrtime()
#     file_prefix = ticker+'_'
#     file_name = f"{file_prefix}{timestamp}.txt"
#     file_path = base_directory + file_name
#     file_contents = dl_stocks(ticker)
#     print (file_path)


#     # dbutils.fs.put(file_path, file_contents, overwrite=True)
#     # df = spark.createDataFrame(file_contents, schema)
#     # df.write.mode("overwrite").csv(output_file_path)


#     time.sleep(1)
#     print('_______')
    
# download_data()














# # Specify the directory path
# directory_path = "/tmp/check"

# # Create the directory using os module
# # os.makedirs(directory_path)


# data = [("John", "25"), ("Jane", "30"), ("Bob", "35")]

# schema = StructType([
#     StructField("Name", StringType(), nullable=False),
#     StructField("Age", StringType(), nullable=False)
# ])



# content = "je suis un fichier texte"

# df = spark.createDataFrame(data, schema)

# # Specify the output file path
# output_file_path = "/tmp/output.csv"


# df.write.mode("overwrite").csv(output_file_path) 

# df2 = spark.read.csv("/tmp/output.csv")

# # Verify if the file was created
# file_exists = spark._jvm.java.nio.file.Files.exists(spark._jvm.java.nio.file.Paths.get(output_file_path))
# if file_exists:
#     print(f"File '{output_file_path}' has been created successfully.")
# else:
#     print(f"Failed to create file '{output_file_path}'.")


