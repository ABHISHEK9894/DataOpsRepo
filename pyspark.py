pip install pyspark

# Import Libraries
from pyspark.sql import SparkSession

# loading the csv data
csv_ata = 'C:\Users\Abhishek\Downloads\git-demo\DataOpsRepo\BigMart Sales.csv' 

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_data)

# Load JSON Data
json_data = 'c:\Users\Abhishek\Downloads\drivers.json'

df1 = spark.read.format("json").option("inferSchema", "True").option("multiline","False").load(json_data)