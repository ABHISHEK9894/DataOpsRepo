pip install pyspark

# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# loading the csv data
csv_ata = 'C:\Users\Abhishek\Downloads\git-demo\DataOpsRepo\BigMart Sales.csv' 

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_data)

# Load JSON Data
json_data = 'c:\Users\Abhishek\Downloads\drivers.json'

df1 = spark.read.format("json").option("inferSchema", "True").option("multiline","False").load(json_data)

# Display only selectec columns
df1.display()

df1.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# Aliasing the column names
df1.select(col('Item_Identifier').alias("Item_ID")).display()

# Filter command
# 1. Filter the data with fat content = Regular
# 1. Slice the data with item type = Soft Drinks and Weight < 10
# 1. Fetch the data with Tier in (Tier1 or Tier2) and outlet size is Null

#1. Filter
df1.filter(col('Item_Fat_Content') == 'Regular').display()

#2. Slice
df1.filter((col('Item_Weight') < 10) & (col('Item_Type') == 'Soft Drinks')).display()

# Fetch the data with Tier in (Tier1 or Tier2) and outlet size is Null

df1.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()