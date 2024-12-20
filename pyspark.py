pip install pyspark

# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# loading the csv data
csv_data = 'C:\Users\Abhishek\Downloads\git-demo\DataOpsRepo\BigMart Sales.csv' 

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

# With Column Rename
df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# Adding Current Date
df = df.withColumn('curr_date', current_date())

# Date Difference
df = df.withColumn('week_after', date_add('curr_date', 7))

# Date b/w 2 intervals
df = df.withColumn('date_diff', datediff('week_after', 'week_before'))

# Date format
df = df.withColumn('week_before', date_format('week_before', format='dd-MM-yyyy'))

# Null COunts

null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])

null_counts.display()

# Handling Null Values - dropna("all")
df.dropna('all').display()
#----------------------------------------------------------------------
# Handling Null values - subset -dropna
df.dropna(subset=['Item_weight']).display()

# Handling Null Values - Fillna
df.dropna(subset=['Item_weight']).display()

# Split and indexing
df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# Explode Function
df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# Array Constraints
df.withColumn('Outlet_Type', array('Outlet_Type')).display()

# Group By 
# Group the data by item time type and find the total cost of the items by group
df.groupBy('Item_Type').sum('Item_MRP').display()

# Avg MRP of the grouped items
df.groupBy('Item_Type').avg('Item_MRP').display()

# Group by Item type and outlet size
df.groupBy('Item_Type', 'Outlet_Size').sum('Item_MRP').display()

# GRoup by Item type and Outlet size and give the sum and avg MRP
df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP'), avg('Item_MRP')).display()
#----------------------------------------------------------------------