# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b78a635f-b24b-4c5a-a036-d4843a7afc22",
# META       "default_lakehouse_name": "rawFiles",
# META       "default_lakehouse_workspace_id": "3b06e9fb-b107-48ac-9eb9-bfbc1e4e89ec",
# META       "known_lakehouses": [
# META         {
# META           "id": "b78a635f-b24b-4c5a-a036-d4843a7afc22"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a45ea507-60c2-424f-9268-ff75de6a4310",
# META       "known_warehouses": [
# META         {
# META           "id": "a45ea507-60c2-424f-9268-ff75de6a4310",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### **1. Transforming the raw files in to Bronze stage**
# 
# 2013 to 2019 have same separate files but 2020 to 2023 are in same file with different sheet

# CELL ********************

# Load the file first
# Going to use pandas

import pandas as pd 

lakehouse = "default"
folder = "MedicareEnrollmentData"
filename = "CPS MDCR ENROLL AB 1-8 2020.xlsx"

path = f"/lakehouse/{lakehouse}/Files/{folder}/{filename}"

df = pd.read_excel(path, sheet_name= "MDCR ENROLL AB 2" , skiprows=2, header=1)

#df = pd.read_excel("abfss://3b06e9fb-b107-48ac-9eb9-bfbc1e4e89ec@onelake.dfs.fabric.microsoft.com/b78a635f-b24b-4c5a-a036-d4843a7afc22/Files/MedicareEnrollmentData/2013_CPS_MDCR_ENROLL_AB_2.xlsx", skiprows = 2, header = 1)
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns = (
                df.columns
                .str.strip()                             # remove leading/trailing spaces
                .str.replace(" ", "_")                   # replace spaces
                .str.replace("-", "_")                   # replace spaces
                .str.replace("%", "Percent")
                .str.replace(r"[^\w]", "_", regex=True)  # remove special characters
                .str.replace(r'_\d+$', '', regex=True)   #remove trailing number
                .str.replace(r"[¹²³⁴⁵⁶⁷⁸⁹⁰]+$", "", regex=True)  # remove superscript numbers
)
print(df.columns)
#df = df[df["Area of Residence"] != "BLANK"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Remove Unwanted rows at the top and bottom**

# CELL ********************

## deleting blank rows

df = df[df["Area of Residence"] != "BLANK"]
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns = df.columns.str.replace(" ", "_")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## deleting the aggregations rows at the bottom 

df53 = df.iloc[0:52]
df53.tail(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Add a new column to hold the year value**

# CELL ********************

df53['year'] = 2013
df53.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Write the file to Silver Lakehouse Table**

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark_df = spark.createDataFrame(df53)
spark_df.write.format("delta") \
  .mode("overwrite") \
  .save("abfss://3b06e9fb-b107-48ac-9eb9-bfbc1e4e89ec@onelake.dfs.fabric.microsoft.com/aa28dc73-b2d0-4532-9853-1650d600564d/Files/MedicareEnrollmentData/2014_data.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Combining all the above cells into one single block to Loop

# CELL ********************

import pandas as pd
import numpy as np
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# set folder path
folder_path = "/lakehouse/default/Files/MedicareEnrollmentData"

# get file names
#raw_files = [f for f in os.listdir(folder_path) if f.endswith("CPS_MDCR_ENROLL_AB_2.xlsx")]
excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]
raw_files = [] 

for file in excel_files:

    path = f"{folder_path}/{file}"
    mode_time = os.path.getmtime(path)
    xls = pd.ExcelFile(path)
    if "MDCR ENROLL AB 2" in xls.sheet_names:
        raw_files.append((file,mode_time))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# order the files based on the arrival times to the lakehouse

raw_files.sort(key=lambda x: x[1])
ordered_files = [f[0] for f in raw_files]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(ordered_files)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Writing the each cleaned file as delta table on to Silver lakehouse


# CELL ********************

import pandas as pd
import numpy as np
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

spark = SparkSession.builder.getOrCreate()

# set folder path
folder_path = "/lakehouse/default/Files/MedicareEnrollmentData"

# get file names
#raw_files = [f for f in os.listdir(folder_path) if f.endswith("CPS_MDCR_ENROLL_AB_2.xlsx")]
excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]
raw_files = [] 

for file in excel_files:

    path = f"{folder_path}/{file}"
    mode_time = os.path.getmtime(path)
    xls = pd.ExcelFile(path)
    if "MDCR ENROLL AB 2" in xls.sheet_names:
        raw_files.append((file, mode_time))

#order the files based on the arrival time to the lakehouse
raw_files.sort(key=lambda x: x[1])
ordered_files = [f[0] for f in raw_files]

# # Sort by year extracted from filename
# def extract_year(filename):
#     match = re.search(r'(20\d{2})', filename)  # match any 4-digit year starting with 20
#     return int(match.group()) if match else 0   # fallback to 0 if no year found

# sorted_files = sorted(file, key=extract_year)

schema = StructType([
    StructField("Area_of_Residence", StringType(), True),
    StructField("State_Population", LongType(), True),
    StructField("Total_Medicare_Enrollment", LongType(), True),
    StructField("Total_Medicare_Enrollment_Percent_of_Resident_Population", DoubleType(), True),
    StructField("Original_Medicare_Enrollment", LongType(), True),
    StructField("Original_Medicare_Enrollment_Percent_of_Total_Enrollment", DoubleType(), True),
    StructField("Medicare_Advantage_and_Other_Health_Plan_Enrollment", LongType(), True),
    StructField("Medicare_Advantage_and_Other_Health_Plan_Enrollment_Percent_of_Total_Enrollment", DoubleType(), True),
    StructField("Total_Enrollment_Metropolitan_Residence", LongType(), True),
    StructField("Total_Enrollment_Micropolitan_Residence", LongType(), True),
    StructField("Total_Enrollment_Non_Core_Based_Statistical_Area", LongType(), True),
    StructField("Year", LongType(), True)
])

Year = 2013

for file in ordered_files:

    path = f"{folder_path}/{file}"
    
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        if "MDCR ENROLL AB 2" in sheet: 
            df = pd.read_excel(path,sheet_name=sheet, skiprows=2, header=1)

            # remove blank rows
            df = df[df["Area of Residence"] != "BLANK"]

            # clean column names
            df.columns = (
                df.columns
                .str.strip()                             # remove leading/trailing spaces
                .str.replace(" ", "_")                   # replace spaces
                .str.replace("-", "_")                   # replace spaces
                .str.replace("%", "Percent")
                .str.replace(r"[^\w]", "_", regex=True)  # remove special characters
                .str.replace(r'_\d+$', '', regex=True)   #remove trailing number
                .str.replace(r"[¹²³⁴⁵⁶⁷⁸⁹⁰]+$", "", regex=True)  # remove superscript numbers
)

            # first 52 rows
            df53 = df.iloc[0:52].copy()

            # Add the Year column
            df53["Year"] = Year

            df53 = df53.replace("*", 0)
            df53 = df53.replace("†", 0)
        
            #Change the data type 
            df53["Area_of_Residence"] = df53["Area_of_Residence"].astype(str)
            df53["State_Population"] = df53["State_Population"].astype("int64")
            df53["Total_Medicare_Enrollment"] = df53["Total_Medicare_Enrollment"].astype("int64")
            df53["Total_Medicare_Enrollment_Percent_of_Resident_Population"] = df53["Total_Medicare_Enrollment_Percent_of_Resident_Population"].astype(float)
            df53["Original_Medicare_Enrollment"] = df53["Original_Medicare_Enrollment"].astype("int64")
            df53["Original_Medicare_Enrollment_Percent_of_Total_Enrollment"] = df53["Original_Medicare_Enrollment_Percent_of_Total_Enrollment"].astype(float)
            df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment"] = df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment"].astype("int64")
            df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment_Percent_of_Total_Enrollment"] = df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment_Percent_of_Total_Enrollment"].astype(float)
            df53["Total_Enrollment_Metropolitan_Residence"] = df53["Total_Enrollment_Metropolitan_Residence"].astype("int64")
            df53["Total_Enrollment_Micropolitan_Residence"] = df53["Total_Enrollment_Micropolitan_Residence"].astype("int64")
            df53["Total_Enrollment_Non_Core_Based_Statistical_Area"] = df53["Total_Enrollment_Non_Core_Based_Statistical_Area"].astype("int64")

            #clean table name
            table_name = os.path.splitext(file)[0]   # remove .xlsx
            table_name = (
                table_name
                    .replace(" ", "_")
                    .replace("-", "_")
                )
            table_name = re.sub(r"[^\w]", "_", table_name)
            
            spark_df = spark.createDataFrame(df53,schema=schema)

            spark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(f"Silver.dbo.{table_name}")                             
                
               #.saveAsTable(f"abfss://3b06e9fb-b107-48ac-9eb9-bfbc1e4e89ec@onelake.dfs.fabric.microsoft.com/aa28dc73-b2d0-4532-9853-1650d600564d/Files/MedicareEnrollmentData/{file}")
                
            Year += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Rather than writing each file as separate delta table I am combining all files into a single file and pivot it and save as a single table

# CELL ********************

import pandas as pd
import numpy as np
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

spark = SparkSession.builder.getOrCreate()

# set folder path
folder_path = "/lakehouse/default/Files/MedicareEnrollmentData"

# get file names
#raw_files = [f for f in os.listdir(folder_path) if f.endswith("CPS_MDCR_ENROLL_AB_2.xlsx")]
excel_files = [f for f in os.listdir(folder_path) if f.endswith(".xlsx")]
raw_files = [] 

for file in excel_files:

    path = f"{folder_path}/{file}"
    mode_time = os.path.getmtime(path)
    xls = pd.ExcelFile(path)
    if "MDCR ENROLL AB 2" in xls.sheet_names:
        raw_files.append((file, mode_time))

#order the files based on the arrival time to the lakehouse
raw_files.sort(key=lambda x: x[1])
ordered_files = [f[0] for f in raw_files]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(ordered_files)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



Year = 2013
combined_df = []

for file in ordered_files:
    path = f"{folder_path}/{file}"    
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        if "MDCR ENROLL AB 2" in sheet: 
            df = pd.read_excel(path,sheet_name=sheet, skiprows=2, header=1)

            # remove blank rows
            df = df[df["Area of Residence"] != "BLANK"]

            # clean column names
            df.columns = (
                df.columns
                .str.strip()                             # remove leading/trailing spaces
                .str.replace(" ", "_")                   # replace spaces
                .str.replace("-", "_")                   # replace spaces
                .str.replace("%", "Percent")
                .str.replace(r"[^\w]", "_", regex=True)  # remove special characters
                .str.replace(r'_\d+$', '', regex=True)   #remove trailing number
                .str.replace(r"[¹²³⁴⁵⁶⁷⁸⁹⁰]+$", "", regex=True)  # remove superscript numbers
                    )

            # first 52 rows
            df53 = df.iloc[0:52].copy()

            # Add the Year column
            df53["Year"] = Year

            df53 = df53.replace("*", 0)
            df53 = df53.replace("†", 0)
        
            #Change the data type 
            df53["Area_of_Residence"] = df53["Area_of_Residence"].astype(str)
            df53["State_Population"] = df53["State_Population"].astype("int64")
            df53["Total_Medicare_Enrollment"] = df53["Total_Medicare_Enrollment"].astype("int64")
            df53["Total_Medicare_Enrollment_Percent_of_Resident_Population"] = df53["Total_Medicare_Enrollment_Percent_of_Resident_Population"].astype(float)
            df53["Original_Medicare_Enrollment"] = df53["Original_Medicare_Enrollment"].astype("int64")
            df53["Original_Medicare_Enrollment_Percent_of_Total_Enrollment"] = df53["Original_Medicare_Enrollment_Percent_of_Total_Enrollment"].astype(float)
            df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment"] = df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment"].astype("int64")
            df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment_Percent_of_Total_Enrollment"] = df53["Medicare_Advantage_and_Other_Health_Plan_Enrollment_Percent_of_Total_Enrollment"].astype(float)
            df53["Total_Enrollment_Metropolitan_Residence"] = df53["Total_Enrollment_Metropolitan_Residence"].astype("int64")
            df53["Total_Enrollment_Micropolitan_Residence"] = df53["Total_Enrollment_Micropolitan_Residence"].astype("int64")
            df53["Total_Enrollment_Non_Core_Based_Statistical_Area"] = df53["Total_Enrollment_Non_Core_Based_Statistical_Area"].astype("int64")

            #clean table name
            # table_name = os.path.splitext(file)[0]   # remove .xlsx
            # table_name = (
            #     table_name
            #         .replace(" ", "_")
            #         .replace("-", "_")
            #     )
            # table_name = re.sub(r"[^\w]", "_", table_name)
            
            # spark_df = spark.createDataFrame(df53,schema=schema)

            # spark_df.write \
            #     .format("delta") \
            #     .mode("overwrite") \
            #     .saveAsTable(f"Silver.dbo.{table_name}")                             
                
            #    #.saveAsTable(f"abfss://3b06e9fb-b107-48ac-9eb9-bfbc1e4e89ec@onelake.dfs.fabric.microsoft.com/aa28dc73-b2d0-4532-9853-1650d600564d/Files/MedicareEnrollmentData/{file}")
            combined_df.append(df53) 
            Year += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = pd.concat(combined_df, ignore_index=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#print(final_df)
final_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

id_cols = ["Area_of_Residence", "Year"]

value_cols = [
    "State_Population",
    "Total_Medicare_Enrollment",
    "Total_Medicare_Enrollment_Percent_of_Resident_Population",
    "Original_Medicare_Enrollment",
    "Original_Medicare_Enrollment_Percent_of_Total_Enrollment",
    "Medicare_Advantage_and_Other_Health_Plan_Enrollment",
    "Medicare_Advantage_and_Other_Health_Plan_Enrollment_Percent_of_Total_Enrollment",
    "Total_Enrollment_Metropolitan_Residence",
    "Total_Enrollment_Micropolitan_Residence",
    "Total_Enrollment_Non_Core_Based_Statistical_Area"
]

long_df = final_df.melt(
    id_vars=id_cols,
    value_vars=value_cols,
    var_name="Metric",
    value_name="Value"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

long_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Convert the Panda's dataframe to Spark df and write the fact table to Silver lakehouse

# CELL ********************

spark_fact_df = spark.createDataFrame(long_df)

spark_fact_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.dbo.medicare_fact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Not using the below format for the fact table

# CELL ********************

pivot_df = long_df.pivot_table(
    index=["Area_of_Residence", "Year"],
    columns="Metric",
    values="Value"
).reset_index()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pivot_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Creating dimension table for Area_of_Residence and the Metrics

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

#get metrics list
metric_dim = spark_fact_df.select("Metric").distinct()

#generating id for each metric
window_spec = Window.orderBy("Metric")

metric_dim = metric_dim.withColumn(
    "Metric_ID",
    row_number().over(window_spec)
)



metric_dim = metric_dim.select(
    "Metric_ID",
    metric_dim.Metric.alias("Metric_Name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#writing it as table on the Silver lakehouse
metric_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.dbo.dim_metric")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Replacing the metric_name with Metric_id on the fact table
fact_with_id = spark_fact_df.join(
    metric_dim,
    spark_fact_df.Metric == metric_dim.Metric_Name,
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_table = fact_with_id.select(
    "Area_of_Residence",
    "Year",
    "Metric_ID",
    "Value"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_table.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_table.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.dbo.medicare_fact_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Doing same for the Area_of_Residence column

# CELL ********************

#get metrics list
state_dim = fact_table.select("Area_of_Residence").distinct()

#generating id for each metric
window_spec = Window.orderBy("Area_of_Residence")

state_dim = metric_dim.withColumn(
    "State_ID",
    row_number().over(window_spec)
)

state_dim = state_dim.select(
    "State_ID",
    "Area_of_Residence"
)

#writing it as table on the Silver lakehouse
state_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.dbo.dim_state")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

fact_with_id = fact_table.join(
    state_dim,
    fact_table.Area_of_Residence == state_dim.Area_of_Residence,
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_table = fact_with_id.select(
    "state_id",
    "Year",
    "Metric_ID",
    "Value"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_table.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.dbo.medicare_enrollment_fact")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

