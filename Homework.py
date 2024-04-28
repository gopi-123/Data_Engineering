Homework:



# Step1 : Create Pandas dataframe -by reading smaple csv Ex: Sample Employee table
import pandas as pd
file_path = "/Workspace/Users/gopinath.mylapura.anjaneyareddy@asml.com/Datasets/Sample_Employeedata.csv" 
df = pd.read_csv(file_path)
#print(df.head())
#df.info()

# Not working below code
#df = spark.read.csv(file_path)
#df.show()
# 
#https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/pandas_pyspark.html

# Step 2: 
# convert pandas Datframe to pandas-on-Spark DataFrame
# pandas DataFrame can be a pandas-on-Spark DataFrame easily as below

import pyspark.pandas as ps
#pandas DataFrame can be a pandas-on-Spark DataFrame easily as below:3 i.e pyspark dataframe
psdf = ps.from_pandas(pdf)
# printing pyspark/pandas on Spark dataframe

print("#### Printing pyspark  dataframe####")
print(psdf.head())
print("type(psdf)",type(psdf))

# Step 3: Write to test table "GOPI_test_table" in following Schema path : lab_ssa.project_qaa_11128.gopi_test_table
#/Workspace/Projects/QAA-11128/KNIME/5Why_8D_Contributing_GLs_V2_PRD
# https://adb-8419334039469530.10.azuredatabricks.net/explore/data/lab_ssa/project_qaa_11128?o=8419334039469530

#pandas-on-Spark DataFrame and Spark DataFrame are virtually interchangeable.-PySpark users can access the full PySpark APIs by calling DataFrame.to_spark().
# sdf = psdf.to_spark()

table_path = "lab_ssa.project_qaa_11128.gopi_test_table"

#PySpark users can access the full PySpark APIs by calling DataFrame.to_spark()
spdf.to_spark().write.mode('overwrite').saveAsTable(table_path)

'''
import pyspark.pandas as ps
spdf = ps.from_pandas(pdf)
spdf.to_spark().write.mode('overwrite').saveAsTable('lab_ssa.project_qaa_11128.RCA_Contributing_GLs_V2')
'''

'''
# Step 2: Convert to Spark DataFrame
# Assuming 'spark' is your SparkSession
from pyspark.sql import 
spark_df = spark.createDataFrame(df)  # If reading from CSV
'''

######  Final output ########

import pyspark.pandas as ps
import pandas as pd

csv_file_path = "/Workspace/Users/gopinath.mylapura.anjaneyareddy@asml.com/Datasets/Sample_Employeedata.csv" 
pdf = pd.read_csv(csv_file_path)


#Create pyspark pandas dataframe: convert pandas dataframe to pyspark dataframe (pandas on Spark Dataframe) #pandas DataFrame can be a pandas-on-Spark DataFrame easily as below:3 i.e pyspark dataframe
psdf = ps.from_pandas(pdf) 

# Create Sparkdataframe i.e convert pyspark pandas dataframe('pandas on Spark Dataframe') to Sparkdataframe
sdf = psdf.to_spark() #convert 'pandas on Spark Dataframe' to Sparkdataframe

# Write to test table "GOPI_test_table" in following schema path
sdf.write.mode('overwrite').saveAsTable('lab_ssa.project_qaa_11128.gopi_test_table')
