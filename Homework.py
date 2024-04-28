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

#Step 2: convert pandas datframe to spark dataframe
import pyspark.pandas as ps
spdf = ps.from_pandas(df)
# printing spark dataframe

print("#### Printing pyspark Spark dataframe####")
print(spdf.head())
print("type(spdf)",type(spdf))

# Step 3: Write to test table "GOPI_test_table" in following schema path
#Schema path : lab_ssa.project_qaa_11128.gopi_test_table
#/Workspace/Projects/QAA-11128/KNIME/5Why_8D_Contributing_GLs_V2_PRD
# https://adb-8419334039469530.10.azuredatabricks.net/explore/data/lab_ssa/project_qaa_11128?o=8419334039469530
table_path = "lab_ssa.project_qaa_11128.gopi_test_table"

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
