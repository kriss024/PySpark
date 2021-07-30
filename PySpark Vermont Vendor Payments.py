#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
path="/home/jovyan/work/PySpark"
os.chdir(path)


# In[2]:

os.system("pyspark --name Vermont-Vendor-Payments --master local[*] --driver-memory 4G --executor-memory 2G --conf spark.sql.catalogImplementation=hive")

# Spark UI http://localhost:4040
sc
spark.sparkContext

# configuration check (optional)
for c in sc.getConf().getAll():
    print(c)


# In[3]:

import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit, expr, col, when, coalesce, concat_ws, trim, length, translate
from pyspark.sql.functions import to_date, row_number, regexp_replace, countDistinct, udf
from pyspark.sql.functions import sum as f_sum, min as f_min, max as f_max
from pyspark.sql.window import Window


# In[4]:


data = [['tom', 10], ['nick', 15], ['juli', 14]] 
pd_df = pd.DataFrame(data, columns = ['Name', 'Age'])
df_spark = spark.createDataFrame(pd_df)


# In[5]:


df_spark.printSchema()


# In[6]:


df_spark.toPandas().head()


# In[7]:


df = spark.read.csv('Vermont_Vendor_Payments.csv', header='true', inferSchema = True)


# In[8]:


df = (df
      .withColumn("Amount", col("Amount").cast(DoubleType()))
      .withColumn("Quarter Ending", to_date(col("Quarter Ending"),"MM/dd/yyy")))


# In[9]:


df.printSchema()


# In[10]:


#we can use the columns attribute just like with pandas
columns = df.columns
print('The column Names are:')
for i in columns:
    print(i)


# In[11]:


print('The total number of rows is:', df.count(), '\nThe total number of columns is:', len(df.columns))


# In[12]:


#show first row
df.head()


# In[13]:


# collect() is an action operation that is used to retrieve all the elements of the dataset (from all nodes) to the driver node. 
# We should use the collect() on smaller dataset usually after filter(), group() e.t.c.
# Retrieving larger datasets results in OutOfMemory error.
dataCollect = df.limit(5).collect()
print(dataCollect)


# In[14]:


df.limit(10).toPandas().head()


# In[15]:


df.describe('Amount').toPandas().head()


# In[16]:


df.select("Fund Description").distinct().limit(20).show(truncate = False)


# In[17]:


df.select("Quarter Ending", "Department", "Amount", "State").orderBy(col("Quarter Ending").desc()).show(truncate = False)


# In[18]:


df.withColumn("datetime", col("Quarter Ending").cast("timestamp")).groupBy().agg(f_min("datetime").alias('Min. datetime'), f_max("datetime").alias('Max. datetime')).show(truncate = False)


# In[19]:


df.filter(col("Quarter Ending").between("2015-01-01 00:00:00","2020-01-01 00:00:00")).select("Quarter Ending", "Department", "Amount", "State").orderBy(col("Quarter Ending").desc()).show(truncate = False)


# In[20]:


df.filter(col("Quarter Ending").between(pd.to_datetime("2015-01-01"),pd.to_datetime("2020-01-01"))).select("Quarter Ending", "Department", "Amount", "State").orderBy(col("Quarter Ending").desc()).show(truncate = False)


# In[21]:


df.select("AcctNo", "Account").withColumn("Account_Desc", concat_ws('',col("AcctNo"), lit(" - "), col("Account"))).withColumn("AccName", trim(regexp_replace(col("Account_Desc"), "[^a-z A-Z]", ""))).show(truncate = False)


# In[22]:


df.select("Fund Description", "Amount").withColumn("No", lit(1)).sort("Amount", ascending=False).withColumnRenamed("No", "Number").show(truncate = False)


# In[23]:


df.select("Fund Description", "Amount").where(col("Fund Description").isNotNull()).sort("Amount", ascending=False).show(truncate = False)


# In[24]:


row1 = df.agg({"Amount": "mean"}).collect()[0]
print(row1)
mean_amt = float(row1["avg(Amount)"])
print(mean_amt)


# In[25]:


state_df = (df
.select("State")
.where(col("State").isNotNull())
.where("State like 'N%'")
.where(length("State")<=3)
.drop_duplicates()
.sort("State"))

state_df.show(truncate = False)


# In[26]:


def func(col1, col2):
    if col1 == 'NSW' or col2 == 'NY':
        return 1
    return None

func_udf = udf(func, IntegerType())

state_df = (state_df
            .withColumn("expr_if", expr("IF(State=='NH' OR State=='NL', 1, NULL)"))
            .withColumn("when_col", when((col("State") =='NC') | (col("State") =='NE'), 1).otherwise(None))
            .withColumn("func_udf", func_udf(col("State"), col("State")))
            .withColumn('coalesce', coalesce(col("expr_if"), col("when_col"), col("func_udf"), lit(0))))

state_df.show(truncate = False)


# In[27]:


df.withColumn("row_number", row_number().over(Window.partitionBy("Account").orderBy(col("Amount").desc()))).where(col("row_number")==1) .drop("row_number").select("Account", "Amount").show(truncate = False)


# In[28]:


df[col('Department').isin(['Labor', 'Education', 'Children and Families'])].select("Department").distinct().show(truncate = False)


# In[29]:


df2 = (df
.groupBy("State")
.count()
.sort("count", ascending=False))

df2.show(truncate = False)


# In[30]:


df2.withColumn("State", when(col("State") == "VT","VT")
                    .when(col("State") == "MA","MA")
                    .otherwise("Unknown"))\
.show(10, truncate=False)


# In[31]:


df2.withColumn("State aggregate", expr("case when State = 'VT' then 'VT' " + 
                           "when State = 'MA' then 'MA' " +
                           "else 'Unknown' end"))\
.show(10, truncate=False)


# In[32]:


df.select("Amount").distinct().fillna({'Amount': 0}).show(10)


# In[33]:


df.select("Department").distinct().count()


# In[34]:


df.selectExpr('count(distinct(Department))').show(truncate=False)


# In[35]:


# I will start by creating a temporary table
df.createOrReplaceTempView('VermontVendor')
# spark.catalog.dropTempView('VermontVendor')

# run query with SQL
sql = """
SELECT `Quarter Ending`, Department, Amount, State FROM VermontVendor
LIMIT 10
"""

spark.sql(sql).show(truncate = False)


spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("SHOW TABLES").show(truncate=False)
spark.sql('DESCRIBE vermontvendor').show(truncate=False)

spark.sql("CREATE DATABASE vermontvendor_db")
spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("USE vermontvendor_db")

# save df to a new table in Hive
df.write.format("hive").mode("overwrite").saveAsTable("vermontvendor_db.vermontvendor")
spark.sql("CREATE TABLE vermontvendor_db.vermontvendor2 AS SELECT * FROM vermontvendor")
spark.sql("SHOW TABLES").show(truncate=False)

spark.sql ("DROP TABLE IF EXISTS vermontvendor_db.vermontvendor3")

sql = """
CREATE TABLE IF NOT EXISTS vermontvendor_db.vermontvendor3
(`Quarter Ending` date, Department string, UnitNo integer, `Vendor Number` string, Vendor string, City string, State string, `DeptID Description` string, DeptID string,
Amount double, Account string, AcctNo string, `Fund Description` string, Fund string)
COMMENT 'A list of all State of Vermont payments to vendors'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1')
"""
spark.sql(sql)
spark.sql("LOAD DATA LOCAL INPATH 'Vermont_Vendor_Payments.csv' OVERWRITE INTO TABLE vermontvendor_db.vermontvendor3")

spark.sql("SELECT count(*) FROM vermontvendor").show(truncate=False)
spark.sql("SELECT count(*) FROM vermontvendor_db.vermontvendor").show(truncate=False)
spark.sql("SELECT count(*) FROM vermontvendor_db.vermontvendor2").show(truncate=False)
spark.sql("SELECT count(*) FROM vermontvendor_db.vermontvendor3").show(truncate=False)


# In[36]:


def parametrize_sql_query(sql_path, sql_query_name, steering_dict):
    #function parametrize given query from sql directory using steering dictionary.
    #Each key from dictionary in sql is replaced with value from dictionary.
    with open(sql_path+sql_query_name+".sql") as sql_file:
        sql_query = sql_file.read()
    if bool(steering_dict):
        for key in steering_dict.iterkeys():
            sql_query=sql_query.replace(key, steering_dict[key])
    return sql_query


# In[37]:


sql = parametrize_sql_query("", "my_select", dict())
print(sql)


# In[38]:


spark.sql(sql).limit(10).show(truncate = False)


# In[39]:


sql = """
SELECT count(distinct `Vendor Number`) as `Unique Vendor Numbers`
, count(1) as `Total rows`
FROM VermontVendor
"""

spark.sql(sql).show(truncate = False)


# In[40]:


sql = """SELECT `Quarter Ending`, Department, Amount, State FROM VermontVendor 
WHERE Department = 'Education'
LIMIT 10"""

spark.sql(sql).show(truncate = False)


# In[41]:


df.filter(col('Amount') >= 0).filter(col('Department')=='Education').select('Quarter Ending', 'Department', 'Amount', 'State').show(truncate = False)


# In[42]:


df.groupBy('Department').agg(f_sum('Amount').alias('Total Amount'),     f_min('Amount').alias('Min. Amount'),     f_max('Amount').alias('Max. Amount')).show(truncate = False)


# In[43]:


df.groupBy('Department').count().sort('count', ascending=False).withColumnRenamed('count','Number of rows').show(truncate = False)


# In[44]:


df.groupBy('Department').sum('Amount').withColumnRenamed('sum(Amount)','Sum of Amount').show(truncate = False)


# In[45]:


df.filter(df.Department == 'Education').groupBy('Department').sum('Amount').withColumnRenamed('sum(Amount)','Sum of Amount').show(truncate = False)


# In[46]:


df.filter(df.Department != 'Education').groupBy('Department').sum('Amount').withColumnRenamed('sum(Amount)','Sum of Amount').show(truncate = False)


# In[47]:


df.groupBy('Department').sum('Amount').withColumnRenamed('sum(Amount)','Sum of Amount').withColumn('Amount in millions', col('Sum of Amount')/1000000).show(10, truncate = False)


# In[48]:


a = (df
     .filter(("Department = 'Education' or Department = 'Labor'"))
     .select('Department'))
b = (df
     .filter(("Department = 'Education' or Department = 'Corrections'"))
     .select('Department'))

a.intersect(b).distinct().show(truncate = False)


# In[49]:


a.unionAll(b).distinct().show(truncate = False)


# In[50]:


a.exceptAll(b).distinct().show(truncate = False)


# In[51]:


top5 = (df
        .groupBy("Department")
        .agg(f_sum('Amount').alias('Sum of Amount'))
        .sort('Sum of Amount', ascending=False)
        .limit(5))


# In[52]:


top5.toPandas().head()


# In[53]:


sql = """SELECT Department, SUM(Amount) as Total FROM VermontVendor 
GROUP BY Department
ORDER BY Total DESC
LIMIT 10"""

plot_df = spark.sql(sql).toPandas()

fig,ax = plt.subplots(1,1,figsize=(10,6))
plot_df.plot(x = 'Department', y = 'Total', kind = 'barh', color = 'C0', ax = ax, legend = False)
ax.set_xlabel('Department', size = 16)
ax.set_ylabel('Total', size = 16)
plt.savefig('barplot.png')
plt.show()


# In[54]:


import numpy as np
import seaborn as sns

sql = """SELECT Department, SUM(Amount) as Total FROM VermontVendor 
GROUP BY Department"""

plot_df = spark.sql(sql).toPandas()

plt.figure(figsize = (10,6))
sns.distplot(np.log(plot_df['Total']))
plt.title('Histogram of Log Totals for all Departments in Dataset', size = 16)
plt.ylabel('Density', size = 16)
plt.xlabel('Log Total', size = 16)
plt.savefig('distplot.png')
plt.show()


# In[55]:


sql = parametrize_sql_query("", "my_select", dict())
print(sql)

df = spark.sql(sql).limit(10)
df.show(truncate = False)


# In[56]:


df.repartition(1)\
    .write.format('csv')\
    .option('header', True)\
    .mode('overwrite')\
    .option('sep',',')\
    .save('df_output.csv')

