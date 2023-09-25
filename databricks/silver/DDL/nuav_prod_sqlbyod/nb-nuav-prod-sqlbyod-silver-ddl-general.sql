-- Databricks notebook source
-- DBTITLE 1,Define Sales Credit Memo Header at Silver
-- MAGIC %md
-- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
-- MAGIC If there is no widget defined, Data Factory will automatically create them.
-- MAGIC For us while developing we can use the try and except trick here.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     TABLE_NAME = dbutils.widgets.get("wg_tableName")
-- MAGIC except:
-- MAGIC     dbutils.widgets.text(name = "wg_tableName", defaultValue = 'AZIENDA')
-- MAGIC     TABLE_NAME = dbutils.widgets.get("wg_tableName")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Read schema from excel
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC storage_account_name = "adls0euw0powerbi"
-- MAGIC storage_account_access_key = "Wubwy6BinrjwRkPPrUXmOWwNneUvoJ1DsKhjLZjlvxYB5gq2E+5sO31cp099NasnzFIacZCMMtEP+AStqfvAaw=="
-- MAGIC
-- MAGIC file_location = "wasbs://upload@"+storage_account_name+".blob.core.windows.net/silver_schema_definition"
-- MAGIC file_type = "csv"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
-- MAGIC   storage_account_access_key)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_schema =  spark.read.format("csv").option('inferSchema','true').option("header", "true").option("delimiter", ";").load(file_location)
-- MAGIC df_sub = df_schema.filter(df_schema.TableName == TABLE_NAME).select("ColumnName", "DataType","NotNull",'BusinessKey','Comment')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC ls = []
-- MAGIC for i in df_sub.collect():
-- MAGIC     ls.append(f"`{i['ColumnName'].replace(' ','')}` {i['DataType']}")
-- MAGIC     if i['NotNull']==1:
-- MAGIC         ls.append(f" NOT NULL COMMENT '{i['Comment']}',")
-- MAGIC     else:
-- MAGIC         ls.append(f" COMMENT '{i['Comment']}',")
-- MAGIC     text =''.join(ls)

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC
-- MAGIC business_key_list = (', '.join([i['ColumnName'] 
-- MAGIC                                 for i in df_schema
-- MAGIC                                 .filter((df_schema.TableName == TABLE_NAME) 
-- MAGIC                                                           & (df_schema.BusinessKey == 1) )
-- MAGIC                                 .select("ColumnName").collect()])
-- MAGIC                      )
-- MAGIC
-- MAGIC sql = (f"CREATE OR REPLACE TABLE {TABLE_NAME}(" + text +
-- MAGIC        f"Sys_RowNumber BIGINT NOT NULL\
-- MAGIC       COMMENT 'Globally unqiue Number in the source database to capture changes. Was calculated by casting the timestamp column to integer.'\
-- MAGIC     ,Sys_DatabaseName STRING NOT NULL\
-- MAGIC       COMMENT 'Name of the Source Database.'\
-- MAGIC     ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP\
-- MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'\
-- MAGIC     ,Sys_Silver_InsertDateTime_UTC TIMESTAMP\
-- MAGIC       DEFAULT current_timestamp()\
-- MAGIC       COMMENT 'The timestamp when this entry landed in silver.'\
-- MAGIC     ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP\
-- MAGIC       DEFAULT current_timestamp()\
-- MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'\
-- MAGIC     ,Sys_Silver_HashKey BIGINT NOT NULL\
-- MAGIC       COMMENT 'HashKey over all but Sys columns.'\
-- MAGIC     ,CONSTRAINT {TABLE_NAME}_pk PRIMARY KEY( "+business_key_list+", Sys_DatabaseName, Sys_RowNumber))"   
-- MAGIC        )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA igsql03;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(sql)
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC
-- MAGIC dateConstraint = ['Bronze_InsertDateTime' ,'Silver_InsertDateTime' ,'Silver_ModifedDateTime']
-- MAGIC
-- MAGIC
-- MAGIC for i in dateConstraint:
-- MAGIC     sql_constraint = f"ALTER TABLE {TABLE_NAME} ADD CONSTRAINT dateWithinRange_{i} CHECK (Sys_{i}_UTC > '1900-01-01');"
-- MAGIC     spark.sql(sql_constraint)

-- COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_dev")
[i[0] for i in spark.catalog.listTables("nuav_prod_sqlbyod")]
