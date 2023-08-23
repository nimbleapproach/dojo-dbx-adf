# Databricks notebook source
try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
except:
    dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices =  ['dev','uat','prod'])
    ENVIRONMENT = dbutils.widgets.get("wg_environment")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema NUAV_PRODTRANS_SQLBYOD;

# COMMAND ----------

def registerTable(tableName : str,schemaName : str, recreate : bool = False):
    cleanedTableName = tableName.replace(" ", "")
    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {schemaName}_{cleanedTableName}
                  """)
    if not spark.catalog.tableExists(f'{schemaName}_{cleanedTableName}'):
        df = spark.read.load(f'/mnt/bronze/nuaz-sqlserver-01/NUAV_PRODTRANS_SQLBYOD/{schemaName}/{tableName}/')
        schema = df.schema
        spark.catalog.createTable(tableName = f'{schemaName}_{cleanedTableName}',
                                path=f"abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/nuaz-sqlserver-01/NUAV_PRODTRANS_SQLBYOD/{schemaName}/{tableName}/",
                                source='delta',
                                schema=schema)

# COMMAND ----------

for schemaName in dbutils.fs.ls('mnt/bronze/nuaz-sqlserver-01/NUAV_PRODTRANS_SQLBYOD'):
    for tableName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/NUAV_PRODTRANS_SQLBYOD/{schemaName.name[:-1]}'):
        print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'NUAV_PRODTRANS_SQLBYOD'.")
        registerTable(tableName.name[:-1],schemaName.name[:-1],recreate = True)
        print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'NUAV_PRODTRANS_SQLBYOD'.")

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema NUAV_PROD_SQLBYOD;

# COMMAND ----------

def registerTable(tableName : str,schemaName : str, recreate : bool = False):
    cleanedTableName = tableName.replace(" ", "")
    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {schemaName}_{cleanedTableName}
                  """)
    if not spark.catalog.tableExists(f'{schemaName}_{cleanedTableName}'):
        df = spark.read.load(f'/mnt/bronze/nuaz-sqlserver-01/NUAV_PROD_SQLBYOD/{schemaName}/{tableName}/')
        schema = df.schema
        spark.catalog.createTable(tableName = f'{schemaName}_{cleanedTableName}',
                                path=f"abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/nuaz-sqlserver-01/NUAV_PROD_SQLBYOD/{schemaName}/{tableName}/",
                                source='delta',
                                schema=schema)

# COMMAND ----------

for schemaName in dbutils.fs.ls('mnt/bronze/nuaz-sqlserver-01/NUAV_PROD_SQLBYOD'):
    for tableName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/NUAV_PROD_SQLBYOD/{schemaName.name[:-1]}'):
        print(f"STARTING: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'NUAV_PROD_SQLBYOD'.")
        registerTable(tableName.name[:-1],schemaName.name[:-1],recreate = True)
        print(f"FINISHED: Registering table '{tableName.name[:-1]}' to catalog 'bronze' in schema 'NUAV_PROD_SQLBYOD'.")
