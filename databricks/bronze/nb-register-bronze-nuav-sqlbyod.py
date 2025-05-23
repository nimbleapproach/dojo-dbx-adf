# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

try:
    REREGISTER = bool(dbutils.widgets.get("wg_reregister") == 'true')
except:
    dbutils.widgets.dropdown(name = "wg_reregister", defaultValue = 'false', choices =  ['false','true'])
    REREGISTER = bool(dbutils.widgets.get("wg_reregister") == 'true')

# COMMAND ----------

ROOT_PATH = 'nuaz-sqlserver-01'

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

def registerTable(rootPath : str, tableName : str,schemaName : str,databaseName: str, recreate : bool = False):
    tableName_uc = f"{schemaName}_{tableName}"
    
    # added this to remap the new name of prodtran.. the "s" has been dropped from the database. and we want to continue using the NUAV_PRODTRANS_SQLBYOD DB
    databaseNameTemp = databaseName.replace('NUAV_PRODTRAN_SQLBYOD', 'NUAV_PRODTRANS_SQLBYOD')
    print(f"using schema {databaseNameTemp}")
    spark.sql(f"""
            use schema {databaseNameTemp}
            """)

    if recreate:
        spark.sql(f"""
                  DROP TABLE IF EXISTS {tableName_uc}
                  """)
    
    if not spark.catalog.tableExists(tableName_uc):
        spark.sql(f"""
        Create table {tableName_uc}
        using DELTA
        Location 'abfss://bronze@adls0ig0{ENVIRONMENT}0westeurope.dfs.core.windows.net/{rootPath}/{databaseName}/{schemaName}/{tableName}/'
        """)

# COMMAND ----------

# only running against the NUAV_PROD_SQLBYOD database

for databaseName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/'):
    str_databaseName = databaseName.name[:-1]
    if (str_databaseName == 'NUAV_PROD_SQLBYOD'):
        for schemaName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/{databaseName.name[:-1]}'):
            for tableName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/{databaseName.name[:-1]}/{schemaName.name[:-1]}'):
                print(f"STARTING: Registering table '{schemaName.name[:-1]}_{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{databaseName.name[:-1]}' .")

                registerTable(rootPath = ROOT_PATH, tableName = tableName.name[:-1],schemaName=schemaName.name[:-1],databaseName=databaseName.name[:-1], recreate=REREGISTER)

                print(f"FINISHED: Registering table '{schemaName.name[:-1]}_{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{databaseName.name[:-1]}' .")
    else:
        print(f"Skipping {str_databaseName}")

# COMMAND ----------

# only running against the NUAV_PRODTRAN_SQLBYOD database

for databaseName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/'):
    str_databaseName = databaseName.name[:-1]
    if (str_databaseName == 'NUAV_PRODTRAN_SQLBYOD'):
        for schemaName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/{databaseName.name[:-1]}'):
            for tableName in dbutils.fs.ls(f'mnt/bronze/nuaz-sqlserver-01/{databaseName.name[:-1]}/{schemaName.name[:-1]}'):
                print(f"STARTING: Registering table '{schemaName.name[:-1]}_{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{databaseName.name[:-1]}' .")

                registerTable(rootPath = ROOT_PATH, tableName = tableName.name[:-1],schemaName=schemaName.name[:-1],databaseName=databaseName.name[:-1], recreate=REREGISTER)

                print(f"FINISHED: Registering table '{schemaName.name[:-1]}_{tableName.name[:-1]}' to catalog 'bronze_{ENVIRONMENT}' in schema '{databaseName.name[:-1]}' .")
    else:
        print(f"Skipping {str_databaseName}")
