# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

!pip install openpyxl --quiet

# COMMAND ----------

class MountHandle:
    def __init__(self):
        self.configs = {"fs.azure.account.auth.type": "OAuth",
                        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                        "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="kv-ig-westeurope",key='secret-databricks-app-id'),
                        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="kv-ig-westeurope",key='secret-databricks-app-value'),
                        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/375313bf-8b9b-44af-97cd-fc2e258a968e/oauth2/token"}
        
    def mount(self,container : str):
        if any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
                print("{}".format(container) + " from {}-ADLS already mounted to".format(container) + " DBFS at /mnt/{}".format(container) +".")
        else:
                dbutils.fs.mount(
                source = f"abfss://{container}@dv0ig0dev0westeurope.dfs.core.windows.net/",
                mount_point = "/mnt/{}".format(container),
                extra_configs = self.configs)
        
    def unmount(self,container :str):
        if any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount("/mnt/{}".format(container))
        else:
            print("DBFS at /mnt/{}".format(container) + " is not mounted.")

# COMMAND ----------

mountHandler = MountHandle()
mountHandler.mount("external")

# COMMAND ----------

dbutils.fs.ls("mnt/external/nuvias_journal_temp/")

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

# COMMAND ----------

file_path = f"/dbfs/mnt/external/nuvias_journal_temp/NU D365 Adjustments.xlsx"
# pd.read_excel(file_path)

# COMMAND ----------

df = pd.read_excel(file_path)
df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
# dfs = ps.from_pandas(df)
dfs = spark.createDataFrame(df)


# COMMAND ----------

dfs.createOrReplaceTempView('temp')

# COMMAND ----------

spark.sql("""
with cte as (
SELECT
`SalesOrderID` as SalesOrderID,
 `Voucher` AS Voucher,
 `TransactionDate` as TransactionDate,
 `Year closed` as YearClosed,
 `Ledger account` as LedgAccount,
 `Account name` as AccountName,
 `Description` as Description,
 `CurrencyCode` as CurrencyCode,
 `Amount Local` as AmountLCY,
 `Amount GBP` AS AmountGBP, 
 `Posting type` AS PostingType,
 `Posting layer` as PostingLayer,
 `ResellerCode` as ResellerCode,
 `ResellerNameInternal` as ResellerNameInternal,
 `EntityID` as EntityID
 from temp
          )
 select
CASE WHEN EntityID = 'Austria'	THEN 'AT2'
    WHEN EntityID = 'France'THEN  'FR3'
    WHEN EntityID = 'Germany'	THEN'DE4'
    WHEN EntityID = 'Netherlands'	THEN'NL2'
    WHEN EntityID = 'Poland'	THEN'PL1'
    WHEN EntityID = 'Spain'THEN	'ES1'
    WHEN EntityID = 'Switzerland'THEN'CH3'
    WHEN EntityID = 'UK' THEN'UK3'
    ELSE EntityID END AS EntityID,
    TransactionDate,
  SalesOrderID,
  Voucher,
  ResellerCode,
  ResellerNameInternal,
   case when charindex('VAC', RIGHT (LedgAccount, LEN(LedgAccount) - charindex('VAC', LedgAccount)-2))>0 
       then   RIGHT(
                       RIGHT (LedgAccount, LEN(LedgAccount) - charindex('VAC', LedgAccount)-2),
                       LEN(RIGHT (LedgAccount, LEN(LedgAccount) - charindex('VAC', LedgAccount)-2)) - charindex('VAC', RIGHT (LedgAccount, LEN(LedgAccount) - charindex('VAC', LedgAccount)-2)) + 1
                   )
   else 
           LEFT(
           RIGHT(
               LedgAccount,
               LEN(LedgAccount) - charindex('VAC', LedgAccount) + 1
           ),
           9
           )
   end AS VendorCode,
  case when AccountName in ('Sales','Customer rebates') then 'Sales' else 'Cost of Sales' end AS AccountName,
  CurrencyCode,
  AmountLCY * (-1) AS AmountLCY
from
  cte
where lower(PostingType) in ('customer revenue','ledger journal')
  """).createOrReplaceTempView('NU_JOURNAL')

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE gold_{ENVIRONMENT}.obt.nuvias_journal AS

SELECT 
EntityID,
SalesOrderID,
TransactionDate,
Voucher,
ResellerCode,
ResellerNameInternal,
VendorCode,
coalesce(Sales,0)Sales,
coalesce(Cost,0)Cost,
CurrencyCode

 FROM NU_JOURNAL
    PIVOT (
        SUM(AmountLCY) AS AmountLCY
        FOR AccountName IN ('Sales' AS Sales, 'Cost of Sales' AS Cost)
    )
    """)
