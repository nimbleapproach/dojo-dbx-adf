# Databricks notebook source
from datetime import datetime, timezone
import os

import delta
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, expr, lit

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
NULL = "NaN"

# COMMAND ----------

silver_ig_crm_db = f"silver_{ENVIRONMENT}.igsql01"
rslr_tbl ="tbl_reseller"
silver_acc_tbl ="accountbase"

reseller = (spark.table(f"{silver_ig_crm_db}.{rslr_tbl}")
                 .select([
                            "reseller",
                            "reseller_code",
                            "sys_database_name",
                            "sys_silver_is_current",
                            "all_names"
                        ]))
account = spark.table(f"{silver_ig_crm_db}.{silver_acc_tbl}")

# bronze
bronze_acc_tbl_full = f"bronze_{ENVIRONMENT}.igsql01.accountbase"
bronze_account = spark.table(bronze_acc_tbl_full)

# COMMAND ----------

def _get_false_true_vals(df: DataFrame) -> tuple[int|float]:
    """Utility function to retrieve values from bool breakdown table.

    Expects dataframe of two columns and max two rows. The first
     column is bool and second numeric. The numeric value is retrieved,
     or, if not present, default of 0 is supplied. 

    Args:
        df (DataFrame): input 2 columns dataframe

    Returns:
        tuple[int|float]: tuple of: false value, true value
    """
    _rows = df.collect()
    _false_rows = [row[1] for row in _rows if row[0] == False]
    false_val = _false_rows[0] if _false_rows else 0
    _true_rows = [row[1] for row in _rows if row[0] == True]
    true_val = _true_rows[0] if _true_rows else 0
    return false_val, true_val

def _get_single_val(df: DataFrame):
    """Retrieve the first row first cell value from filtered df.
    """
    return df.first()[0] if df.count() > 0 else None

# COMMAND ----------

# bronze nulls breakdown
bronze_nulls_breakdown = (
    bronze_account.withColumn("is_null", col("inf_customerno").isNull())
          .groupBy("is_null")
          .agg((F.count("*")).alias("count"))
)
bronze_not_nulls, bronze_nulls = _get_false_true_vals(bronze_nulls_breakdown)
bronze_not_nulls, bronze_nulls

# COMMAND ----------

# silver stage 1
account_nulls_breakdown = (
    account.where("Sys_Silver_IsCurrent")
           .withColumn("is_null", col("inf_customerno") == lit(NULL))
           .groupBy("is_null")
           .agg((F.count("*")).alias("count"))
)
account_not_nulls, account_nulls = _get_false_true_vals(account_nulls_breakdown)
account_not_nulls, account_nulls

# COMMAND ----------

# silver stage 2
reseller_nulls_breakdown = (
    reseller.where("sys_silver_is_current")
            .withColumn("is_null", col("reseller_code") == lit(NULL))
            .groupBy("is_null")
            .agg((F.count("*")).alias("count"))
)
reseller_not_nulls, reseller_nulls = _get_false_true_vals(reseller_nulls_breakdown)
reseller_not_nulls, reseller_nulls

# COMMAND ----------

bronze_dups_df = (
    bronze_account.where(col("inf_customerno") != lit(NULL))
                  .select("Sys_DatabaseName", "inf_customerno")
                  .groupBy("Sys_DatabaseName", "inf_customerno")
                  .agg((F.count("*") - lit(1)).alias("dups_ct"))
                  .select(F.sum(col("dups_ct")).alias("br_acc_dups"))
)
bronze_dups_count = _get_single_val(bronze_dups_df)
bronze_dups_count

# COMMAND ----------

# silver account dups
account_dups_df = (
    account.where("Sys_Silver_IsCurrent")
           .where(col("inf_customerno") != lit(NULL))
           .select("Sys_DatabaseName", "inf_customerno")
           .groupBy("Sys_DatabaseName", "inf_customerno")
           .agg((F.count("*") - lit(1)).alias("dups_ct"))
           .select(F.sum(col("dups_ct")).alias("acc_dups"))
)
account_dups_count = _get_single_val(account_dups_df)
account_dups_count

# COMMAND ----------

# silver stage 2 dups
reseller_dups_df = (
    reseller.where("sys_silver_is_current")
            .select("sys_database_name", "reseller_code")
            .groupBy("sys_database_name", "reseller_code")
            .agg((F.count("*") - lit(1)).alias("dups_ct"))
            .select(F.sum(col("dups_ct")).alias("rsr_dups"))
)
reseller_dups_count = _get_single_val(reseller_dups_df)
reseller_dups_count

# COMMAND ----------

# MAGIC %md
# MAGIC #### measures in conjunction with ERP

# COMMAND ----------

# sources

# limit transactions to window of past year
transactions = (
  spark.table(f"platinum_{ENVIRONMENT}.obt.globaltransactions")
       .select([
                     "GroupEntityCode",
                     "EntityCode",
                     "TransactionDate",
                     "ResellerCode",
                     "ResellerNameInternal",
                     "ResellerGeographyInternal",
                     "ResellerStartDate",
                     "ResellerGroupCode",
                     "RevenueAmount",
                     "RevenueAmount_Euro",
              ])
       .where("TransactionDate >= (current_date - make_interval(1))")
)

entity_mapping = spark.table(f"gold_{ENVIRONMENT}.obt.entity_mapping")

# COMMAND ----------

joined = (
    transactions.join(entity_mapping,
                 transactions.EntityCode == entity_mapping.TagetikEntityCode
                )
            .join(reseller,
                  (transactions.ResellerCode == reseller.reseller_code)
                  &
                  (F.right(reseller.sys_database_name, lit(2)) == entity_mapping.SourceEntityCode),
                  how="left"
            )
)

reseller_aggregated = (
    joined.where("GroupEntityCode = 'IG'")
          .withColumn("in_ig_crm", col("reseller_code").isNotNull())
          .groupBy("ResellerCode", "EntityCode", "in_ig_crm")
          .agg(
              F.count("*").alias("transaction_ct"), # TODO
              F.sum("RevenueAmount").alias("total_revenue")
          )
)

comp_breakdown = (
    reseller_aggregated.withColumn("non_zero_revenue", col("total_revenue") != lit(0))
                       .groupBy("in_ig_crm", "non_zero_revenue")
                       .agg(F.count("*").alias("reseller_ct"),
                            F.sum("total_revenue").alias("total_revenue")                           )
)

# COMMAND ----------

# retrieve values from df
comp_breakdown.cache()

in_crm = comp_breakdown.where("in_ig_crm")
in_crm_with_rev = in_crm.where("non_zero_revenue")
in_crm_without_rev = in_crm.where("NOT non_zero_revenue")

oo_crm = comp_breakdown.where("NOT in_ig_crm")
oo_crm_with_rev = oo_crm.where("non_zero_revenue")
oo_crm_without_rev = oo_crm.where("NOT non_zero_revenue")

# counts

# The number of resellers in CRM with revenue
crm_rev_resellers = _get_single_val(in_crm_with_rev.select("reseller_ct"))

# The number of resellers in CRM with no revenue recorded
crm_no_rev_resellers = _get_single_val(in_crm_without_rev.select("reseller_ct"))

# The number of resellers in ERP with revenue
not_crm_rev_resellers = _get_single_val(oo_crm_with_rev.select("reseller_ct"))
erp_rev_resellers = crm_rev_resellers + not_crm_rev_resellers
# The number of resellers in ERP with no revenue recorded
not_crm_no_rev_resellers = _get_single_val(oo_crm_without_rev.select("reseller_ct"))
erp_no_rev_resellers = crm_no_rev_resellers + not_crm_no_rev_resellers

# totals
# Total revenue from resellers in CRM
crm_resellers_rev = _get_single_val(in_crm_with_rev.select("total_revenue"))
#  Total revenue from resellers in ERP
not_crm_resellers_rev = _get_single_val(oo_crm_with_rev.select("total_revenue"))
erp_resellers_rev = crm_resellers_rev + not_crm_resellers_rev

comp_breakdown.unpersist()

print(
    crm_rev_resellers,
    crm_no_rev_resellers,
    erp_rev_resellers,
    erp_no_rev_resellers,
    crm_resellers_rev,
    erp_resellers_rev
)

# COMMAND ----------

name_mismatch_count = (
 joined.where(col("ResellerNameInternal") != col("reseller"))
       .select("ResellerCode", "EntityCode", "ResellerNameInternal", "reseller")
       .distinct()
       .count()
)
name_mismatch_count

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the row to dq table

# COMMAND ----------

TARGET_TABLE = f"silver_{ENVIRONMENT}.dq.crm_reseller_dq"
IG_ENTITY = "IG"

as_of_ts = datetime.now(timezone.utc)
as_of_ts

# COMMAND ----------

target_data = [(
        IG_ENTITY,
        as_of_ts.date(),
        as_of_ts,

        reseller_not_nulls + reseller_nulls,
        reseller_nulls,
        reseller_dups_count,

        bronze_not_nulls + bronze_nulls,
        bronze_nulls,
        bronze_dups_count,

        account_not_nulls + account_nulls,
        account_nulls,
        account_dups_count,

        crm_rev_resellers,
        crm_no_rev_resellers,
        erp_rev_resellers,
        erp_no_rev_resellers,

        int(crm_resellers_rev),
        int(erp_resellers_rev),

        name_mismatch_count,
),]

schema = """
    entity_group: string,
    as_of_date: date,
    as_of_ts: timestamp,

    reseller_total_count: long,
    reseller_nulls_count: long,
    reseller_dups_count: long,

    bronze_total_count: long,
    bronze_nulls_count: long,
    bronze_dups_count: long,

    silver_total_count: long,
    silver_nulls_count: long,
    silver_dups_count: long,

    crm_rev_resellers_count: long,
    crm_no_rev_resellers_count: long,
    all_rev_resellers_count: long,
    all_no_rev_resellers_count: long,
    crm_resellers_revenue: long,
    all_resellers_revenue: long,

    name_mismatch_count: long
"""

target_df = spark.createDataFrame(target_data, schema)

# COMMAND ----------

# append a row (and create table if not exists)
target_df.write.mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE)

# COMMAND ----------

# keep only one set per day
# once the row was written, remove any previous rows for the same date
dq_dt = delta.DeltaTable.forName(spark, TARGET_TABLE)
dq_dt.delete((col('entity_group') == lit(IG_ENTITY))
             & (col('as_of_date') == lit(as_of_ts.date()))
             & (col('as_of_ts') < lit(as_of_ts)))
