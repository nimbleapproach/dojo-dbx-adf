# Databricks notebook source
# MAGIC %md
# MAGIC # create_visit_tables

# COMMAND ----------

dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

dbutils.widgets.text('schema_ar', 'dojo_db1')
schema_ar = dbutils.widgets.get('schema_ar')

# schema_ar = dbutils.jobs.taskValues.get(
#     taskKey="task_start", key="schema_ar", debugValue="dojo_db1"
# )
# COMMAND ----------
spark.sql(f"set catalog {catalog}")
# COMMAND ----------
spark.sql("SELECT current_catalog()").show()
spark.sql("SELECT current_schema()").show()

# COMMAND ----------
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user")
print("Code is running under user:", user)

# COMMAND ----------

ar_trans     = f"{catalog}.{schema_ar}.ar_cust_full_visit_history"
ar_store_trans     = f"{catalog}.{schema_ar}.ar_cust_rewards_store_visits"

# COMMAND ----------


query = f""" 
-- drop table if exists {catalog}.{schema_ar}.stg_01_instore_wallet_master;

CREATE OR REPLACE TABLE {schema_ar}.stg_01_instore_wallet_master (
  
    wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  --, order_id string  COMMENT 'Match made to smv member master'
  --, store_id STRING COMMENT 'Link to the master Id'
  , match_smv STRING COMMENT 'Match to Master by wallet_id'
) USING DELTA
    COMMENT "This table identifies wallet used instore, also identifies if a master Id is available"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;"""
spark.sql(query)


# COMMAND ----------


query = f""" 
--drop table if exists {catalog}.{schema_ar}.stg_02_instore_master_details;

CREATE OR REPLACE TABLE {schema_ar}.stg_02_instore_master_details (
  
  rn_postxn int COMMENT 'order of visit dupes which are created by the trans_rcpt_nbr'
  , cn_postxn  int COMMENT  'count no of visit dupes created by the trans_rcpt_nbr'
  ,  wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , rn_wallet int  COMMENT 'order of wallet relative to the time used within the visit'
  , cn_wallet int COMMENT 'count no of times wallet was used instore'
  , match_smv STRING COMMENT 'Match to Master by wallet_id'
  , visit_nbr BIGINT COMMENT 'The unique identifier of an order'
  -- , order_id BIGINT COMMENT 'The unique identifier of an order'
  -- , xref STRING COMMENT 'The card complete an transaction  AKA customer_ref_number'
  , trans_rcpt_nbr  STRING COMMENT 'The transaction receipt number used id dupes'
  -- , store_id int COMMENT 'The unique identifier of a store'
  , store_nbr int COMMENT 'The unique identifier of a store Number'
  , event_ts timestamp COMMENT 'The time of the transaction'
  , visit_dt date COMMENT 'The date of the visit'
  --, unified_cust_id STRING COMMENT 'The unified customer id'
) USING DELTA
    COMMENT "This table identifies visit level instore dupelicate transactions, also carries over master Id of the transaction"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;"""
spark.sql(query)


# COMMAND ----------

query = f""" 
--drop table if exists {catalog}.{ar_store_trans};

CREATE OR REPLACE TABLE {ar_store_trans} (
  
    wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , rn_wallet int  COMMENT 'order of wallet relative to the time used within the visit'
  , cn_wallet int COMMENT 'count no of times wallet was used instore'
  , match_smv STRING COMMENT 'Match to Master by wallet_id'
  -- , order_id BIGINT COMMENT 'The unique identifier of an order'
  , basket_id DECIMAL(38,0) COMMENT 'The unique identifier of an order'
  
  , till_type int COMMENT 'till type'
  , self_check_out_ind int COMMENT 'self checkout indicator'
  , colleague_ind int COMMENT 'colleague at till'
  , sng_cart_id STRING COMMENT 'sng cart id'
  , channel_id int COMMENT 'count no of times wallet was used instore'
  
  , xref STRING COMMENT 'The card complete an transaction  AKA customer_ref_number'
  , tc_nbr STRING COMMENT 'Transaction Number'
  , trans_rcpt_nbr  STRING COMMENT 'The transaction receipt number used id dupes'
  -- , store_id int COMMENT 'The unique identifier of a store'
  , store_nbr int COMMENT 'The unique identifier of a store Number'
  , reg_nbr int COMMENT 'The unique identifier of a Register Number'
  , visit_nbr int COMMENT 'The unique identifier of a visit'
  , asda_wk_nbr bigint COMMENT 'The asda week number of the transation'
  , event_ts timestamp COMMENT 'The time of the event'
  , visit_ts timestamp COMMENT 'The time of the visit'
  , visit_dt date COMMENT 'The date of the visit'
  , unified_cust_id STRING COMMENT 'The unified customer id'
  , sale_amt_inc_vat decimal(9,2) COMMENT 'The sale amount on the transaction'
  , sale_amt_exc_vat decimal(9,2) COMMENT 'The sale amount on the transaction exc VAT'
  , sales_asda decimal(9,2) COMMENT 'The discount applied amount on the transaction' 
  , item_qty  int  COMMENT 'number of items in the basket'
  , unmeasured_qty  decimal(32,2)  COMMENT 'number of unmeasured items in the basket'

) USING DELTA
    COMMENT "This table contains visit level instore transactions"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;"""
spark.sql(query)


# COMMAND ----------


query = f""" 
--drop table if exists {catalog}.{ar_store_trans}_qurantine;

CREATE OR REPLACE TABLE {ar_store_trans}_quarantine (
  
   rn_postxn int COMMENT 'order of visit dupes which are created by the trans_rcpt_nbr'
  , cn_postxn  int COMMENT 'count no of visit dupes created by the trans_rcpt_nbr'
  , wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , rn_wallet int  COMMENT 'order of wallet relative to the time used within the visit'
  , cn_wallet int COMMENT 'count no of times wallet was used instore'
  , match_smv STRING COMMENT 'Match to Master by wallet_id'
  -- , order_id BIGINT COMMENT 'The unique identifier of an order'
  , basket_id DECIMAL(38,0) COMMENT 'The unique identifier of an order'
  
  , till_type int COMMENT 'till type'
  , self_check_out_ind int COMMENT 'self checkout indicator'
  , colleague_ind int COMMENT 'colleague at till'
  , sng_cart_id STRING COMMENT 'sng cart id'
  , channel_id int COMMENT 'count no of times wallet was used instore'

  , xref STRING COMMENT 'The card complete an transaction  AKA customer_ref_number'
  , tc_nbr STRING COMMENT 'Transaction Number'
  , trans_rcpt_nbr  STRING COMMENT 'The transaction receipt number used id dupes'
  -- , store_id int COMMENT 'The unique identifier of a store'
  , store_nbr int COMMENT 'The unique identifier of a store Number'
  , reg_nbr int COMMENT 'The unique identifier of a Register Number'
  , visit_nbr int COMMENT 'The unique identifier of a visit'
  , asda_wk_nbr bigint COMMENT 'The asda week number of the transation'
  , event_ts timestamp COMMENT 'The time of the event'
  , visit_ts timestamp COMMENT 'The time of the visit'
  , visit_dt date COMMENT 'The date of the visit'
  , unified_cust_id STRING COMMENT 'The unified customer id'
  , sale_amt_inc_vat decimal(9,2) COMMENT 'The sale amount on the transaction'
  , sale_amt_exc_vat decimal(9,2) COMMENT 'The sale amount on the transaction exc VAT'
  , sales_asda decimal(9,2) COMMENT 'The discount applied amount on the transaction' 
  , item_qty  int  COMMENT 'number of items in the basket'
  , unmeasured_qty  decimal(32,2)  COMMENT 'number of unmeasured items in the basket'

) USING DELTA
    COMMENT "This table contains visit level instore transactions where dupes are found"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;"""
spark.sql(query)

