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

ar_trans     = f"{catalog}.{schema_ar}.ar_cust_full_visit_history"
ar_store_trans     = f"{catalog}.{schema_ar}.ar_cust_rewards_store_visits"
reward_visits     = f"{catalog}.{schema_ar}.ar_cust_rewards_visits"
ar_cust      = f"{catalog}.{schema_ar}.stg_01_cust_attrib_extended"

# COMMAND ----------


query = f""" 
-- drop table if exists {schema_ar}.stg_01_cust_attrib;

CREATE OR REPLACE TABLE {schema_ar}.stg_01_cust_attrib (
    wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , spid STRING COMMENT 'Single Profile View Id'
  , sp_match_xref STRING  COMMENT 'Match to xref by spid'
  , sp_match_smv STRING  COMMENT 'Match to Master by spid'
  , iw_match_smv STRING COMMENT 'Match to Master by wallet_id within instore visits'
  , ow_match_smv STRING COMMENT 'Match to Master by wallet_id within online visits'
  , acc_type  STRING COMMENT 'identify type of account. we determine this based on how their details were added to the system'
  
  , regtn_ts  timestamp COMMENT 'The time of the registration'
) USING DELTA
    COMMENT "This table contains prestaged Asda Reward Customer details, also identifies if a master Id is available"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;""" 
spark.sql(query)
 

# COMMAND ----------


query = f""" 
--ar_visits (custprod.dsa_mart.ar_cust_rewards_visits)

-- drop table if exists {reward_visits};

CREATE OR REPLACE TABLE {reward_visits} ( 
    wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , rn_wallet int  COMMENT 'order of time wallet was used'
  , cn_wallet int COMMENT 'no of time store count of wallet'
  , rn_full_wallet int  COMMENT 'all visits of wallets ordered by time'
  , cn_full_wallet int COMMENT 'count of all visits of wallets ordered by time'
  , chnl_nm STRING COMMENT 'sata source online|instore'
  , order_id BIGINT COMMENT 'The unique identifier of an order'
  , basket_id DECIMAL(38,0) COMMENT 'The unique identifier of an order'
  
  , till_type int COMMENT 'till type'
  , self_check_out_ind int COMMENT 'self checkout indicator'
  , colleague_ind int COMMENT 'colleague at till'
  , sng_cart_id STRING COMMENT 'sng cart id'
  , channel_id int COMMENT 'count no of times wallet was used instore'
  
  , asda_wk_nbr bigint COMMENT 'The asda week number of the transation'
  , xref STRING COMMENT 'The card complete an transaction  AKA customer_ref_number'
  , tc_nbr STRING COMMENT 'Transaction Number'
  , trans_rcpt_nbr  STRING COMMENT 'The transaction receipt number used id dupes'
  -- , store_id int COMMENT 'The unique identifier of a store'
  , store_nbr int COMMENT 'The unique identifier of a store Number'
  , reg_nbr int COMMENT 'The unique identifier of a Register Number'
  , visit_nbr int COMMENT 'The unique identifier of a visit'
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
    COMMENT "This table contains Asda Reward visits combnining instore and online"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;""" 
spark.sql(query)


# COMMAND ----------

query = f""" 
-- drop table if exists {ar_cust};

CREATE OR REPLACE TABLE {ar_cust} ( 
    wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , spid STRING COMMENT 'The unique identifier of an account'
  , acc_type  STRING COMMENT 'identify type of account. we determine this based on how their details were added to the system'
  
  , sp_match_xref STRING  COMMENT 'Match to xref by spid'
  , sp_match_smv STRING  COMMENT 'Match to Master by spid'
  , iow_match_smv STRING COMMENT 'Match to Master by wallet_id instore & online visits '
  , regtn_ts  timestamp COMMENT 'The time of the registration'
  , first_visit_rn_wallet int  COMMENT 'order of time wallet was used'
  , first_visit_cn_wallet int COMMENT 'no of time store count of wallet'
  , first_visit_rn_full_wallet int  COMMENT 'all visits of wallets ordered by time'
  , first_visit_cn_full_wallet int COMMENT 'count of all visits of wallets ordered by time'
  , first_visit_chnl_nm STRING COMMENT 'data source online|instore'  
  , first_visit_order_id BIGINT COMMENT 'The unique identifier of an order'
  , first_visit_basket_id DECIMAL(38,0) COMMENT 'The unique identifier of an order'
  , first_visit_xref STRING COMMENT 'The card complete an transaction  AKA customer_ref_number'
  , first_visit_tc_nbr STRING COMMENT 'Transaction Number'
  , first_visit_trans_rcpt_nbr  STRING COMMENT 'The transaction receipt number used id dupes'
  , first_visit_store_nbr int COMMENT 'The unique identifier of a store Number'
  , first_visit_nbr int COMMENT 'The unique identifier of a visit'
  , first_visit_event_ts timestamp COMMENT 'The time of the event'
  , first_visit_ts timestamp COMMENT 'The time of the visit'
  , first_visit_dt date COMMENT 'The date of the visit'
  , first_visit_unified_cust_id STRING COMMENT 'The unified customer id'
  , first_visit_sale_amt_inc_vat decimal(9,2) COMMENT 'The sale amount on the transaction'
  , first_visit_sale_amt_exc_vat decimal(9,2) COMMENT 'The sale amount on the transaction exc VAT'
  , first_visit_sales_asda decimal(9,2) COMMENT 'The discount applied amount on the transaction' 
  , first_visit_item_qty  int  COMMENT 'number of items in the basket'
  , first_visit_unmeasured_qty  decimal(32,2)  COMMENT 'number of unmeasured items in the basket'


) USING DELTA
    COMMENT "This table contains Asda Reward Customer details"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;""" 
spark.sql(query)


# COMMAND ----------



query = f""" 
CREATE OR REPLACE TABLE {ar_trans} ( 
    --visit_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1) COMMENT "Datamart Generated Unique identifier for a visit"
    wallet_id int COMMENT 'The unique identifier of a AR Wallet Account'
  , spid STRING  COMMENT 'The unique identifier of an account'
  , acc_type  STRING COMMENT 'identify type of account. we determine this based on how their details were added to the system'
  , spid_match_type STRING COMMENT 'matching the visit to a spid'
  , card_flag  STRING COMMENT 'matching the visit to a spid'
  , first_visit_dt date COMMENT 'The date of the first visit'
  , first_visit_ts  timestamp COMMENT 'The datetime of the first visit'
  , first_visit_chnl_nm STRING COMMENT 'data source online|instore of the first visit'
  , first_visit_nbr int COMMENT  'The unique identifier of a visit'
  , ar_visit_flag SMALLINT COMMENT 'was it an AR visit a visit without the AR card'
  , sp_match_xref STRING  COMMENT 'Match to xref by spid'
  , sp_match_smv STRING  COMMENT 'Match to Master by spid'
  , iow_match_smv STRING COMMENT 'Match to Master by wallet_id instore & online visits '
  
  , rn_wallet int  COMMENT 'order of time wallet was used'
  , cn_wallet int COMMENT 'no of time store count of wallet'
  , rn_full_wallet int  COMMENT 'all visits of wallets ordered by time'
  , cn_full_wallet int COMMENT 'count of all visits of wallets ordered by time'
  , chnl_nm STRING COMMENT 'data source online|instore'
  , order_id BIGINT COMMENT 'The unique identifier of an order'
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
    COMMENT "This table contains both online & instore visits level transactions for asda reward customers"
  --TBLPROPERTIES (delta.enableChangeDataFeed=true)
;""" 
spark.sql(query)

