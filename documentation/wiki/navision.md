## Context

Navision, a precursor to MS Dynamics, is an on premise ERP system that is utilised by Infinigate. It contains various data sets around purchasing, sales & customers that is key to understanding the financial performance of the organisation.  

## Technical Overview
![Navision](../assets/images/architecture-navision.png =900x)

## Components

- Reporting DBs - Reporting databases at a country level that hold country specific Navision data
- Self hosted IR - A Self Hosted Azure Data Factory Integrated Runtime, required to acquire data from the on premise DBs
- Azure hosted SQL DB - Contains metadata to support acquisition pipelines for this and other systems
- Azure Data Factory - Orchestration and processing provider for acquisition processes
- Data Lake - File based storage system, using a heirarchichal namespace, where raw data is initiall stored as parquet and then trandformed using databricks


The current acqusition processes utilises Azure Data Factory, using both Azure hosted and Self hosted Integration Runtimes, to pull data from the metadata & source tables using two pipelines (Available under Bronze > Navision, within ADF):

Prior to running the below pipelines, users must update the CT_IGSQL03_TABLES metadata table in the metadata database. This can be done by updating the table creation script (https://dev.azure.com/InfinigateHolding/Group%20IT%20Program/_git/inf-sqldb-ig-metadata?path=/deployment/scripts/InitSQLScriptLibrary/1.%20Init%20Scripts/10.%20CT_IGSQL03_TABLES.sql) to add the required databases, tables and watermarks (0 if first time load) and rerunning the script on the database by following the instructions within the repo.  
If a new table has been added via the above process, users must also manually trigger the PL_00_BRONZE_REGISTER_TABLES_MASTER pipeline, under the bronze folder in ADF. This will launch a databricks notebook (nb-register-bronze) that will create a new table under the associated schema.

### PL_10_IGSQL03_DB_Runner
This pipeline is designed to connect to the metadata database and collect an object of country specific databases that data should be pulled from. For each of these DBs, the PL_20_IGSQL03_Table_Runner pipeline is then ran sequentially.

### PL_20_IGSQL03_Table_Runner

This pipeline is formed of the following stages:
- Using the country DB name to filter rows, provided by the PL_10_IGSQL03_DB_Runner pipeline, an object of tables to pull data from, and their associated watermarks, is collected from the metadata database

This object is then iterated over with the following steps, with up to 10 runs occuring in parallel
- A row count is calculated, using the current watermark, to determine if new data is available
- If no new data is available, the process ends
- If new data is available, the latest watermark and a list of columns (from the information schema) are pulled from the associated database
- Data is then pulled from the table, using the current watermark to pull only new / updated data, and saved as a parquet file in the datalake
- The watermark value in the metadata table is then updated to the latest value and a logging entry added to the logging table in the same database


## Tables

|Table  | Purpose |
|--|--|
|Company Information| |  
|Cust_ Ledger Entry| |  
|Customer| |  
|Customer _ Cust_ Disc_ Group| |  
|Customer Discount Group| |  
|Default Dimension| |  
|Detailed Cust_ Ledg_ Entry| |  
|Dimension Set Entry| |  
|Dimension Value| |  
|G_L Account| |  
|G_L Entry| |  
|General Ledger Setup| |  
|INF MSP Usage Header| |  
|INF MSP Usage Line| |  
|Item| |  
|Item Ledger Entry| |  
|Job| |  
|Payment Terms| |  
|Purch_ Inv_ Header| |  
|Purch_ Inv_ Line| |  
|Purch_ Rcpt_ Header| |  
|Purch_ Rcpt_ Line| |  
|Purchase Header| |  
|Purchase Header Archive| |  
|Purchase Line| |  
|Purchase Line Archive| |  
|Purchase Price| |  
|Resource| |  
|Sales Cr_Memo Header| |  
|Sales Cr_Memo Line| |  
|Sales Header| |  
|Sales Header Archive| |  
|Sales Invoice Header| |  
|Sales Invoice Line| |  
|Sales Ledger Entry| |  
|Sales Line| |  
|Sales Line Archive| |  
|Sales Shipment Header| |  
|Sales Shipment Line| |  
|Value Entry| |  
|Vendor| | 

 
## Cleansing & Transformations

Silver:  


Gold:  