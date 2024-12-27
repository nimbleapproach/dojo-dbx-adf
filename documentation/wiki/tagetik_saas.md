## Context

Tagetik is a corporate software product used for monitors and managing financial performance and processes, such as budgeting, consolidation, planning and forecasting, disclosure management, and reporting. This system is teh core financial performance system for IG, and is used as a benchmark to compare other financial analysis to. The data is pulled into the lakehouse in order to reconcile other financial calculations and ensure they reconcile against the numbers approved by the finance team.   

## Technical Components

- Tagetik SASS - A SAAS instance of the tagetik software, holding data for various financial scenarios and years
- Self hosted IR - A Self Hosted Azure Data Factory Integrated Runtime, required to acquire data from the SASS instance
- Azure hosted SQL DB - Contains metadata to support acquisition pipelines for this and other systems
- Azure Data Factory - Orchestration and processing provider for acquisition processes
- Data Lake - File based storage system, using a heirarchichal namespace, where raw data is initiall stored as parquet and then trandformed using databricks

The current acqusition processes utilises Azure Data Factory, using both Azure hosted and Self hosted Integration Runtimes, to pull data from the metadata & source tables using two pipelines (Available under Bronze > Tagetik > SaaS, within ADF):

Prior to running the below pipelines, users must update the CT_TAG_SAAS_TABLES metadata table in the metadata database. 
This can be done by updating the table creation script (https://dev.azure.com/InfinigateHolding/Group%20IT%20Program/_git/inf-sqldb-ig-metadata?path=/deployment/scripts/InitSQLScriptLibrary/1.%20Init%20Scripts/18.%20CT_TAG_SAAS_TABLES.sql) to add the required codes, tables and full load param (1 if dim, 0 for fact) and rerunning the script on the database by following the instructions within the repo.  

If a new table has been added via the above process, users must also manually trigger the PL_00_BRONZE_REGISTER_TABLES_MASTER pipeline, under the bronze folder in ADF. This will launch a databricks notebook (nb-register-bronze) that will create a new table under the associated schema.

## Delta Load Pipeline

### PL_10_TAG_Saas_DeltaLoad_MASTER
This pipeline first creates a timestamp for midnight on the previous day, to use in the API request in order to only pull back data that has been updated since then. Next it collects the associated metadata from the SQL DB, and loops over this running an additional pipeline. 

### PL_20_TAG_Saas_DeltaLoad

This pipeline collects the data from the saas instance and saves into the datalake. It utilises a data copy activity and a pre configured dataset, which does not involve a scenario parameter. 
As such, this feature needs to be disabled in the underlying tagetik API before this pipeline can be ran for the fact tables.

## Full Load Pipeline

### PL_10_TAG_Saas_FullLoad_MASTER
This pipeline collects the associated metadata from the SQL DB, and loops over this running an additional pipeline. 

### PL_20_TAG_Saas_FullLoad
This pipeline checks the associated metadata and performs an activity depending on the value.  
If the full_load value is set to 1, it runs a copy activity that utilises the associated DIM_FULL dataset, in order to pull a full copy of the associated table without a scenario code as part of the request (only required for fact tables).  
If the full_load value is set to 0, it runs the PL_30_TAG_Saas_FullLoad_Fact pipeline.  

### PL_30_TAG_Saas_FullLoad_Fact
The piepline takes in an array of manually provided scenario values and loops over them, each time performing a copy activity against a specified tagetik fact table for that scenario. This is performed as the full fact tables are too large to pull in a single request, and so these need to be broken down to a more managable level.  

## Outline process

- Check tagetik staging datasets allow the scenario parameter for the two fact tables (dati_rett_riga & dati_saldi_lordi)
- Run the PL_10_TAG_Saas_FullLoad_MASTER pipeline to pull a full copy of tagetik data
- Update tagetik staging datasets for fact tables to remove requirement for a scenario parameter
- Make sure the PL_10_TAG_Saas_DeltaLoad_MASTER is part of the schedule nightly load process
- Check new data matches expectations