## Overview

This page outlines the various manual data loads that are currently required as part of the data platform. These loads should be carried out when data is available, although timeframes can be inconsistent from month to month, depending on when financial data has been closed down.


## IG Cloud ERP (Vuzion)

**Format:**  
XLSX File  
**Frequency & Date:**  
Monthly - ~ 10 days post end of month  
**Environments to promote through:**  
Dev, UAT, Prod  
**Source Location:**  
IG Sharepoint - 15 - Infinigate Cloud Data (Check Access)  [LINK](https://infinigate.sharepoint.com/sites/NuviasVendorOperations/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FNuviasVendorOperations%2FShared%20Documents%2FVendor%20Operations%20Team%2FReporting%2FProjects%2FActivity%20Dashboard%2F1%2E%20Source%20Data%2F15%2E%20Infinigate%20Cloud%20Data)  
**ADLS Location:**  
dv0ig0[ENV]0westeurope / external / vuzion / monthly_export / pending  
**ADF Pipeline:**  
Bronze - Vuzion - Monthly  
**Databricks Jobs:**  
vuzion-bronze-to-silver, silver-to-gold-obt  
**Process:**  
* Download XLSX file from sharepoint site
* Open file and save current month sheet into a new file, named FY24 Vuzion Revenue FY24 budget margin.xlsx - Number following FY should match current FY
* Navigate to the correct ADLS folder, if a file is already present, download it, delete it and then add new file
* If a previous file has been downloaded, upload this to the archived folder
* Run ADF Pipeline, via debug mode
* Run associated databricks jobs
* Repeat for remaining environments

## IG Cloud Adjustments (Vuzion)

**Format:**  
XLSX File  
**Frequency & Date:**  
Monthly - ~ 10 days post end of month  
**Environments to promote through:**  
Dev, UAT, Prod  
**Source Location:**  
IG Sharepoint - 15 - Infinigate Cloud Data (Check Access)  [LINK](https://infinigate.sharepoint.com/sites/NuviasVendorOperations/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FNuviasVendorOperations%2FShared%20Documents%2FVendor%20Operations%20Team%2FReporting%2FProjects%2FActivity%20Dashboard%2F1%2E%20Source%20Data%2F15%2E%20Infinigate%20Cloud%20Data)  
**ADLS Location:**  
dv0ig0[ENV]0westeurope / external / vuzion / monthly_revenue_adjustment / pending  
**ADF Pipeline:**  
Bronze - Vuzion - Monthly_adj
**Databricks Jobs:**  
vuzion-bronze-to-silver, silver-to-gold-obt  
**Process:**  
* Download XLSX file from sharepoint site
* Navigate to the correct ADLS folder, download existing file and then delete from folder
* Upload a copy of same file to archived folder in ADLS directory
* Open new file and save with filename IGCloud-MonthlyAdjustments.xlsx
* Navigate to the correct ADLS folder and add new file
* Run ADF Pipeline, via debug mode
* Run associated databricks jobs
* Repeat for remaining environments

## Nuvias D365 Adjustments

**Format:**  
XLSX File  
**Frequency & Date:**  
Monthly - ~ 1 day post end of month  
**Environments to promote through:**  
Dev, UAT, Prod  
**Source Location:**  
Karl Naughton or Alroy Lopes to provide  
**ADLS Location:**  
dv0ig0[ENV]0westeurope / external / nuvias_journal_temp  
**ADF Pipeline:**  
No ADF pipeline  
**Databricks Jobs:**  
nb-obt-gold-nuvias-journal (Notebook), silver-to-gold-obt  
**Process:**  
* Download XLSX file provided
* Navigate to the correct ADLS folder, download existing file and then delete from folder
* Upload a copy of same file to archived folder in ADLS directory
* Open both files, append new data onto previous data
* Save as new file with filename NU D365 Adjustments.xlsx
* Navigate to the correct ADLS folder and add new file
* Run associated databricks notebook & job
* Repeat last step for remaining environments (Data is only present in dev, uat and prod are connected to this)

## Netsafe

**Format:**  
XLSX File  
**Frequency & Date:**  
Monthly - ~ 5/6 day post end of month  
**Environments to promote through:**  
Dev, UAT, Prod  
**Source Location:**  
Alroy Lopes to provide  
**ADLS Location:**  
dv0ig0[ENV]0westeurope / external / netsafe / pending  
**ADF Pipeline:**  
Bronze - Netsafe  
**Databricks Jobs:**  
netsafe-bronze-to-silver, silver-to-gold-obt  
**Process:**  
* Download XLSX files provided (4 in total, 1 per country)
* Navigate to the correct ADLS folder, download existing files and then delete from folder
* Upload a copy of same files to archived folder in ADLS directory
* Save new files into pending folder
* Run ADF Pipeline, via debug mode
* Run associated databricks jobs
* Repeat for remaining environments

## ARR

**Format:**  
XLSX File  
**Frequency & Date:**  
Monthly - ~ 10 days post end of month  
**Environments to promote through:**  
Dev, UAT, Prod  
**Source Location:**  
Karl Naughton to provide  
**ADLS Location:**  
dv0ig0[ENV]0westeurope / external / datanow / arr / latest  
**ADF Pipeline:**  
Bronze - DATANOW - Latest  
**Databricks Jobs:**  
masterdata-bronze-to-silver, silver-to-gold-obt  
**Process:**  
* Download XLSX file provided
* Open file and ensure a single tab has been provided, rename this tab to database if it is not named this. If there are multiple tabs, check with data provider.  
* Navigate to the correct ADLS folder, download existing file and then delete from folder
* Upload a copy of same file to old folder in ADLS directory
* Save new file into latest folder
* Run ADF Pipeline, via debug mode
* Run associated databricks jobs
* Repeat for remaining environments