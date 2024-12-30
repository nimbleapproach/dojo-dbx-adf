# 🎯 Master Data Management (MDM) Overview

## Summary 
The MDM is a POC, which is created to showcase the Dimensional Modelling standards and include means of Mastering the Source Data
1. [📥 Stage Data](#staging-process)
   - Standardises SKU and vendor data
   - Maps local SKUs to manufacturer SKUs
   - Multiple match passes (exact & fuzzy)
   - Hold multiple occurences in Data Quality tables
   - Setup prioty columns to Master records based on their priority

2. [🔄 Modelling framework](#modelling-framework-demo)
   - Configure the model to process an MDM Entity
   - Create target table
   - Run job

## Key Features
- Mastering Entities by Priority Order 
- Fuzzy matching with configurable thresholds
- Quality control checks


# 📥 Stage Data {#staging-process}
📄nb-orion-common.py (data prep)

The notebook above holds 2 main mastering functions, `map_parent_child_keys`, `check_duplicate_keys`. the first `map_parent_child_keys` is used to prioritise multiple occurences and help rank them based on their priority. 

```python
def map_parent_child_keys(
    df, 
    key_cols=["Manufacturer Item No_", "Consolidated Vendor Name", "sys_databasename"], 
    order_cols=None, 
    priority_col="sys_databasename",
    priority_map = {
        "ReportsUK": 1,
        "ReportsDE": 2,
        "ReportsCH": 3
        .......
    }
):
```

:::mermaid
graph TD
    A[Raw Sales Data] --> B[Priotise composite records]
    B --> C[stage priority entity]
    B --> D[store multiple occurence in dq table]
    C --> E[process entity through Meta Framework]
:::

## Key Processing Steps

1. **Master records based on their priority**
- create a specific notebook to master the entity into a stage table
- use the priotiy function to help master the multiple occurences
- for example the vendor table will be called `dim_vendor_stg`
  - this will be paired with a master vendor table, called `dim_master_vendor_stg`
    - the master vendor record will be dedtermined by the last node vendor in the `dim_vendor_stg` table

2. **Meta Framework**
- configured dim_vendor_master & dim_vendor staged tables will be process into their target table 
- loads data into the `dim_vendor` table for the `dim_vendor_stg`
- the meta framework handles Type 2 Data 
:::mermaid
graph TD
    A[dim_vendor_stg] --> B[dim_vendor]
:::

## 📑 File Dependencies
- nb-orion-common (utilities)
- meta.json (metadata)
- nb-orion-process-fact-model.py (run entire model)
