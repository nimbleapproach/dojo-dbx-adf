# Orion
Orion the the Name of our Facts and Dimension Data Product sitting in the Gold and preceeding layers.

The main goal of Orion is to substitute OBT which serves as company wide data foundation for reporting purposes.

The reasons for Orion are:
1) Having a proper data model to reduce development time
2) Reduce complexity
3) Allow implementation of advanced techniques such as SCD2 and RLS (if really needed)
4) Increase the performance and reduce the time of PowerBI Imports and Direct Queries as well as the need of Power Query.

Our current Orion target picture contains the following tables:

- facts

    1) fact_revenue

- dimensions

    2) dim_products

We start by using only Starlink (*silver.netsuite*) data and populating the *dim_products* table first.

## dim_products
Currently we have the *silver.datanowarr* table and the domain specific tables (e.g. *netsuite.masterdatasku* in starlinks case) which contain all the needed informations.

### Key Considerations
1) We need to use the datanow data if possbile. This can be done by joining on the SKU_ID, Vendor_Name column from that table. In particular these two columsn form a natural key.

    - **Problems**: Can we trust those columns? Can there be typos or can a Vendor_Name change/deleted? What if there is no match for a SKU? Is there always a Other Vendors Entry for all SKUs we can then use?
    - **Considerations**: We need to be able to update als products easily whenever a new Excel Sheet arrives.
    - **Solution**: SCD2 and Surrogate Keys.

2) We want to use a Surrogate Key as reference to the facts tables to handle changes and fixes which is done by using so called **key tables** which replaces the natural key with a technical one (typically a bigint). This table should never be truncated. In addition to that we are using **historic key tables** which handles the SCD2 behavior of the dimension table.
For a more detailed explaination i refer here to the following medium article: 
[Slowly Changing Dimensions with Dynamic Tables](https://medium.com/snowflake/slowly-changing-dimensions-with-dynamic-tables-d0d76582ff31)

## Schema

| Column Name                 	| Data Type     	| Unique         	| Nullable       	| is Primary Key   	| Hierarchies    	|
|----------------------------	|-----------------	|-----------------	|-----------------	|------------------	|-----------------	|
| ProductSID                	| BIGINT         	| Yes 	            | No            	| Yes              	|                	|
| ProductHSID                  	| BIGINT        	| Yes               | No 	            | No        	    |                   | 
| SKU                        	| STRING        	| Yes            	| No            	| No               	|               	|
| Description                  	| STRING          	| No              	| No            	| No               	|                   |
| ProductType               	| STRING 	        | No            	| No  	            | No               	|                   |
| CommitmentDuration1       	| STRING           	| No               	| No            	| No            	|                  	|
| CommitmentDuration2        	| STRING         	| No            	| No            	| No            	|                  	|
| BillingFrequency          	| STRING           	| No  	            | No            	| No            	|                  	|
| ConsumptionModel          	| STRING         	| No            	| No             	| No            	|                  	|
| Sys_Gold_InsertDateTime_UTC 	| TIMESTAMP       	| No            	| No             	| No              	|                  	|
| Sys_Gold_ModifedDateTime_UTC  | TIMESTAMP       	| No            	| No               	| No              	|                  	|
| Sys_Gold_StartDateTime_UTC   	| TIMESTAMP       	| No            	| No            	| No               	|                  	|
| Sys_Gold_EndDateTime_UTC   	| TIMESTAMP       	| No            	| Yes           	| No               	|                  	|