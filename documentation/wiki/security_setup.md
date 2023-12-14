| Entra ID Groups            	| Azure Cloud     	| Data Factory    	| Metadata SQL DB 	| Databricks                                 	| Azure DevOps    	|
|----------------------------	|-----------------	|-----------------	|-----------------	|--------------------------------------------	|-----------------	|
| az_edw_admins              	| No Restrictions 	| No Restrictions 	| No Restrictions 	| No Restrictions                            	| No Restrictions 	|
| az_edw_data_scientists     	| No Access       	| No Access       	| No Access       	| Can Use: SQL, Repos, ML                    	| Can Use         	|
| az_edw_devops_engineers    	| No Access       	| No Access       	| No Access       	| No Access                                  	| Can Manage      	|
| az_edw_data_architects     	| No Restrictions 	| No Restrictions 	| No Restrictions 	| No Restrictions                            	| Can Use         	|
| az_edw_data_analysts       	| No Access       	| No Access       	| No Access       	| Can Use: SQL                               	| No Access       	|
| az_edw_ml_engineers        	| No Access       	| No Access       	| No Access       	| Can Use: Repos, ML<br>Can Manage: Clusters 	| Can Use         	|
| az_edw_data_engineers      	| Can Read        	| No Restrictions 	| No Restrictions 	| Can Use: SQL, Repos<br>Can Manage:Clusters 	| Can Use         	|
| az_edw_alerts_data_factory 	| No Access       	| No Access       	| No Access       	| No Access                                  	| No Access       	|
| az_edw_alerts_data_factory 	| No Access       	| No Access       	| No Access       	| No Access                                  	| No Access       	|
| az_edw_alerts_databricks   	| No Access       	| No Access       	| No Access       	| No Access                                  	| No Access       	|