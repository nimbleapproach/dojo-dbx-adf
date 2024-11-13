# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA obt;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE nace_2_codes
# MAGIC   ( section STRING
# MAGIC       COMMENT 'Section Code - Most General Level'
# MAGIC     ,division STRING
# MAGIC       COMMENT 'Division Code - Second Most General Level'
# MAGIC     ,activity_description STRING
# MAGIC       COMMENT 'Original description'
# MAGIC     ,display_text STRING
# MAGIC       COMMENT 'Text to be used as display'
# MAGIC   )
# MAGIC COMMENT 'This table provides simplified structure of the industry verticals specified by NACE 2 codes and mapping to group descriptions to the displayed.'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Populate the relevant meta data...**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO nace_2_codes (section, division, activity_description, display_text)
# MAGIC VALUES
# MAGIC   ("A", NULL, "Agriculture, Forestry And Fishing", "Agriculture"),
# MAGIC   ("A", "01", "Crop and animal production, hunting and related service activities", NULL),
# MAGIC   ("A", "02", "Forestry and logging", NULL),
# MAGIC   ("A", "03", "Fishing and aquaculture", NULL),
# MAGIC   ("B", NULL, "Mining And Quarrying", "Mining"),
# MAGIC   ("B", "05", "Mining of coal and lignite", NULL),
# MAGIC   ("B", "06", "Extraction of crude petroleum and natural gas", NULL),
# MAGIC   ("B", "07", "Mining of metal ores", NULL),
# MAGIC   ("B", "08", "Other mining and quarrying", NULL),
# MAGIC   ("B", "09", "Mining support service activities", NULL),
# MAGIC   ("C", NULL, "Manufacturing", "Manufacturing"),
# MAGIC   ("C", "10", "Manufacture of food products", NULL),
# MAGIC   ("C", "11", "Manufacture of beverages", NULL),
# MAGIC   ("C", "12", "Manufacture of tobacco products", NULL),
# MAGIC   ("C", "13", "Manufacture of textiles", NULL),
# MAGIC   ("C", "14", "Manufacture of wearing apparel", NULL),
# MAGIC   ("C", "15", "Manufacture of leather and related products", NULL),
# MAGIC   ("C", "16", "Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials", NULL),
# MAGIC   ("C", "17", "Manufacture of paper and paper products", NULL),
# MAGIC   ("C", "18", "Printing and reproduction of recorded media", NULL),
# MAGIC   ("C", "19", "Manufacture of coke and refined petroleum products", NULL),
# MAGIC   ("C", "20", "Manufacture of chemicals and chemical products", NULL),
# MAGIC   ("C", "21", "Manufacture of basic pharmaceutical products and pharmaceutical preparations", NULL),
# MAGIC   ("C", "22", "Manufacture of rubber and plastic products", NULL),
# MAGIC   ("C", "23", "Manufacture of other non-metallic mineral products", NULL),
# MAGIC   ("C", "24", "Manufacture of basic metals", NULL),
# MAGIC   ("C", "25", "Manufacture of fabricated metal products, except machinery and equipment", NULL),
# MAGIC   ("C", "26", "Manufacture of computer, electronic and optical products", NULL),
# MAGIC   ("C", "27", "Manufacture of electrical equipment", NULL),
# MAGIC   ("C", "28", "Manufacture of machinery and equipment n.e.c.", NULL),
# MAGIC   ("C", "29", "Manufacture of motor vehicles, trailers and semi-trailers", NULL),
# MAGIC   ("C", "30", "Manufacture of other transport equipment", NULL),
# MAGIC   ("C", "31", "Manufacture of furniture", NULL),
# MAGIC   ("C", "32", "Other manufacturing", NULL),
# MAGIC   ("C", "33", "Repair and installation of machinery and equipment", NULL),
# MAGIC   ("D", NULL, "Electricity, Gas, Steam And Air Conditioning Supply", "Electricity, Gas, Steam And Air Conditioning Supply"),
# MAGIC   ("D", "35", "Electricity, gas, steam and air conditioning supply", NULL),
# MAGIC   ("E", NULL, "Water Supply; Sewerage, Waste Management And Remediation Activities", "Water Supply; Sewerage, Waste Management And Remediation Activities"),
# MAGIC   ("E", "36", "Water collection, treatment and supply", NULL),
# MAGIC   ("E", "37", "Sewerage", NULL),
# MAGIC   ("E", "38", "Waste collection, treatment and disposal activities; materials recovery", NULL),
# MAGIC   ("E", "39", "Remediation activities and other waste management services", NULL),
# MAGIC   ("F", NULL, "Construction", "Construction"),
# MAGIC   ("F", "41", "Construction of buildings", NULL),
# MAGIC   ("F", "42", "Civil engineering", NULL),
# MAGIC   ("F", "43", "Specialised construction activities", NULL),
# MAGIC   ("G", NULL, "Wholesale And Retail Trade; Repair Of Motor Vehicles And Motorcycles", "Wholesale And Retail Trade; Repair Of Motor Vehicles And Motorcycles"),
# MAGIC   ("G", "45", "Wholesale and retail trade and repair of motor vehicles and motorcycles", NULL),
# MAGIC   ("G", "46", "Wholesale trade, except of motor vehicles and motorcycles", NULL),
# MAGIC   ("G", "47", "Retail trade, except of motor vehicles and motorcycles", NULL),
# MAGIC   ("H", NULL, "Transportation And Storage", "Transportation And Storage"),
# MAGIC   ("H", "49", "Land transport and transport via pipelines", NULL),
# MAGIC   ("H", "50", "Water transport", NULL),
# MAGIC   ("H", "51", "Air transport", NULL),
# MAGIC   ("H", "52", "Warehousing and support activities for transportation", NULL),
# MAGIC   ("H", "53", "Postal and courier activities", NULL),
# MAGIC   ("I", NULL, "Accommodation And Food Service Activities", "Accommodation And Food Service Activities"),
# MAGIC   ("I", "55", "Accommodation", NULL),
# MAGIC   ("I", "56", "Food and beverage service activities", NULL),
# MAGIC   ("J", NULL, "Information And Communication", "Information And Communication"),
# MAGIC   ("J", "58", "Publishing activities", NULL),
# MAGIC   ("J", "59", "Motion picture, video and television programme production, sound recording and music publishing activities", NULL),
# MAGIC   ("J", "60", "Programming and broadcasting activities", NULL),
# MAGIC   ("J", "61", "Telecommunications", NULL),
# MAGIC   ("J", "62", "Computer programming, consultancy and related activities", NULL),
# MAGIC   ("J", "63", "Information service activities", NULL),
# MAGIC   ("K", NULL, "Financial And Insurance Activities", "Financial And Insurance Activities"),
# MAGIC   ("K", "64", "Financial service activities, except insurance and pension funding", NULL),
# MAGIC   ("K", "65", "Insurance, reinsurance and pension funding, except compulsory social security", NULL),
# MAGIC   ("K", "66", "Activities auxiliary to financial services and insurance activities", NULL),
# MAGIC   ("L", NULL, "Real Estate Activities", "Real Estate Activities"),
# MAGIC   ("L", "68", "Real estate activities", NULL),
# MAGIC   ("M", NULL, "Professional, Scientific And Technical Activities", "Professional, Scientific And Technical Activities"),
# MAGIC   ("M", "69", "Legal and accounting activities", NULL),
# MAGIC   ("M", "70", "Activities of head offices; management consultancy activities", NULL),
# MAGIC   ("M", "71", "Architectural and engineering activities; technical testing and analysis", NULL),
# MAGIC   ("M", "72", "Scientific research and development", NULL),
# MAGIC   ("M", "73", "Advertising and market research", NULL),
# MAGIC   ("M", "74", "Other professional, scientific and technical activities", NULL),
# MAGIC   ("M", "75", "Veterinary activities", NULL),
# MAGIC   ("N", NULL, "Administrative And Support Service Activities", "Administrative And Support Service Activities"),
# MAGIC   ("N", "77", "Rental and leasing activities", NULL),
# MAGIC   ("N", "78", "Employment activities", NULL),
# MAGIC   ("N", "79", "Travel agency, tour operator reservation service and related activities", NULL),
# MAGIC   ("N", "80", "Security and investigation activities", NULL),
# MAGIC   ("N", "81", "Services to buildings and landscape activities", NULL),
# MAGIC   ("N", "82", "Office administrative, office support and other business support activities", NULL),
# MAGIC   ("O", NULL, "Public Administration And Defence; Compulsory Social Security", "Public Administration And Defence"),
# MAGIC   ("O", "84", "Public administration and defence; compulsory social security", NULL),
# MAGIC   ("P", NULL, "Education", "Education"),
# MAGIC   ("P", "85", "Education", NULL),
# MAGIC   ("Q", NULL, "Human Health And Social Work Activities", "Human Health And Social Work Activities"),
# MAGIC   ("Q", "86", "Human health activities", NULL),
# MAGIC   ("Q", "87", "Residential care activities", NULL),
# MAGIC   ("Q", "88", "Social work activities without accommodation", NULL),
# MAGIC   ("R", NULL, "Arts, Entertainment And Recreation", "Arts, Entertainment And Recreation"),
# MAGIC   ("R", "90", "Creative, arts and entertainment activities", NULL),
# MAGIC   ("R", "91", "Libraries, archives, museums and other cultural activities", NULL),
# MAGIC   ("R", "92", "Gambling and betting activities", NULL),
# MAGIC   ("R", "93", "Sports activities and amusement and recreation activities", NULL),
# MAGIC   ("S", NULL, "Other Service Activities", "Other Service Activities"),
# MAGIC   ("S", "94", "Activities of membership organisations", NULL),
# MAGIC   ("S", "95", "Repair of computers and personal and household goods", NULL),
# MAGIC   ("S", "96", "Other personal service activities", NULL),
# MAGIC   ("T", NULL, "'Activities Of Households As Employers; Undifferentiated Goods- And Services-producing Activities Of Households For Own Use'", "Activities Of Households As Employers"),
# MAGIC   ("T", "97", "Activities of households as employers of domestic personnel", NULL),
# MAGIC   ("T", "98", "Undifferentiated goods- and services-producing activities of private households for own use", NULL),
# MAGIC   ("U", NULL, "Activities Of Extraterritorial Organisations And Bodies", "Activities Of Extraterritorial Organisations And Bodies"),
# MAGIC   ("U", "99", "Activities of extraterritorial organisations and bodies", NULL)
# MAGIC
