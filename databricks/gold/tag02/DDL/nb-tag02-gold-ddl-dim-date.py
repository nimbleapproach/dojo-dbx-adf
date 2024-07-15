# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

#import needed functions
from datetime import date, timedelta, datetime,time
from dateutil.relativedelta import relativedelta
from math import ceil,floor
import string

# COMMAND ----------

#dates range for dim
start_date = date(datetime.today().year-10, 1, 1)
end_date = date(datetime.today().year+10, 12, 31)

# COMMAND ----------

#needed beacause datetime.workday numbers starts on Monday
def get_weekday_number_by_name(input_date):
    weekday_name = input_date.strftime('%A')
    week_days = {"Sunday":1,
             "Monday":2,
             "Tuesday":3,
             "Wednesday":4,
             "Thursday":5,
             "Friday":6,
             "Saturday":7}
    return week_days[weekday_name]

# COMMAND ----------

def get_number_of_days_in_month(input_date):
    month_start_date = date(input_date.year,input_date.month,1)
    month_end_date = month_start_date + relativedelta(months = 1) + timedelta(days=-1)
    return (month_end_date - month_start_date).days + 1

# COMMAND ----------

#get the start and last month of quarter
quarters = [{"month":1,"start_month":1,"end_month":3},
            {"month":2,"start_month":1,"end_month":3},
            {"month":3,"start_month":1,"end_month":3},
            {"month":4,"start_month":4,"end_month":6},
            {"month":5,"start_month":4,"end_month":6},
            {"month":6,"start_month":4,"end_month":6},
            {"month":7,"start_month":7,"end_month":9},
            {"month":8,"start_month":7,"end_month":9},
            {"month":9,"start_month":7,"end_month":9},
            {"month":10,"start_month":10,"end_month":12},
            {"month":11,"start_month":10,"end_month":12},
            {"month":12,"start_month":10,"end_month":12}]

# COMMAND ----------

def get_start_of_quarter(input_date):
    return date(input_date.year,quarters[input_date.month-1]["start_month"],1)

# COMMAND ----------

def get_end_of_quarter(input_date):
    return date(input_date.year,quarters[input_date.month-1]["end_month"],1) + relativedelta(months=1) + timedelta(days=-1)

# COMMAND ----------

def get_last_day_of_month(input_date):
    return (date(input_date.year,input_date.month,1) + relativedelta(months = 1)) + timedelta(days=-1)

# COMMAND ----------

def get_first_day_of_week(input_date):
    input_weekday = get_weekday_number_by_name(input_date)
    return input_date+timedelta(days=-(input_weekday-1))

# COMMAND ----------

def get_last_day_of_week(input_date):
    input_weekday = get_weekday_number_by_name(input_date)
    return input_date+timedelta(days=(7 -input_weekday))

# COMMAND ----------

# list of dates
dates = []
# delta ot time to advanced on each loop
delta = timedelta(days=1)
loop_date = start_date

while loop_date <= end_date:
    # add loop_date with all attributes to list
    row = {
        "date_id" : int(str(loop_date.year) + str(loop_date.month).zfill(2) + str(loop_date.day).zfill(2)),
        "date" : loop_date, 
        "day_of_month":loop_date.day,
        "day_name" :loop_date.strftime('%A'),
        "day_of_week": get_weekday_number_by_name(loop_date),
        "day_of_year":loop_date.strftime('%-j'),
        "week_of_year": int(loop_date.strftime("%U"))+1,
        "month": loop_date.month,
        "month_name": loop_date.strftime('%B'),
        "short_month_name_year": loop_date.strftime('%B')[:3] + "-" +str(loop_date.year),
        "short_month_name": loop_date.strftime("%b"),
        "quarter": ceil(loop_date.month / 3),
        "year": loop_date.year,
        "first_day_of_month": date(loop_date.year,loop_date.month,1),
        "last_day_of_month": get_last_day_of_month(loop_date),
        "first_day_of_quarter": get_start_of_quarter(loop_date),
        "last_day_of_quarter": get_end_of_quarter(loop_date),
        "first_day_of_year": date(loop_date.year,1,1),
        "last_day_of_year": get_last_day_of_month(date(loop_date.year,12,1)),
        "week_start_date": get_first_day_of_week(loop_date),
        "week_end_date": get_last_day_of_week(loop_date),
          }
    dates.append(row)
    # increment start date by timedelta
    loop_date += delta

# COMMAND ----------

dates_df = spark.createDataFrame(dates)

# COMMAND ----------

#display dataframe to check data
display(dates_df)

# COMMAND ----------

#write dataframe to table
dates_df.write.mode("overwrite").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_date")

# COMMAND ----------

dbutils.notebook.exit("success")
