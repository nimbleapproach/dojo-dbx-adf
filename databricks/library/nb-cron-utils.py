# Databricks notebook source
# MAGIC %pip install cron-converter --quiet
# MAGIC
# MAGIC from cron_converter import Cron
# MAGIC from datetime import datetime

# COMMAND ----------

def get_previous_date(reference_date: datetime, cron) -> datetime:
    cron_instance = Cron(cron, {'output_hashes': True})
    print(f"reference date: {reference_date}")
    schedule = cron_instance.schedule(reference_date)
    return schedule.prev()

def get_next_date(reference_date: datetime, cron) -> datetime:
    cron_instance = Cron(cron, {'output_hashes': True})
    print(f"reference date: {reference_date}")
    schedule = cron_instance.schedule(reference_date)
    return schedule.next()
