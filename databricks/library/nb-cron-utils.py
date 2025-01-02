# Databricks notebook source
# MAGIC %pip install cron-converter --quiet

# COMMAND ----------

from cron_converter import Cron
from datetime import datetime

def get_previous_date(reference_date: datetime, cron) -> datetime:
    cron_instance = Cron(cron, {'output_hashes': True})
    schedule = cron_instance.schedule(reference_date)
    return schedule.prev()

def get_next_date(reference_date: datetime, cron) -> datetime:
    cron_instance = Cron(cron, {'output_hashes': True})
    schedule = cron_instance.schedule(reference_date)
    return schedule.next()