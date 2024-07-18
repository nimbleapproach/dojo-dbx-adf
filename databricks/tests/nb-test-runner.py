# Databricks notebook source
# MAGIC %run ../library/nb-enable-imports

# COMMAND ----------

import os
import unittest

# COMMAND ----------

start_dir = os.path.abspath(".")
loader = unittest.defaultTestLoader
suite = loader.discover(start_dir, pattern='*_test.py')
runner = unittest.TextTestRunner()
runner.run(suite)
