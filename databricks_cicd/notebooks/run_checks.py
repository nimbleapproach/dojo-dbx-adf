# Databricks notebook source
# MAGIC %pip install pytest
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

# COMMAND ----------

# MAGIC %md
# MAGIC The below widget offers two testing modes: 'Targeted' and 'Comprehensive'. The default option is 'Comprehensive' and runs all tests in our repo. In order to run tests for specific modules, select the 'Targeted' option and add pytestmark = pytest.mark.devmarker to the top of the desired module. Pytest will then run tests for those modules exclusively.

# COMMAND ----------

dbutils.widgets.dropdown("Testing Mode", "Comprehensive", ["Targeted", "Comprehensive"])
mode = dbutils.widgets.get("Testing Mode")

if mode == "Comprehensive":
    devmarker = ""
elif mode == "Targeted":
    devmarker = "-m devmarker"

# COMMAND ----------

import pytest
import os
import sys

# COMMAND ----------

# pass schema as arguments to test modules
sys.argv = [catalog]

# COMMAND ----------

# Get the repo's root directory name.
rootdir = os.getcwd()

# Prepare to run pytest from the repo.
os.chdir(rootdir)
print(rootdir)
# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# # Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider", devmarker])

# # Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
