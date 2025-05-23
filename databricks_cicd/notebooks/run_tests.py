# Databricks notebook source
import pytest
import os
import sys

# COMMAND ----------

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace{repo_root}")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True
# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider", "-W", "ignore::DeprecationWarning", "--ignore=notebooks/test_unit"])

# # Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

