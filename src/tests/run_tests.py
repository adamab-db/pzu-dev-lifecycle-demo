# Databricks notebook source
# MAGIC %md
# MAGIC # Run Unit Tests
# MAGIC
# MAGIC Invokes pytest against the test suite. Following
# MAGIC [Databricks testing best practices](https://docs.databricks.com/aws/en/notebooks/testing):
# MAGIC - Transformation logic lives in `src/helpers/transformations.py`
# MAGIC - Tests use **fake data** — no production tables touched
# MAGIC - Tests are discovered and run by **pytest**

# COMMAND ----------

# MAGIC %pip install pytest
# MAGIC %restart_python

# COMMAND ----------

import os
import sys
import shutil
import tempfile

# Get workspace paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
ws_tests_dir = "/Workspace" + os.path.dirname(notebook_path)
ws_src_dir = os.path.normpath(os.path.join(ws_tests_dir, ".."))

# Copy only the .py files (not notebooks) to a temp dir for pytest
tmp_dir = tempfile.mkdtemp()

# Copy helpers module
tmp_helpers = os.path.join(tmp_dir, "helpers")
shutil.copytree(os.path.join(ws_src_dir, "helpers"), tmp_helpers)

# Copy test file
tmp_tests = os.path.join(tmp_dir, "tests")
os.makedirs(tmp_tests)
shutil.copy2(os.path.join(ws_tests_dir, "test_transformations.py"), tmp_tests)

# Add to path
sys.path.insert(0, tmp_dir)

print(f"tmp_dir: {tmp_dir}")
print(f"helpers: {os.listdir(tmp_helpers)}")
print(f"tests: {os.listdir(tmp_tests)}")

# COMMAND ----------

import pytest

retcode = pytest.main([
    os.path.join(tmp_dir, "tests", "test_transformations.py"),
    "-v",
    "-p", "no:cacheprovider",
    "--tb=short",
])

# Cleanup
shutil.rmtree(tmp_dir)

if retcode == 0:
    dbutils.notebook.exit("All tests passed")
else:
    dbutils.notebook.exit(f"FAILED with return code {retcode}")
