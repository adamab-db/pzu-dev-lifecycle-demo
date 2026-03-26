# Databricks notebook source
# MAGIC %md
# MAGIC # Run Unit Tests
# MAGIC
# MAGIC Runs pytest against `test_transformations.py`.
# MAGIC Transformation logic lives in `src/helpers/transformations.py`;
# MAGIC tests use fake data — no production tables are touched.

# COMMAND ----------

# MAGIC %pip install pytest
# MAGIC %restart_python

# COMMAND ----------

import pytest, sys, os, shutil, tempfile

# Workspace filesystem requires copying .py files to local disk for pytest discovery
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
ws_tests = "/Workspace" + os.path.dirname(notebook_path)
ws_src = os.path.normpath(os.path.join(ws_tests, ".."))

tmp = tempfile.mkdtemp()
shutil.copytree(os.path.join(ws_src, "helpers"), os.path.join(tmp, "helpers"))
shutil.copy2(os.path.join(ws_tests, "test_transformations.py"), tmp)
sys.path.insert(0, tmp)

retcode = pytest.main([tmp, "-v", "-p", "no:cacheprovider", "--tb=short"])

shutil.rmtree(tmp)
assert retcode == 0, f"pytest failed with return code {retcode}"
