# Databricks notebook source
storage_account_name = "stfmcganalytics1725"
container_name = "raw"
access_key = dbutils.secrets.get(scope="azure-storage", key="access-key")


# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)


# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{container_name}"))

# COMMAND ----------

