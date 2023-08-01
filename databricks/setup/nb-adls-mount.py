# Databricks notebook source
# MAGIC %md
# MAGIC # Setup for connecting to ADLS

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup for connecting to ADLS
# MAGIC %md
# MAGIC ## MountHandle
# MAGIC This class is our entry point for mounting an ADLS to our DBFS (Databricks File System).
# MAGIC
# MAGIC `self.configs` builds the config data we will pass over to the mount function.
# MAGIC
# MAGIC `dbutils.secrets.get(scope="kv-ig-westeurope",key="secret-databricks-app-secret")` gets the secret for the current environment.
# MAGIC
# MAGIC ### mount
# MAGIC `mount` first checks if there is already data mounted if not it mounts the referenced container such as ***bronze, silver or gold***.
# MAGIC
# MAGIC ###unmount
# MAGIC
# MAGIC `unmount` first checks if there is already data mounted if so it unmounts the referenced container such as ***bronze, silver or gold***.

# COMMAND ----------

class MountHandle:
    def __init__(self):
        self.configs = {"fs.azure.account.auth.type": "OAuth",
                        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                        "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="kv-ig-westeurope",key='secret-databricks-app-id'),
                        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="kv-ig-westeurope",key='secret-databricks-app-value'),
                        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/375313bf-8b9b-44af-97cd-fc2e258a968e/oauth2/token"}
        
    def mount(self,container : str):
        if any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
                print("{}".format(container) + " from {}-ADLS already mounted to".format(container) + " DBFS at /mnt/{}".format(container) +".")
        else:
                dbutils.fs.mount(
                source = f"abfss://{container}@{dbutils.secrets.get(scope='kv-ig-westeurope',key='secret-adls-accountname')}.dfs.core.windows.net/",
                mount_point = "/mnt/{}".format(container),
                extra_configs = self.configs)
        
    def unmount(self,container :str):
        if any(mount.mountPoint == "/mnt/{}".format(container) for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount("/mnt/{}".format(container))
        else:
            print("DBFS at /mnt/{}".format(container) + " is not mounted.")

# COMMAND ----------

mountHandler = MountHandle()

# COMMAND ----------

# mountHandler.mount("bronze")
# mountHandler.mount("silver")
# mountHandler.mount("gold")
# mountHandler.mount("checkpoints")
# dbutils.fs.ls("mnt/checkpoints")

# COMMAND ----------

# mountHandler.unmount("bronze")
# mountHandler.unmount("silver")
# mountHandler.unmount("gold")
# dbutils.fs.ls("mnt/bronze")
