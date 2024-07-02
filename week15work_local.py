# Databricks notebook source
dbutils.secrets.list("smweek15entrasecscope")

# COMMAND ----------

client_id = dbutils.secrets.get(scope="smweek15entrasecscope", key="client-id")
tenant_id = dbutils.secrets.get(scope="smweek15entrasecscope", key="tenant-id")
client_secret = dbutils.secrets.get(scope="smweek15entrasecscope", key="client-secret")
storage_account_name = "smdatabricksstore"
container_name = "week15"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

mount_point = f"/mnt/{container_name}"
try:
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = mount_point,
        extra_configs = configs
    )
    print(f"Successfully mounted {container_name} to {mount_point}")
except Exception as e:
    print(f"Error mounting {container_name}: {e}")

# COMMAND ----------

dbutils.fs.ls("/mnt/week15")

# COMMAND ----------

claims_raw_df=spark.read.csv("/mnt/week15/bronze/claims.csv", header=True, inferSchema= True)

# COMMAND ----------

import pyspark.sql.functions as f
claims_uniqID_df = claims_raw_df.withColumn("uniqID", f.concat(f.col("ClaimID"),f.lit("_"),f.col("ServiceCode")))

# COMMAND ----------

claims_uniqID_df.display()

# COMMAND ----------

claims_per_region_df = claims_uniqID_df.groupBy("Claim_rgn").count().orderBy("count", ascending=False)

# COMMAND ----------

claims_per_region_df.display()

# COMMAND ----------

Claims_10k_df = claims_uniqID_df.filter("Amount > 4000").select("ClaimID","Claim_rgn","memberID","ServiceCode","Amount")

# COMMAND ----------

Claims_10k_df.display()

# COMMAND ----------


