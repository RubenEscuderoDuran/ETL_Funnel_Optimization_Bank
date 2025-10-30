# Databricks notebook source
# MAGIC %md
# MAGIC First we need to create a storage credential. For default, databricks create a storage location, but we need to create and assign permissions to create external locations. We can do manually, go to the data, catalog explorer, external data, credentials and create credentials. Or we can do in SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE STORAGE CREDENTIAL cred_projectamebank
# MAGIC -- WITH (
# MAGIC -- STORAGE_ACCOUNT = 'projectamebank',
# MAGIC -- STORAGE_ACCOUNT_KEY = 'YOUR_KEY'
# MAGIC --)

# COMMAND ----------

# MAGIC %md
# MAGIC Once the storage credentials is created, now create the external location

# COMMAND ----------

spark.sql('''CREATE EXTERNAL LOCATION IF NOT EXISTS landing
URL 'abfss://landing@projectamebank.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_projectamebank)''')

spark.sql('''CREATE EXTERNAL LOCATION IF NOT EXISTS bronze
URL 'abfss://bronze@projectamebank.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_projectamebank)''')

spark.sql('''CREATE EXTERNAL LOCATION IF NOT EXISTS silver
URL 'abfss://silver@projectamebank.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_projectamebank)''')

spark.sql('''CREATE EXTERNAL LOCATION IF NOT EXISTS gold
URL 'abfss://gold@projectamebank.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_projectamebank)''')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'abfss://landing@projectamebank.dfs.core.windows.net/'
