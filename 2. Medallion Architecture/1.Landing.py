# Databricks notebook source
# MAGIC %md
# MAGIC #### For this project, we created a storage account following the medallion architecture: bronze, silver, and gold. In this case, we placed our generated datasets in landing container so we could work with them (clients.csv and usuarios_banco_con_funnel.csv). The usres_bank_funnel contain the data, stage and date, and clients contain age, state and client_id. 
# MAGIC
# MAGIC #### Once they are there, we can copy the data by creating a pipeline in ADF to move the data to bronze, or we can read the data manually.

# COMMAND ----------

from pyspark.sql import *
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if we can see the data

# COMMAND ----------

##df_users = spark.read.option('header', True).csv('abfss://landing@projectamebank.dfs.core.windows.net/usuarios_banco_con_funnel.csv')
##display(df_users)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Given the type of project we are working on, we know that at some point, when we process the data, new data will arrive at the landing container whenever users register. Therefore, we have to consider our project and prepare it for incremental loading. 
# MAGIC
# MAGIC #### To do this, we will develop a function that reads the data in the landing container, checks if there is data in our bronze container, and if there is no data, saves the data as a table. However, if there is data, it applies the incremental load without the need to rewrite our data again.
# MAGIC
# MAGIC

# COMMAND ----------

def create_or_merge_table(table_name, merge_condition):
    #Source in landing folder
    source_path = f'abfss://landing@projectamebank.dfs.core.windows.net/{table_name}.csv'
    df = spark.read.option('header', True).csv(source_path)
    df = df.withColumn("fecha_ingesta", current_timestamp()) #Add a new column to verify that incremental loading is working and also add the ingestion date


    # Target in bronze folder
    target_path = f'abfss://bronze@projectamebank.dfs.core.windows.net/{table_name}'

    # Check if the Delta table exists
    if DeltaTable.isDeltaTable(spark, target_path):
        print("🔄 Delta Table found. Applying incremental load...")

        delta_target = DeltaTable.forPath(spark, target_path)

        delta_target.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    else:
        print("🆕 No Delta Table found. Creating new table...")
        df.write.format("delta") \
            .mode("overwrite") \
            .save(target_path)

# COMMAND ----------

create_or_merge_table(
    'usuarios_banco_con_funnel',
    "source.ID_cliente = target.ID_cliente AND source.stage = target.stage"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We need to do the same for the clients, but in clients don't have stage column, only we need to modify the merge_condition to ID_cliente.

# COMMAND ----------

##df_clients = spark.read.option('header', True).csv('abfss://landing@projectamebank.dfs.core.windows.net/clientes.csv')
##display(df_clients)

# COMMAND ----------

create_or_merge_table(
    'clientes',
    "source.ID_cliente = target.ID_cliente"
)
