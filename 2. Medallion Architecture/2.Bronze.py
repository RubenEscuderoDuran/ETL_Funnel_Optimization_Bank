# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now we can work and see the data in bronze folder and apply some transformations.
# MAGIC

# COMMAND ----------

df= spark.read.format('delta').load('abfss://bronze@projectamebank.dfs.core.windows.net/usuarios_banco_con_funnel')
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## First, we observe that the data type does not correspond to certain columns. For example, date is a string, as is process_complete. We also observe that the column names are not appropriate or may cause conflicts later on.
# MAGIC - Id_cliente - id_client
# MAGIC - App - app,
# MAGIC - date - register_date and datatype as date
# MAGIC - Proceso_completo - process_complete and datatype as Int

# COMMAND ----------

df = df.withColumnRenamed('ID_cliente', 'client_id') \
       .withColumnRenamed('App', 'app') \
       .withColumnRenamed('date', 'register_date') \
       .withColumnRenamed('Proceso_completo', 'complete_process') \
       .withColumnRenamed('fecha_ingesta', 'ingestion_date') \
       .withColumn('register_date',col('register_date').cast(DateType())) \
       .withColumn('complete_process',col('complete_process').cast(IntegerType())) \
       .withColumn('register_date', to_date(col('register_date'), 'yyyy/MM/dd'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### We need to do the same for clients data

# COMMAND ----------

df_clients = spark.read.format('delta').load('abfss://bronze@projectamebank.dfs.core.windows.net/clientes')
df_clients.show(10, truncate=False)

# COMMAND ----------

df_clients.printSchema()

# COMMAND ----------

df_clients = df_clients.withColumnRenamed('ID_cliente', 'client_id') \
                       .withColumnRenamed('Nombre', 'name') \
                       .withColumnRenamed('Correo','email') \
                       .withColumnRenamed('Edad', 'age') \
                       .withColumnRenamed('Estado', 'state') \
                       .withColumnRenamed('fecha_ingesta', 'ingestion_date') \
                       .withColumn('ingestion_date', to_timestamp(col('ingestion_date'), 'yyyy/MM/dd HH:mm:ss')) \
                       .withColumn('age', col('age').cast(IntegerType())) \
                       .withColumn('update_date', current_timestamp()) ##Here we add a new column update_date, This helps us when performing incremental loading, so we can see if any data has been modified.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now the data has been cleaned and transformed, we need to create the tables in silver container.

# COMMAND ----------

#Once for funnel users

schema_funnel = StructType([
    StructField("client_id", StringType(), True),
    StructField("app", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("register_date", DateType(), True),
    StructField("complete_process", IntegerType(), True),
    StructField("ingestion_date", TimestampType(), True)
])

funnel_df = spark.createDataFrame([], schema_funnel)

funnel_df.write \
    .format("delta") \
    .option("path", "abfss://silver@projectamebank.dfs.core.windows.net/users_funnel") \
    .save()

# COMMAND ----------

#Once for clients

schema_clients = StructType([
    StructField("client_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("ingestion_date", TimestampType(), True),
    StructField('update_date', TimestampType(), True)
])

clients_df = spark.createDataFrame([], schema_clients)

clients_df.write \
    .format("delta") \
    .option("path", "abfss://silver@projectamebank.dfs.core.windows.net/catalog") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ok, now, we need to do a merge to insert the data in bronze container into silver table.

# COMMAND ----------

#Path for table in silver container
silver_path = "abfss://silver@projectamebank.dfs.core.windows.net/users_funnel"

# Load the table 
delta_silver = DeltaTable.forPath(spark, silver_path)

delta_silver.alias("silver").merge(
    source=df.alias("bronze"),
    condition=" silver.client_id = bronze.client_id and silver.stage = bronze.stage"
).whenNotMatchedInsert(
    values = {
        "client_id": "bronze.client_id",
        "app": "bronze.app",
        "stage": "bronze.stage",
        "register_date": "bronze.register_date",
        "complete_process": "bronze.complete_process",
        "ingestion_date": current_timestamp()
    }
) .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying the same for catalog data.

# COMMAND ----------

silver_path = "abfss://silver@projectamebank.dfs.core.windows.net/catalog"
df_silver = spark.read.format("delta").load(silver_path)

delta_silver = DeltaTable.forPath(spark, silver_path)

delta_silver.alias("silver").merge(
    source=df_clients.alias("bronze"),
    condition="silver.client_id = bronze.client_id"
).whenMatchedUpdate(
    condition="""
        silver.name <> bronze.name OR
        silver.email <> bronze.email OR
        silver.age <> bronze.age OR
        silver.state <> bronze.state
    """,
    set={
        "name": "bronze.name",
        "email": "bronze.email",
        "age": "bronze.age",
        "state": "bronze.state",
        "ingestion_date": "bronze.ingestion_date",
        "update_date": current_timestamp() #Here only insert the current_timestamp when the data is modified 
    }
).whenNotMatchedInsert(values={
    "client_id": "bronze.client_id",
    "name": "bronze.name",
    "email": "bronze.email",
    "age": "bronze.age",
    "state": "bronze.state",
    "ingestion_date": "bronze.ingestion_date",
    "update_date": 'Null' #When the data is inserted, the update_date is null because it is a new record.
}).execute()
