# Databricks notebook source
# MAGIC %md
# MAGIC ### We need to create gold tables to start with the data transformations. First, we need a table to join the funnel data with catalog data, in this table we need to add a new column 'quality_check' 

# COMMAND ----------

from pyspark.sql.functions import length, col, when, max, count, current_timestamp, lit, min, StructType, datediff
from pyspark.sql.types import StructField, StringType, IntegerType, DateType, TimestampType
from delta.tables import DeltaTable


# COMMAND ----------

#This table are created to join with catalog data.

schema_funnel = StructType([
    StructField("client_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("email", StringType(), True),
    StructField("app", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("register_date", DateType(), True),
    StructField("complete_process", IntegerType(), True),
    StructField('quality_check', StringType(), True),
    StructField("ingestion_date", TimestampType(), True)
])

funnel_gold_df = spark.createDataFrame([], schema_funnel)

funnel_gold_df.write \
    .format("delta") \
    .option("path", "abfss://gold@projectamebank.dfs.core.windows.net/clients_funnel") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## For the clients we need to create a table and use the pivot functions to pivot in the colum stage, separate by stage using the data in silver table.

# COMMAND ----------

#Table for the pivoted stages and register_date

schema_clients = StructType([
    StructField('client_id', StringType(), True),
    StructField('complete_process', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('state', StringType(), True),
    StructField('email', StringType(), True),
    StructField('app', StringType(), True),
    StructField('introduction', DateType(), True),
    StructField('data_request', DateType(), True),
    StructField('data_validation', DateType(), True),
    StructField('facial_recognition', DateType(), True),
    StructField('signature', DateType(), True),
    StructField('data_confirmation', DateType(), True),
    StructField('ingestion_time', TimestampType(), True),
    StructField('update_date', TimestampType(), True)
])

clients_gold_df = spark.createDataFrame([], schema_clients)

clients_gold_df.write \
    .format("delta") \
    .option("path", "abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage") \
    .save()
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ok, our data are inserted into silver tables, cleaned and transformed. Now we can work and apply data transformations for audit.
# MAGIC - We can apply the inner join  funnel data with catalog data. This will help us keep track of users and their information in each process.
# MAGIC

# COMMAND ----------

#Load the funnel data
funnel_data = spark.read \
                   .format('delta') \
                    .load('abfss://silver@projectamebank.dfs.core.windows.net/users_funnel') \
                    .select('client_id', 'app', 'stage', 'register_date', 'complete_process') \
                    .withColumn('quality_check', when((length(col('client_id')) == 5) & (col('complete_process').isin(0, 1)), 'Passed') \
                    .otherwise('Not Passed'))
#Add a new column quality_check, to check if the client_id is valid or not and if the complete_process is 0 or 1.

#Applying the same for catalog 
catalog_data = spark.read \
                    .format('delta') \
                    .load('abfss://silver@projectamebank.dfs.core.windows.net/catalog') \
                    .select('client_id', 'name', 'age', 'state', 'email')

#Now, apply the join, to get the final df, this final df is merge with the gold table and validate some information 
join_df = funnel_data.join(catalog_data, on='client_id', how="inner") \
                     .select('client_id', 'name', 'age', 'state', 'email', 'app', 'stage', 'register_date', 'complete_process', 'quality_check')


# COMMAND ----------

#Now we need to do the merge into gold table
gold_path = 'abfss://gold@projectamebank.dfs.core.windows.net/clients_funnel'

delta_table = DeltaTable.forPath(spark, gold_path)

delta_table.alias('t').merge(
    join_df.alias('s'),
    ' t.client_id = s.client_id and t.stage = s.stage'
    ).whenNotMatchedInsert(
        values = {
            'client_id': 's.client_id',
            'name': 's.name',
            'age': 's.age',
            'state': 's.state',
            'email': 's.email',
            'app': 's.app',
            'stage': 's.stage',
            'register_date': 's.register_date',
            'complete_process': 's.complete_process',
            'quality_check': 's.quality_check',
            'ingestion_date': current_timestamp() #Add the ingestion date when the data is inserted into the table
        }
    ).execute()


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Now we have our table cliets_funnel join, we can do a pivot. Because, we need to separate the stages one by one to know the date stage by stage, to do that, we use the table in gold container. But, we can see the stages are in spanish, we need the change the name.

# COMMAND ----------

clients_df = spark.read \
                  .format('delta') \
                  .option('header', True) \
                  .load('abfss://gold@projectamebank.dfs.core.windows.net/clients_funnel') \
                  .select('client_id', 'complete_process', 'name', 'age', 'state', 'email', 'app', 'stage', 'register_date') \
                  .withColumn('stage',
                                     when(col('stage') == 'Introduccion', 'introduction')
                                    .when(col('stage') == 'Solicitud de datos', 'data_request')
                                    .when(col('stage') == 'Validacion de datos', 'data_validation')
                                    .when(col('stage') == 'Reconocimiento facial', 'facial_recognition')
                                    .when(col('stage') == 'Firma', 'signature')
                                    .when(col('stage') == 'Confirmacion de datos', 'data_confirmation')) 

#Use a pivot function to create a new table with the stages and the date per stage.
pivot_df = clients_df \
                     .groupBy('client_id', 'complete_process', 'name', 'age', 'state', 'email', 'app') \
                     .pivot('stage', [
                         'introduction',
                         'data_request',
                         'data_validation',
                         'facial_recognition',
                         'signature',
                         'data_confirmation'
                     ]) \
                     .agg(min(col('register_date'))) \
                     .orderBy('client_id')

display(pivot_df)

# COMMAND ----------

#Now we need to do the merge pivot_df into gold table pv_clients_stage
gold_pv_path = 'abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage'

delta_table = DeltaTable.forPath(spark, gold_pv_path)

(
    delta_table.alias('t')
    .merge(
        pivot_df.alias('s'),
        't.client_id = s.client_id'
    )
    .whenMatchedUpdate(
        condition=(
            (col('t.name') != col('s.name')) |
            (col('t.email') != col('s.email')) |
            (col('t.age') != col('s.age')) |
            (col('t.state') != col('s.state')) |
            (col('t.complete_process') != col('s.complete_process')) |
            (col('t.introduction') != col('s.introduction')) |
            (col('t.data_request') != col('s.data_request')) |
            (col('t.data_validation') != col('s.data_validation')) |
            (col('t.facial_recognition') != col('s.facial_recognition')) |
            (col('t.signature') != col('s.signature')) |
            (col('t.data_confirmation') != col('s.data_confirmation'))
        ),
        set={
            'name': col('s.name'),
            'email': col('s.email'),
            'age': col('s.age'),
            'state': col('s.state'),
            'complete_process': col('s.complete_process'),
            'introduction': col('s.introduction'),
            'data_request': col('s.data_request'),
            'data_validation': col('s.data_validation'),
            'facial_recognition': col('s.facial_recognition'),
            'signature': col('s.signature'),
            'data_confirmation': col('s.data_confirmation'),
            'update_date': current_timestamp() #When the data has modified, add the current timestamp
        }
    )
    .whenNotMatchedInsert(
        values={
            'client_id': col('s.client_id'),
            'complete_process': col('s.complete_process'),
            'name': col('s.name'),
            'age': col('s.age'),
            'state': col('s.state'),
            'email': col('s.email'),
            'app': col('s.app'),
            'introduction': col('s.introduction'),
            'data_request': col('s.data_request'),
            'data_validation': col('s.data_validation'),
            'facial_recognition': col('s.facial_recognition'),
            'signature': col('s.signature'),
            'data_confirmation': col('s.data_confirmation'),
            'ingestion_time': current_timestamp() #When the data not changed, we add the ingest into gold table.
        }
    )
    .execute()
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Now,we can do some calculations, for example:
# MAGIC - Clients that finished all stages
# MAGIC - Total time between the firts and last stage for each client
# MAGIC - Most frequent abandonment stage
# MAGIC

# COMMAND ----------

#Clients that finished all stages
clients_finished_all = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                 .filter(col('complete_process') == 1) \
                                 .select(count('*').alias('finished_all'))
display(clients_finished_all)


# COMMAND ----------

#Clients didn't finish the process
clients_not_finished_all = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                 .filter(col('complete_process') == 0) \
                                 .select(count('*')).alias('didnt_finish_all')
display(clients_not_finished_all)

# COMMAND ----------

#Total time between the firts and last stage for each client
total_time = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                        .filter(col('complete_process') == 1) \
                        .groupBy('client_id', 'complete_process') \
                        .agg(max('data_confirmation').alias('max'), min('introduction').alias('min')) \
                        .withColumn('total_time_days', datediff(col('max'), col('min'))) \
                        .orderBy('client_id')
display(total_time)

# COMMAND ----------

#Most abandoned stage abandoned 
most_abandoned_stage = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                 .filter(col('complete_process') == 0) \
                                 .agg(
                                       count(
                                              when(
                                                col('data_request').isNull() &
                                                col('data_validation').isNull() &
                                                col('facial_recognition').isNull() &
                                                col('signature').isNull() &
                                                col('data_confirmation').isNull(), 1)).alias('TI'),
                                       count(
                                             when(
                                                   col('data_request').isNotNull() &
                                                col('data_validation').isNull() &
                                                col('facial_recognition').isNull() &
                                                col('signature').isNull() &
                                                col('data_confirmation').isNull(), 1)).alias('TSD'),
                                       count(
                                             when(
                                                col('data_validation').isNotNull() &
                                                col('facial_recognition').isNull() &
                                                col('signature').isNull() &
                                                col('data_confirmation').isNull(), 1)).alias('TVD'),
                                       count(
                                             when(
                                                col('facial_recognition').isNotNull() &
                                                col('signature').isNull() &
                                                col('data_confirmation').isNull(), 1)).alias('TRF'),
                                       count(
                                             when(
                                                col('signature').isNotNull() &
                                                col('data_confirmation').isNull(), 1)).alias('TF'),
                                       count(
                                             when(
                                                col('data_confirmation').isNotNull(), 1)).alias('TCD')
                                          ) \
                                     .orderBy('TI')   
display(most_abandoned_stage)
