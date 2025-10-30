# Databricks notebook source
# MAGIC %md
# MAGIC # Analityc Model 

# COMMAND ----------

from pyspark.sql.functions import col, when, max, count, current_timestamp, avg, lit, sum, round, datediff
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.- Clients amount per country thaty finished the process

# COMMAND ----------

clients_per_country = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                .filter(col('complete_process') == 1) \
                                .groupBy('state') \
                                .agg(count('*').alias('Total_per_country')) \
                                .orderBy(col('Total_per_country').desc())
display(clients_per_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.- Clients amount per country that didn't finished the process

# COMMAND ----------

clients_per_country_not_finished = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                .filter(col('complete_process') == 0) \
                                .groupBy('state') \
                                .agg(count('*').alias('Total_per_country')) \
                                .orderBy(col('Total_per_country').desc())
display(clients_per_country_not_finished)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Clients amount per country that finished the process but not all the stages.

# COMMAND ----------

not_finish_all_stages = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                                  .filter((col('complete_process') == 0) & (col('data_confirmation').isNotNull())) \
                                                  .groupBy('state') \
                                                  .agg(count('*').alias('Total_per_country')) \
                                                  .orderBy(col('Total_per_country').desc())
display(not_finish_all_stages)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Average per client to finish the process

# COMMAND ----------

av_per_stage = spark.read.format('delta').load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                                         .filter((col('complete_process') == 1)) \
                                         .groupBy('state') \
                                         .agg(avg(datediff(col('data_confirmation'), col('introduction'))).alias('AV')) \
                                         .orderBy(col('AV').desc())
display(av_per_stage)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.- Clients per state

# COMMAND ----------

clients_per_stage = spark.read \
                         .format('delta') \
                         .load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                         .groupBy('state') \
                         .agg(count('*').alias('Total_per_country')) \
                         .orderBy(col('Total_per_country').desc())
display(clients_per_stage)                 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Total clients per stage (amount of clients that finished a stage)

# COMMAND ----------

total_clients_per_stage = spark.read \
                               .format('delta') \
                               .load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                               .agg(
                                   count('*').alias('TI'),
                                   count(
                                       when(
                                            col('data_request').isNotNull() |
                                            col('data_validation').isNotNull() |
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull () |
                                            col('data_confirmation').isNotNull(), 1)).alias('TSD'),
                                   count(
                                       when(
                                            col('data_validation').isNotNull() |
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TVD'),
                                   count(
                                       when(
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TRF'),
                                   count(
                                       when(
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TF'),
                                   count(
                                       when(
                                           col('data_confirmation').isNotNull(), 1)).alias('TCD')
                                ) 

display(total_clients_per_stage)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 7.- Calculate the conversation rate per state (stage, amount, conversion)

# COMMAND ----------

total_clients_per_stage = spark.read \
                               .format('delta') \
                               .load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                               .groupBy('state') \
                               .agg(
                                   count('*').alias('TI'),
                                   count(
                                       when(
                                            col('data_request').isNotNull() |
                                            col('data_validation').isNotNull() |
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull () |
                                            col('data_confirmation').isNotNull(), 1)).alias('TSD'),
                                   count(
                                       when(
                                            col('data_validation').isNotNull() |
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TVD'),
                                   count(
                                       when(
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TRF'),
                                   count(
                                       when(
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TF'),
                                   count(
                                       when(
                                           col('data_confirmation').isNotNull(), 1)).alias('TCD')
                                ) \
                                .orderBy('state')

unpivot_df = total_clients_per_stage.selectExpr(
    'state',
    'stack(6, "TI", TI, "TSD", TSD, "TVD", TVD, "TRF", TRF, "TF", TF, "TCD", TCD) as (stage, Total_users)'
)

total_ti = unpivot_df \
    .filter(col('stage') == 'TI') \
    .agg(sum('Total_users').alias('Total_TI')) \
    .collect()[0][0]

#Calculate the conversion rate
conv_rate = unpivot_df \
                     .groupBy('stage') \
                     .agg(sum('Total_users').alias('Total_by_stage')) \
                     .withColumn('Percentage_conversion', round(col('Total_by_stage') * 100.0 / total_ti, 2)) \
                     .orderBy(col('Percentage_conversion').desc())
display(conv_rate)       



# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.- Conversion per state (state, stage, amount, conversion)

# COMMAND ----------


total_clients_per_state = spark.read \
                               .format('delta') \
                               .load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
                               .groupBy('state') \
                               .agg(
                                   count('*').alias('TI'),
                                   count(
                                       when(
                                            col('data_request').isNotNull() |
                                            col('data_validation').isNotNull() |
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull () |
                                            col('data_confirmation').isNotNull(), 1)).alias('TSD'),
                                   count(
                                       when(
                                            col('data_validation').isNotNull() |
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TVD'),
                                   count(
                                       when(
                                            col('facial_recognition').isNotNull() |
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TRF'),
                                   count(
                                       when(
                                            col('signature').isNotNull() |
                                            col('data_confirmation').isNotNull(), 1)).alias('TF'),
                                   count(
                                       when(
                                           col('data_confirmation').isNotNull(), 1)).alias('TCD')
                                ) \
                                .orderBy('state')



unpivot_df = total_clients_per_state.selectExpr(
    'state',
    'stack(6, "TI", TI, "TSD", TSD, "TVD", TVD, "TRF", TRF, "TF", TF, "TCD", TCD) as (stage, Total_users)'

)

a = unpivot_df.alias('a')
b = unpivot_df.alias('b')

join_df = a.join(
    b.filter(b.stage == 'TI'),
    on='state',
    how= 'inner'
) ##Here, we are joining the two DataFrames on the state column to bring the total number of users per state, in this case with the stage 'TI'

conversion_rate = join_df \
    .select(
        a.state,
        a.stage,
        a.Total_users,
        round((a.Total_users * 100.0 / b.Total_users), 2).alias('Percentage_conversion')
    )

display(conversion_rate)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.- Stage by age group
# MAGIC

# COMMAND ----------

users_per_state = spark.read \
    .format('delta') \
    .load('abfss://gold@projectamebank.dfs.core.windows.net/pv_clients_stage') \
    .withColumn(
        'Age_group',
                     when((col('age') >= 18) & (col('age') <= 25), '18-25')
                    .when((col('age') >= 26) & (col('age') <= 35), '26-35')
                    .when((col('age') >= 36) & (col('age') <= 50), '36-50')
                    .otherwise('50+')
    ) \
    .groupBy('state', 'Age_group') \
    .agg(
        count('*').alias('TI'),
        count(
            when(
                col('data_request').isNotNull() |
                col('data_validation').isNotNull() |
                col('facial_recognition').isNotNull() |
                col('signature').isNotNull() |
                col('data_confirmation').isNotNull(), 1)).alias('TSD'),
        count(
            when(
                col('data_validation').isNotNull() |
                col('facial_recognition').isNotNull() |
                col('signature').isNotNull() |
                col('data_confirmation').isNotNull(), 1)).alias('TVD'),
        count(
            when(
                col('facial_recognition').isNotNull() |
                col('signature').isNotNull() |
                col('data_confirmation').isNotNull(), 1)).alias('TRF'),
        count(
            when(
                col('signature').isNotNull() |
                col('data_confirmation').isNotNull(), 1)).alias('TF'),
        count(
            when(
                col('data_confirmation').isNotNull(), 1)).alias('TCD')
            ) \
    .orderBy('state', 'Age_group')


unpivot_per_age = users_per_state.selectExpr(
    'state',
    'Age_group',
    'stack(6, "TI", TI, "TSD", TSD, "TVD", TVD, "TRF", TRF, "TF", TF, "TCD", TCD) as (stage, Total_users)'
)

ti_per_state = unpivot_per_age \
    .filter(col('stage') == 'TI') \
    .groupBy('state') \
    .agg(sum('Total_users').alias('Total_TI'))

conver_rate = unpivot_per_age \
    .join(
        ti_per_state,
        on='state',
        how='inner'
    ).withColumn(
        'Percentage_conversion',
        round((col('Total_users') * 100.0 / col('Total_TI')), 2)
    ).orderBy('state', 'Age_group', 'stage')

display(conver_rate)
