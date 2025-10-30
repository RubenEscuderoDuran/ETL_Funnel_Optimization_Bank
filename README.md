# ETL_Funnel_Optimization_Bank

In this project, I contributed to the design and development of an end-to-end ELT in Databricks, considerer incremental load, aimed, analyzing and optinmizing the digital funnel for new banking customers.

The solution focused on analyzing user behavior within a mobile app that enables account opening without visiting a branch. Drop-off patterns, average time per stage, and conversion differences by region and age group were identified. These insights enabled the business to redesign parts of the process to improve conversion rates.

# ETL Process

This diagram presents a detailed representation of the complete ETL process implemented to accomplish the proposed objectives for this project.

![alt text](https://github.com/RubenEscuderoDuran/ETL_Funnel_Optimization_Bank/blob/main/ETL.png)

# Dashboard

This funnel chart represents the customer conversion rate by stage, showing that only 76.40% of users completed the registration process to open a bank account trough the mobile app.

![alt text](https://github.com/RubenEscuderoDuran/ETL_Funnel_Optimization_Bank/blob/main/Funnel_model.png)

This bar chart represents the percentage by conversion to TCD per state, showing only in Aguascalientes the users complete all the process to open a bank account.

![alt text](https://github.com/RubenEscuderoDuran/ETL_Funnel_Optimization_Bank/blob/main/Percentage_by_TCD.png)

This bar chart represents the age group by stage, from this, we can see only users aged 50+ are carrying out the process, following by age group 36-50.

![alt text](https://github.com/RubenEscuderoDuran/ETL_Funnel_Optimization_Bank/blob/main/Percentage_by_agegroup.png)

# Automate Workflow 

This job allows us to automatically orchestrate the execution of tasks (ETL processes) defined in the notebooks. It is scheduled to run periodically.

![alt text](https://github.com/RubenEscuderoDuran/ETL_Funnel_Optimization_Bank/blob/main/Workflow.png)



