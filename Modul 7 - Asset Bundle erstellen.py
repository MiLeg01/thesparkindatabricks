# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Asset Bundles
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Authentifizierung in der Databricks CLI
# MAGIC > 
# MAGIC ```sh
# MAGIC databricks auth login --host <workspace-url>
# MAGIC ```
# MAGIC Username := Mailadresse

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Initialize Databricks Bundle
# MAGIC > 
# MAGIC ```sh
# MAGIC databricks bundle init
# MAGIC ```
# MAGIC
# MAGIC Selections:
# MAGIC - `default-python`
# MAGIC - provide `name`
# MAGIC - no sample notebook
# MAGIC - no sample DLT pipeline
# MAGIC - no sample Python package
# MAGIC - **yes** to serverless

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Review Bundlestruktur & src Folder
# MAGIC
# MAGIC - Review der Folderstruktur
# MAGIC - Erzeuge einen src/ Ordner und füge .py Files hinein die deployed und ausgeführt werden sollen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Review `databricks.yml`
# MAGIC
# MAGIC - Review des Files databricks.yml
# MAGIC - databricks.yml ist das zentrale File in der Databricks Asset Bundle Struktur
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Erzeuge ein File `variables.yml` in \\resources
# MAGIC > 
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: Unity Catalog name
# MAGIC     default: workspace
# MAGIC   schema:
# MAGIC     description: Schema name
# MAGIC     default: default
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Definiere einen Job in `databricks.yml`
# MAGIC > 
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl_job:
# MAGIC       name: "Kapitel 7 ETL Job"
# MAGIC       tasks:
# MAGIC         - task_key: run_etl
# MAGIC           spark_python_task:
# MAGIC             python_file: src/df_operations.py
# MAGIC           environment_key: Default
# MAGIC       environments:
# MAGIC         - environment_key: Default
# MAGIC           spec:
# MAGIC             client: "3"
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Bundle validieren, deployen und Run durchführen
# MAGIC
# MAGIC **Validieren:**
# MAGIC ```sh
# MAGIC databricks bundle validate
# MAGIC ```
# MAGIC
# MAGIC **Deployen:**
# MAGIC ```sh
# MAGIC databricks bundle deploy
# MAGIC ```
# MAGIC
# MAGIC **Run:**
# MAGIC ```sh
# MAGIC databricks bundle run -t dev etl_job
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Erzeuge einen Service Principal
# MAGIC
# MAGIC - Service Principal in der Admin-Konsole erzeugen
# MAGIC - Service Principal berechtigen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Produktionsumgebung anpassen
# MAGIC > 
# MAGIC ```yaml
# MAGIC prod:
# MAGIC   mode: production
# MAGIC   presets:
# MAGIC     name_prefix: "[prod SP]"
# MAGIC   workspace:
# MAGIC     host: https://...
# MAGIC     root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
# MAGIC   run_as:
# MAGIC     service_principal_name: tbd
# MAGIC   permissions:
# MAGIC     - user_name: tbd
# MAGIC       level: CAN_MANAGE
# MAGIC     - group_name: admins
# MAGIC       level: CAN_MANAGE
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Run Production Deployment
# MAGIC >             
# MAGIC ```sh
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
