# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Bundles Walkthrough
# MAGIC
# MAGIC ## 1. Authentifizierung in der Databricks CLI
# MAGIC
# MAGIC ```sh
# MAGIC databricks auth login --host <workspace-url>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Initialize Databricks Bundle
# MAGIC
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
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Review der Bundlestruktur & Create `src` Folder
# MAGIC
# MAGIC - Review und erkläre die Folderstruktur
# MAGIC - Erzeuge einen`src/` Ordner und füge .py Files hinzu
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Review `databricks.yml`
# MAGIC
# MAGIC - Review und erkläre das File databricks.yml
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Erzeuge ein File `variables.yml` in \\resources
# MAGIC
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: Unity Catalog name
# MAGIC     default: workspace
# MAGIC   schema:
# MAGIC     description: Schema name
# MAGIC     default: default
# MAGIC   table_name:
# MAGIC     description: Target table
# MAGIC     default: -
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Erzeuge einen Catalog/Schema in Databricks
# MAGIC
# MAGIC - Set up the catalog, if not created yet and fill in the variables.
# MAGIC - Erzeuge einen Catalog/Schema in Databricks, falls benötigt und fülle die entsprechenden Namen im variables.yml ein
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. Definiere einen Job in `databricks.yml`
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl_job:
# MAGIC       name: "Kapitel 7 ETL Job"
# MAGIC       tasks:
# MAGIC         - task_key: run_etl
# MAGIC           spark_python_task:
# MAGIC             python_file: src/etl_job.py
# MAGIC ```
# MAGIC
# MAGIC ## 8. Erstes Deployment
# MAGIC
# MAGIC ```sh
# MAGIC databricks bundle deploy
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9. Erzeuge einen Service Principal
# MAGIC
# MAGIC - Weise deinem Konto die Rolle User zu.
# MAGIC - Gib dem Service-Principal Berechtigungen für den Katalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 10. Run Job & Staging Environment konfigurieren
# MAGIC
# MAGIC ```yaml
# MAGIC staging:
# MAGIC   presets:
# MAGIC     name_prefix: "[staging SP]"
# MAGIC   workspace:
# MAGIC     host: https://...
# MAGIC     root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
# MAGIC   run_as:
# MAGIC     service_principal_name: tbd
# MAGIC   permissions:
# MAGIC     - user_name: tbd
# MAGIC       level: CAN_MANAGE
# MAGIC     - group_name: users
# MAGIC       level: CAN_MANAGE
# MAGIC ```
# MAGIC
# MAGIC - Run a **staging checkout run**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 11. Produktionsumgebung anpassen
# MAGIC
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
# MAGIC ---
# MAGIC
# MAGIC ## 12. Run Production Deployment
# MAGIC
# MAGIC ```sh
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
