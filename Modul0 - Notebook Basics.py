# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Notebook & Databricks Basics
# MAGIC
# MAGIC Notebooks sind das wichtigste Mittel, um auf Databricks interaktiv Code zu entwickeln und auszuführen. Diese Lektion bietet eine grundlegende Einführung in die Arbeit mit Databricks-Notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mit einem Cluster oder Serverless verbinden
# MAGIC
# MAGIC Klicke oben rechts auf deinem Bildschirm auf den Cluster-Auswahlknopf („Connect“-Schaltfläche) und wähle entweder einen Cluster oder die Serverless-Option aus dem Dropdown-Menü. Sobald das Notebook verbunden ist, zeigt die Schaltfläche den Namen der Ressource (Cluster oder Serverless) an.
# MAGIC
# MAGIC **HINWEIS:**
# MAGIC
# MAGIC Das Bereitstellen eines Clusters kann mehrere Minuten dauern. Ein durchgehender grüner Kreis erscheint links neben dem Clusternamen, sobald die Ressourcen bereitgestellt wurden. Wenn dein Cluster links daneben einen leeren grauen Kreis anzeigt, musst du den Anweisungen folgen, um den Cluster zu <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">starten</a>.
# MAGIC
# MAGIC Bei der Nutzung von Serverless übernimmt Databricks automatisch die Bereitstellung und Verwaltung der Ressourcen. Dadurch entfällt das manuelle Starten oder Stoppen eines Clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook-Grundlagen
# MAGIC
# MAGIC Notebooks ermöglichen die schrittweise Ausführung von Code in einzelnen Zellen. Innerhalb eines Notebooks können mehrere Programmiersprachen kombiniert werden. Zusätzlich können Benutzer Diagramme, Bilder und Markdown-Text einfügen, um ihren Code anschaulicher zu gestalten.
# MAGIC
# MAGIC **Eine Zelle ausführen**
# MAGIC
# MAGIC Führe die Zelle unten mit einer der folgenden Optionen aus:
# MAGIC
# MAGIC CTRL+ENTER oder CTRL+RETURN
# MAGIC
# MAGIC SHIFT+ENTER oder SHIFT+RETURN, um die Zelle auszuführen und direkt zur nächsten zu springen

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardsprache für ein Notebook festlegen
# MAGIC
# MAGIC Die Zelle oben führt einen Python-Befehl aus, da die aktuelle Standardsprache für dieses Notebook auf Python eingestellt ist.
# MAGIC
# MAGIC Databricks-Notebooks unterstützen Python, SQL, Scala und R. Eine Sprache kann beim Erstellen eines Notebooks festgelegt werden, diese Einstellung lässt sich jedoch jederzeit ändern.
# MAGIC
# MAGIC Die aktuelle Standardsprache wird rechts neben dem Titel des Notebooks oben auf der Seite angezeigt. In diesem Kurs verwenden wir eine Mischung aus SQL- und Python-Notebooks.
# MAGIC
# MAGIC Wir ändern nun die Standardsprache dieses Notebooks auf SQL.
# MAGIC
# MAGIC Schritte:
# MAGIC * Klicke oben neben dem Notizbuchtitel auf **Python**
# MAGIC * Wähle im sich öffnenden Dropdown-Menü **SQL** aus

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eine SQL-Zelle erstellen und ausführen
# MAGIC
# MAGIC * Markiere diese Zelle und drücke die B-Taste auf deiner Tastatur, um darunter eine neue Zelle zu erstellen.
# MAGIC * Kopiere anschließend den folgenden Code in die neue Zelle und führe sie aus:
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC
# MAGIC **HINWEIS:**
# MAGIC Es gibt verschiedene Methoden, um Zellen hinzuzufügen, zu verschieben oder zu löschen, einschließlich Optionen in der Benutzeroberfläche und Tastenkombinationen. Weitere Details findest du in der Dokumentation.
# MAGIC <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">docs</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Magic Commands
# MAGIC * Magic commands are specific to the Databricks notebooks
# MAGIC * They are very similar to magic commands found in comparable notebook products
# MAGIC * These are built-in commands that provide the same outcome regardless of the notebook's language
# MAGIC * A single percent (%) symbol at the start of a cell identifies a magic command
# MAGIC   * You can only have one magic command per cell
# MAGIC   * A magic command must be the first thing in a cell

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Language Magics
# MAGIC Language magic commands allow for the execution of code in languages other than the notebook's default. In this course, we'll see the following language magics:
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC Adding the language magic for the currently set notebook type is not necessary.
# MAGIC
# MAGIC When we changed the notebook language from Python to SQL above, existing cells written in Python had the <strong><code>&#37;python</code></strong> command added.
# MAGIC
# MAGIC **NOTE**: Rather than changing the default language of a notebook constantly, you should stick with a primary language as the default and only use language magics as necessary to execute code in another language.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### %run
# MAGIC * You can run a notebook from another notebook by using the magic command **%run**
# MAGIC * Notebooks to be run are specified with relative paths
# MAGIC * The referenced notebook executes as if it were part of the current notebook, so temporary views and other local declarations will be available from the calling notebook

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Uncommenting and executing the following cell will generate the following error:<br/>
# MAGIC **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC But we can declare it and a handful of other variables and functions by running this cell:

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

#dbutils.library.restartPython()
%run ./Includes/Classroom-Setup-01.2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The **`../Includes/Classroom-Setup-01.2`** notebook we referenced includes logic to create and **`USE`** a schema, as well as creating the temp view **`demo_temp_vw`**.
# MAGIC
# MAGIC We can see this temp view is now available in our current notebook session with the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We'll use this pattern of "setup" notebooks throughout the course to help configure the environment for lessons and labs.
# MAGIC
# MAGIC These "provided" variables, functions and other objects should be easily identifiable in that they are part of the **`DA`** object, which is an instance of **`DBAcademyHelper`**.
# MAGIC
# MAGIC With that in mind, most lessons will use variables derived from your username to organize files and schemas. 
# MAGIC
# MAGIC This pattern allows us to avoid collisions with other users in a shared workspace.
# MAGIC
# MAGIC The cell below uses Python to print some of those variables previously defined in this notebook's setup script:

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In addition to this, these same variables are "injected" into the SQL context so that we can use them in SQL statements.
# MAGIC
# MAGIC We will talk more about this later, but you can see a quick example in the following cell.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Note the subtle but important difference in the casing of the word **`da`** and **`DA`** in these two examples.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.schema_name}' as schema_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Databricks Utilities
# MAGIC Databricks notebooks include a **`dbutils`** object that provides a number of utility commands for configuring and interacting with the environment: <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC
# MAGIC Throughout this course, we'll occasionally use **`dbutils.fs.ls()`** to list out directories of files from Python cells.

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## display()
# MAGIC
# MAGIC When running SQL queries from cells, results will always be displayed in a rendered tabular format.
# MAGIC
# MAGIC When we have tabular data returned by a Python cell, we can call **`display`** to get the same type of preview.
# MAGIC
# MAGIC Here, we'll wrap the previous list command on our file system with **`display`**.

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The **`display()`** command has the following capabilities and limitations:
# MAGIC * Preview of results limited to 10,000 records or 2 MB, whichever is less.
# MAGIC * Provides button to download results data as CSV
# MAGIC * Allows rendering plots

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Downloading Notebooks
# MAGIC
# MAGIC There are a number of options for downloading either individual notebooks or collections of notebooks.
# MAGIC
# MAGIC Here, you'll go through the process to download this notebook as well as a collection of all the notebooks in this course.
# MAGIC
# MAGIC ### Download a Notebook
# MAGIC
# MAGIC Steps:
# MAGIC * At the top left of the notebook, Click the **File** option 
# MAGIC * From the menu that appears, hover over **Export** and then select **Source file**
# MAGIC
# MAGIC The notebook will download to your personal laptop. It will be named with the current notebook name and have the file extension for the default language. You can open this notebook with any file editor and see the raw contents of Databricks notebooks.
# MAGIC
# MAGIC These source files can be uploaded into any Databricks workspace.
# MAGIC
# MAGIC ### Download a Collection of Notebooks
# MAGIC
# MAGIC **NOTE**: The following instructions assume you have imported these materials using **Repos**.
# MAGIC
# MAGIC Steps:
# MAGIC * Click the  ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** on the left sidebar
# MAGIC   * This should give you a preview of the parent directories for this notebook
# MAGIC * On the left side of the directory preview around the middle of the screen, there should be a left arrow. Click this to move up in your file hierarchy.
# MAGIC * You should see a directory called **Data Engineer Learning Path**. Click the the down arrow/chevron to bring up a menu
# MAGIC * From the menu, hover over **Export** and select **DBC Archive**
# MAGIC
# MAGIC The DBC (Databricks Cloud) file that is downloaded contains a zipped collection of the directories and notebooks in this course. Users should not attempt to edit these DBC files locally, but they can be safely uploaded into any Databricks workspace to move or share notebook contents.
# MAGIC
# MAGIC **NOTE**: When downloading a collection of DBCs, result previews and plots will also be exported. When downloading source notebooks, only code will be saved.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Learning More
# MAGIC
# MAGIC We like to encourage you to explore the documentation to learn more about the various features of the Databricks platform and notebooks.
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">User Guide / Notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importing notebooks - Supported Formats</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Administration Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Cluster Configuration</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Release Notes</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## One more note! 
# MAGIC
# MAGIC At the end of each lesson you will see the following command, **`DA.cleanup()`**.
# MAGIC
# MAGIC This method drops lesson-specific schemas and working directories in an attempt to keep your workspace clean and maintain the immutability of each lesson.
# MAGIC
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
