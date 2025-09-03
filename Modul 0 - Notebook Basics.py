# Databricks notebook source
# MAGIC %md
# MAGIC # Menti Umfrage

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC https://www.menti.com/aldhq2dr1ua6
# MAGIC
# MAGIC ![](./Helper/mentimeter_qr_code.png)

# COMMAND ----------

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
# MAGIC * Magic commands sind spezifisch für Databricks-Notebooks.
# MAGIC * Es handelt sich um eingebaute Befehle, die unabhängig von der im Notebook eingestellten Sprache dasselbe Ergebnis liefern.
# MAGIC * Ein einzelnes Prozentzeichen (%) am Anfang einer Zelle kennzeichnet einen Magic command.
# MAGIC   * Pro Zelle darf nur ein Magic command verwendet werden.
# MAGIC   * Ein Magic command muss immer als erstes in einer Zelle stehen.

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
# MAGIC
# MAGIC ### Language Magics
# MAGIC
# MAGIC Language magic commands ermöglichen die Ausführung von Code in anderen Sprachen als der im Notebook eingestellten Standardsprache. In diesem Kurs verwenden wir folgende language magics:
# MAGIC
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC Das Hinzufügen des language magic für die aktuell eingestellte Notebook-Sprache ist nicht erforderlich.
# MAGIC
# MAGIC Als wir oben die Standardsprache des Notebooks von Python auf SQL geändert haben, wurde bei den bereits vorhandenen Zellen in Python automatisch das <strong><code>%python</code></strong>-Kommando hinzugefügt.
# MAGIC
# MAGIC HINWEIS: Anstatt ständig die Standardsprache eines Notebooks zu ändern, solltest du dich für eine Hauptsprache entscheiden und nur dann language magics einsetzen, wenn du Code in einer anderen Sprache ausführen möchtest.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### %run
# MAGIC
# MAGIC * Du kannst ein Notebook aus einem anderen Notebook heraus ausführen, indem du den magic command %run verwendest.
# MAGIC * Die auszuführenden Notebooks werden dabei über relative Pfade angegeben.
# MAGIC * Das referenzierte Notebook wird so ausgeführt, als wäre es Teil des aktuellen Notebooks. Dadurch stehen temporäre Views und andere lokale Deklarationen auch im aufrufenden Notebook zur Verfügung.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view

# COMMAND ----------

# MAGIC %run ./Helper/NotebookRunMagic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Databricks Utilities
# MAGIC
# MAGIC In Databricks-Notebooks gibt es ein integriertes Hilfsmodul namens Databricks Utilities (**dbutils**).
# MAGIC
# MAGIC Es ist kein Teil von Apache Spark, sondern ein **zusätzliches Paket, das von Databricks bereitgestellt wird**, um gängige Aufgaben innerhalb des Workspaces zu erleichtern.
# MAGIC
# MAGIC Mit dbutils kannst du:
# MAGIC * auf das Databricks File System (DBFS) zugreifen - dbutils.fs
# MAGIC * Widgets (Parameter) erstellen, um Notebooks interaktiv zu machen - dbutils.widgets
# MAGIC * andere Notebooks ausführen und miteinander verketten - dbutils.notebooks
# MAGIC * Secrets (geheime Daten) sicher verwalten - dbutils.secrets
# MAGIC * Bibliotheken programmatisch installieren oder verwalten - dbutils.library
# MAGIC * auf Job-Informationen zugreifen dbutils.jobs
# MAGIC
# MAGIC So lassen sich Databricks-Plattformfunktionen direkt aus dem Code nutzen, ohne immer auf die Benutzeroberfläche wechseln zu müssen.

# COMMAND ----------

# Zeige die verfügbaren Hauptfunktionen von dbutils
#dbutils.help()

# Hilfe für die Dateisystem-Utilities anzeigen
dbutils.fs.help()

dbutils.fs.ls('/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### %run vs dbutils.notebook.run()

# COMMAND ----------

# ------------------------------
# %run
# ------------------------------
%run ./Helper/NotebookRunMagic

# Zugriff auf TempView und Funktion
print("Durchschnitt Salary (aus run):", durchschnitt_salary())
spark.sql("SELECT * FROM temp_view").show()


# COMMAND ----------

# ------------------------------
# dbutils.notebook.run()
# ------------------------------
result = dbutils.notebook.run("./Helper/NotebookDBUtilsRun", 60, {"min_salary": "5500"})
print("Ergebnis aus dbutils.notebook.run():", result)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Erklärung
# MAGIC
# MAGIC **%run**
# MAGIC * Alles aus HilfsNotebookMagic ist direkt im aktuellen Notebook verfügbar (zahl und begruessung).
# MAGIC * Keine Parameterübergabe nötig.
# MAGIC
# MAGIC **dbutils.notebook.run()**
# MAGIC * Das Notebook HilfsNotebookRun läuft in einem eigenen Kontext.
# MAGIC * Wir übergeben einen Parameter ({"name": "Bob"}) und bekommen ein Ergebnis zurück (dbutils.notebook.exit()).
# MAGIC * Variablen aus dem Hilfsnotebook sind nicht automatisch im Hauptnotebook verfügbar.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## display()
# MAGIC
# MAGIC Wenn SQL-Abfragen aus Zellen ausgeführt werden, werden die Ergebnisse immer in einem gerenderten Tabellenformat angezeigt.
# MAGIC
# MAGIC Wenn tabellarische Daten von einer Python-Zelle zurückgegeben werden, können wir display aufrufen, um die gleiche Art von Vorschau zu erhalten.
# MAGIC
# MAGIC Hier werden wir den vorherigen list-Befehl auf unserem Dateisystem mit display umschließen.

# COMMAND ----------

dbutils.fs.ls('/')

display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md
# MAGIC Der **`display()`** Befehl hat folgende Möglichkeiten und Einschränkungen:
# MAGIC * Vorschau der Ergebnisse ist begrenzt auf 10.000 Datensätze oder 2 MB, jenachdem was kleiner ist.
# MAGIC * Bietet eine Schaltfläche, um die Ergebnisdaten als CSV-Datei herunterzuladen.
# MAGIC * Ermöglicht das Darstellen von Diagrammen.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Download von Notebooks
# MAGIC
# MAGIC Es gibt mehrere Möglichkeiten, entweder einzelne Notebooks oder Sammlungen von Notebooks herunterzuladen.
# MAGIC
# MAGIC ### Download eines Notebooks
# MAGIC
# MAGIC **Schritte:**
# MAGIC * Klicke oben links im Notebook auf die Option File
# MAGIC * Fahre im angezeigten Menü über Export und wähle dann Source file aus
# MAGIC
# MAGIC Das Notebook wird auf deinen Laptop heruntergeladen. Es erhält den aktuellen Notizbuchnamen sowie die Dateiendung der Standardsprache. Du kannst dieses Notebook mit jedem Texteditor öffnen und die Rohinhalte eines Databricks-Notebooks einsehen.
# MAGIC
# MAGIC Diese Quelldateien können in jeden Databricks-Workspace hochgeladen werden.
# MAGIC
# MAGIC ### Download eines Ordners bzw. Repos
# MAGIC
# MAGIC **Schritte:**
# MAGIC * Öffne Workspace in der linken Seitenleiste.
# MAGIC * Navigiere zum Ordner, der dein gesamtes Projekt enthält.
# MAGIC * Öffne am Ordner das Kontextmenü (Rechtsklick oder Chevron).
# MAGIC * Wähle Export → DBC Archive.
# MAGIC * Bestätige und lade die .dbc-Datei herunter.
# MAGIC
# MAGIC **Was steckt im DBC?**
# MAGIC * Ein gezipptes Paket deiner Ordnerstruktur und Notebooks.
# MAGIC * Beim Export von DBC-Sammlungen werden auch Result-Previews und Plots mitgenommen.
# MAGIC * Zum Bearbeiten ist DBC nicht gedacht; es lässt sich aber sicher in jedem Workspace wieder importieren.
# MAGIC
