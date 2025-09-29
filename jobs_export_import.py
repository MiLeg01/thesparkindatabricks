import os
import requests
import json

class JobExporter:
    def __init__(self, instance_url: str, token: str, export_dir: str = "databricks_jobs"):
        # Basis-URL und Token setzen
        self.instance_url = instance_url.rstrip("/")  # trailing slash entfernen
        self.headers = {"Authorization": f"Bearer {token}"}
        self.export_dir = export_dir

        # Export-Verzeichnis erstellen, falls nicht vorhanden
        os.makedirs(self.export_dir, exist_ok=True)

    def list_jobs(self):
        """Alle Jobs abrufen"""
        jobs_list_url = f"{self.instance_url}/api/2.1/jobs/list"
        resp = requests.get(jobs_list_url, headers=self.headers)

        if resp.status_code != 200:
            raise Exception(f"Fehler beim Abrufen der Jobs: {resp.text}")

        return resp.json().get("jobs", [])

    def get_job_details(self, job_id: int):
        """Details zu einem Job abrufen"""
        job_detail_url = f"{self.instance_url}/api/2.1/jobs/get?job_id={job_id}"
        resp = requests.get(job_detail_url, headers=self.headers)

        if resp.status_code != 200:
            raise Exception(f"Fehler beim Abrufen von Job {job_id}: {resp.text}")

        return resp.json()

    def export_jobs(self):
        """Alle Jobs exportieren und als JSON speichern"""
        jobs = self.list_jobs()
        print(f"Gefundene Jobs: {len(jobs)}")

        for job in jobs:
            job_id = job["job_id"]
            job_data = self.get_job_details(job_id)

            # Jobnamen aus den Settings nehmen und für Dateinamen säubern
            job_name = job["settings"]["name"].replace(" ", "_").replace("/", "_")
            filename = os.path.join(self.export_dir, f"job_{job_id}_{job_name}.json")

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(job_data, f, indent=2, ensure_ascii=False)

            print(f"Job exportiert: {filename}")

class JobImporter:
    def __init__(self, instance_url: str, token: str, import_dir: str = "databricks_jobs"):
        self.instance_url = instance_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.import_dir = import_dir

    def import_jobs(self):
        """Alle Jobs aus JSON-Dateien wieder anlegen"""
        files = [f for f in os.listdir(self.import_dir) if f.endswith(".json")]
        print(f"Gefundene Job-Definitionen: {len(files)}")

        for filename in files:
            path = os.path.join(self.import_dir, filename)
            with open(path, "r", encoding="utf-8") as f:
                job_data = json.load(f)

            # nur die Settings an API senden
            job_settings = job_data.get("settings")
            if not job_settings:
                print(f"Keine Settings in {filename}, überspringe")
                continue

            resp = requests.post(
                f"{self.instance_url}/api/2.1/jobs/create",
                headers=self.headers,
                json=job_settings,
            )

            if resp.status_code == 200:
                new_job_id = resp.json().get("job_id")
                print(f"Job aus {filename} erstellt (ID: {new_job_id})")
            else:
                print(f"Fehler bei {filename}: {resp.text}")

if __name__ == "__main__":
    # Konfiguration
    DATABRICKS_INSTANCE = ""
    TOKEN = ''

    is_export = False
    is_import = True

    if is_export:
        exporter = JobExporter(DATABRICKS_INSTANCE, TOKEN)
        exporter.export_jobs()

    if is_import:
        importer = JobImporter(DATABRICKS_INSTANCE, TOKEN)
        importer.import_jobs()
