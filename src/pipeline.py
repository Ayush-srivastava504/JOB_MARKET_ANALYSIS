from .collectors.adzuna import AdzunaCollector
from .collectors.usajobs import USAJobsCollector
from .processing.enrich import enrich_jobs
from .storage.db import Database
from .storage.exporter import Exporter

class JobPipeline:
    def __init__(self, keywords: str, max_results: int = 500):
        self.keywords = keywords
        self.max_results = max_results
        self.collectors = [AdzunaCollector(), USAJobsCollector()]

    def run(self):
        all_jobs = []
        for collector in self.collectors:
            try:
                jobs = collector.fetch_jobs(self.keywords, self.max_results // len(self.collectors))
                all_jobs.extend(jobs)
                print(f"{collector.__class__.__name__}: {len(jobs)} jobs")
            except Exception as e:
                print(f"Failed {collector.__class__.__name__}: {e}")

        if not all_jobs:
            return {"error": "no jobs collected"}

        enriched = enrich_jobs(all_jobs)

        db = Database(use_sqlite=True)
        db.connect()
        db.create_tables()
        stored = db.insert_jobs(enriched)
        db.close()

        exp = Exporter()
        csv_path = exp.to_csv(enriched)
        json_path = exp.to_json(enriched)

        return {
            "total": len(enriched),
            "stored": stored,
            "csv": csv_path,
            "json": json_path
        }
        