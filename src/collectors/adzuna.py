import requests
import time
from datetime import datetime
from typing import List, Dict, Optional

from .. import config
from .base import BaseJobCollector

class AdzunaCollector(BaseJobCollector):
    def __init__(self):
        self.app_id = config.ADZUNA_APP_ID
        self.app_key = config.ADZUNA_APP_KEY
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "JobDataCollector/1.0"})

    def fetch_jobs(self, keywords: str, max_results: int = 500, country: str = "us") -> List[Dict]:
        jobs = []
        per_page = 50
        pages = (max_results // per_page) + 1

        for page in range(1, pages + 1):
            url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
            params = {
                "app_id": self.app_id,
                "app_key": self.app_key,
                "what": keywords,
                "results_per_page": per_page,
                "content-type": "application/json",
            }

            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                break

            data = resp.json()
            raw_jobs = data.get("results", [])
            if not raw_jobs:
                break

            jobs.extend(self._standardize(raw_jobs))
            time.sleep(0.5)

            if len(jobs) >= max_results:
                jobs = jobs[:max_results]
                break

        return jobs

    def _standardize(self, raw_jobs: List[Dict]) -> List[Dict]:
        standardized = []
        for job in raw_jobs:
            salary_min = self._safe_float(job.get("salary_min"))
            salary_max = self._safe_float(job.get("salary_max"))

            std = {
                "source": "adzuna",
                "source_id": str(job.get("id", "")),
                "title": job.get("title", "").strip(),
                "company": job.get("company", {}).get("display_name", "Unknown"),
                "company_standardized": self._standardize_company(job.get("company", {}).get("display_name")),
                "location": job.get("location", {}).get("display_name", "Remote"),
                "location_standardized": self._standardize_location(job.get("location", {}).get("display_name")),
                "description": job.get("description", ""),
                "url": job.get("redirect_url"),
                "post_date": job.get("created", "")[:10],
                "scraped_at": datetime.now().isoformat(),
                "salary_display": self._format_salary(salary_min, salary_max),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_avg": self._salary_midpoint(salary_min, salary_max),
                "job_type": job.get("contract_type", "Full-time"),
                "category": job.get("category", {}).get("label", ""),
                "required_skills": self._extract_skills(job.get("description", "")),
                "is_remote": 1 if "remote" in str(job.get("location", {})).lower() else 0,
            }
            standardized.append(std)
        return standardized