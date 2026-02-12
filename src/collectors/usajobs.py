import requests
from datetime import datetime
from typing import List, Dict

from .. import config
from .base import BaseJobCollector

class USAJobsCollector(BaseJobCollector):
    def __init__(self):
        self.api_key = config.USAJOBS_API_KEY
        self.user_email = config.USAJOBS_USER_EMAIL
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_email,
            "Authorization-Key": self.api_key,
            "Host": "data.usajobs.gov",
            "Accept": "application/json",
        })

    def fetch_jobs(self, keywords: str, max_results: int = 500) -> List[Dict]:
        jobs = []
        page = 1
        per_page = 100

        while len(jobs) < max_results:
            url = "https://data.usajobs.gov/api/search"
            params = {
                "Keyword": keywords,
                "ResultsPerPage": per_page,
                "Page": page,
                "DatePosted": 30,
            }

            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                break

            data = resp.json()
            items = data.get("SearchResult", {}).get("SearchResultItems", [])
            if not items:
                break

            jobs.extend(self._standardize(items))
            page += 1

            total = data.get("SearchResult", {}).get("SearchResultCountAll", 0)
            if len(jobs) >= total or len(jobs) >= max_results:
                break

        return jobs[:max_results]

    def _standardize(self, items: List[Dict]) -> List[Dict]:
        standardized = []
        for item in items:
            obj = item.get("MatchedObjectDescriptor", {})
            salary_min, salary_max = self._extract_salary(obj)

            std = {
                "source": "usajobs",
                "source_id": obj.get("PositionID", ""),
                "title": obj.get("PositionTitle", ""),
                "company": obj.get("OrganizationName", "U.S. Government"),
                "company_standardized": self._standardize_company(obj.get("OrganizationName")),
                "location": self._extract_location(obj),
                "location_standardized": self._standardize_location(self._extract_location(obj)),
                "description": self._extract_description(obj),
                "url": obj.get("PositionURI"),
                "post_date": obj.get("PublicationStartDate", "")[:10],
                "scraped_at": datetime.now().isoformat(),
                "salary_display": self._format_salary(salary_min, salary_max),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_avg": self._salary_midpoint(salary_min, salary_max),
                "job_type": self._extract_job_type(obj),
                "required_skills": self._extract_skills(self._extract_description(obj)),
                "is_remote": 1 if self._is_remote(obj) else 0,
            }
            standardized.append(std)
        return standardized

    def _extract_salary(self, obj):
        remuneration = obj.get("PositionRemuneration", [{}])
        if remuneration:
            min_sal = self._safe_float(remuneration[0].get("MinimumRange"))
            max_sal = self._safe_float(remuneration[0].get("MaximumRange"))
            return min_sal, max_sal
        return None, None

    def _extract_location(self, obj):
        locs = obj.get("PositionLocation", [])
        if locs:
            return locs[0].get("LocationName", "Multiple Locations")
        return "Multiple Locations"

    def _extract_description(self, obj):
        parts = []
        if obj.get("QualificationSummary"):
            parts.append(f"Qualifications: {obj['QualificationSummary']}")
        duties = obj.get("MajorDuties", [])
        if duties:
            parts.append("Major Duties:")
            parts.extend([f"- {d}" for d in duties])
        return "\n\n".join(parts)

    def _extract_job_type(self, obj):
        schedule = obj.get("PositionSchedule", [{}])
        return schedule[0].get("Name", "Full-time") if schedule else "Full-time"

    def _is_remote(self, obj):
        locs = obj.get("PositionLocation", [])
        for loc in locs:
            if "remote" in loc.get("LocationName", "").lower():
                return True
        details = obj.get("UserArea", {}).get("Details", {})
        return bool(details.get("TeleworkEligible", False))