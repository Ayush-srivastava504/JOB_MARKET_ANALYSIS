import re
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime

from ..processing.salary import parse_salary_string, format_salary_display, salary_midpoint

class BaseJobCollector(ABC):
    """Common fields and methods for all job collectors."""

    SKILL_KEYWORDS = {
        "python", "sql", "r", "java", "scala", "javascript", "aws", "azure",
        "docker", "kubernetes", "tensorflow", "pytorch", "pandas", "tableau",
    }

    @abstractmethod
    def fetch_jobs(self, keywords: str, max_results: int = 500) -> List[Dict]:
        pass

    def _safe_float(self, value) -> Optional[float]:
        return parse_salary_string(value)

    def _format_salary(self, min_val, max_val) -> str:
        return format_salary_display(min_val, max_val)

    def _salary_midpoint(self, min_val, max_val) -> Optional[float]:
        return salary_midpoint(min_val, max_val)

    def _standardize_company(self, name: str) -> str:
        if not name or name.lower() in ("unknown", "confidential", "private"):
            return "Unknown"
        # remove common suffixes
        name = re.sub(r"\s+(Inc\.?|LLC|Corp\.?|Corp|Corporation|Ltd\.?|Co\.?)$", "", name, flags=re.I)
        return name.strip()

    def _standardize_location(self, location: str) -> str:
        if not location:
            return "Remote"
        loc_lower = location.lower()
        if any(term in loc_lower for term in ["remote", "anywhere", "virtual", "telecommute", "work from home"]):
            return "Remote"
        # extract "City, ST"
        match = re.search(r"([A-Za-z\s]+),\s*([A-Z]{2})", location)
        if match:
            return f"{match.group(1).strip()}, {match.group(2)}"
        return location.strip()

    def _extract_skills(self, text: str) -> List[str]:
        if not text:
            return []
        text_lower = text.lower()
        return [skill for skill in self.SKILL_KEYWORDS if re.search(rf"\b{re.escape(skill)}\b", text_lower)]