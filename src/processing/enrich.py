import re
from typing import Dict, List

SENIORITY_KEYWORDS = {
    "entry": ["entry", "junior", "jr.", "associate", "graduate", "trainee", "i", "level i"],
    "mid": ["mid", "intermediate", "ii", "level ii"],
    "senior": ["senior", "sr.", "lead", "principal", "staff", "iii", "iv", "v", "level iii"],
    "manager": ["manager", "mgr", "supervisor", "team lead"],
    "director": ["director", "head of", "vp", "vice president"],
    "executive": ["chief", "cfo", "cto", "ceo", "president"],
}

CATEGORY_KEYWORDS = {
    "data_science": ["data scientist", "machine learning", "ai", "ml engineer"],
    "data_analyst": ["data analyst", "business analyst", "analytics"],
    "data_engineer": ["data engineer", "etl", "data pipeline"],
    "software": ["software", "developer", "engineer", "programmer"],
    "cloud": ["cloud", "devops", "sre"],
    "bi": ["business intelligence", "tableau", "powerbi"],
    "research": ["research", "scientist", "phd"],
    "government": ["government"],
}

def infer_seniority(title: str) -> str:
    if not title:
        return "Not Specified"
    title_lower = title.lower()
    for level, keywords in SENIORITY_KEYWORDS.items():
        if any(k in title_lower for k in keywords):
            return level.capitalize()
    return "Not Specified"

def infer_category(job: Dict) -> str:
    if job.get("source") == "usajobs":
        return "Government"
    title = str(job.get("title", "")).lower()
    desc = str(job.get("description", "")).lower()
    combined = f"{title} {desc}"
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(k in combined for k in keywords):
            return cat.replace("_", " ").title()
    return "Technology"

def extract_experience_years(description: str) -> int | None:
    if not description:
        return None
    patterns = [r"(\d+)\+?\s*years?", r"(\d+)\+?\s*yrs?", r"experience.*?(\d+).*?years"]
    for pattern in patterns:
        match = re.search(pattern, description.lower())
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None

def enrich_jobs(jobs: List[Dict]) -> List[Dict]:
    enriched = []
    for job in jobs:
        job = job.copy()
        job["seniority"] = infer_seniority(job.get("title", ""))
        job["category"] = infer_category(job)
        job["experience_years"] = extract_experience_years(job.get("description", ""))
        enriched.append(job)
    return enriched