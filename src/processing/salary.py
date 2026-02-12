import re
from typing import Optional, Tuple

def parse_salary_string(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(value)))
    except (ValueError, TypeError):
        return None

def format_salary_display(min_val: Optional[float], max_val: Optional[float]) -> str:
    if min_val and max_val:
        return f"${min_val:,.0f} - ${max_val:,.0f}"
    if min_val:
        return f"From ${min_val:,.0f}"
    if max_val:
        return f"Up to ${max_val:,.0f}"
    return "Not specified"

def salary_midpoint(min_val: Optional[float], max_val: Optional[float]) -> Optional[float]:
    if min_val and max_val:
        return (min_val + max_val) / 2
    return min_val or max_val