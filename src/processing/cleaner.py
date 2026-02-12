import pandas as pd
import numpy as np
from pathlib import Path
from .salary import parse_salary_string

def clean_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names, parse salaries, dates, remote flag."""
    df = df.copy()

    # snake_case columns
    df.columns = (
        df.columns
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
    )
    df = df.loc[:, ~df.columns.duplicated()]

    # salary columns
    for col in ["salary_min", "salary_max", "salary_avg"]:
        if col in df.columns:
            df[col] = df[col].apply(parse_salary_string)

    # ensure required salary columns
    for col in ["salary_min", "salary_max", "salary_avg"]:
        if col not in df.columns:
            df[col] = np.nan

    # fix min > max
    mask = df["salary_min"].notna() & df["salary_max"].notna() & (df["salary_min"] > df["salary_max"])
    df.loc[mask, ["salary_min", "salary_max"]] = np.nan

    # impute avg from min/max
    avg_mask = df["salary_avg"].isna() & df["salary_min"].notna() & df["salary_max"].notna()
    df.loc[avg_mask, "salary_avg"] = (df.loc[avg_mask, "salary_min"] + df.loc[avg_mask, "salary_max"]) / 2

    # remote indicator
    if "is_remote" not in df.columns:
        df["is_remote"] = 0
    else:
        remote_map = {"true": 1, "false": 0, "yes": 1, "no": 0, "remote": 1, "onsite": 0}
        df["is_remote"] = (
            df["is_remote"].astype(str).str.lower()
            .map(remote_map)
            .fillna(0)
            .astype(int)
        )

    # dates
    for col in ["post_date", "scraped_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df