import pandas as pd
import numpy as np

# ==============================
# 1. Load data
# ==============================
df = pd.read_csv("job_data_excel_20251222_000914.csv")

# ==============================
# 2. Normalize column names
# ==============================
df.columns = (
    df.columns
      .str.lower()
      .str.strip()
      .str.replace(" ", "_")
)

# ==============================
# 3. Remove duplicate columns
# ==============================
df = df.loc[:, ~df.columns.duplicated()]

# ==============================
# 4. Identify salary-related columns
# ==============================
salary_cols = [col for col in df.columns if "salary" in col]

# ==============================
# 5. Clean salary columns safely
# ==============================
for col in salary_cols:
    series = df[col].astype(str)

    series = series.str.replace(r"[\$,₹,]", "", regex=True)
    series = pd.to_numeric(series, errors="coerce")

    df[col] = series

# ==============================
# 6. Ensure required salary columns exist
# ==============================
for col in ["salary_avg", "salary_min", "salary_max"]:
    if col not in df.columns:
        df[col] = np.nan
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ==============================
# 7. Salary sanity check
# ==============================
invalid_salary_mask = df["salary_min"] > df["salary_max"]
df.loc[invalid_salary_mask, ["salary_min", "salary_max"]] = np.nan

# ==============================
# 8. Fill salary_avg from min/max
# ==============================
fill_mask = (
    df["salary_avg"].isna()
    & df["salary_min"].notna()
    & df["salary_max"].notna()
)

df.loc[fill_mask, "salary_avg"] = (
    df.loc[fill_mask, "salary_min"] + df.loc[fill_mask, "salary_max"]
) / 2

# ==============================
# 9. Fill remaining salary nulls
# ==============================
df["salary_avg"] = df["salary_avg"].fillna(df["salary_avg"].median())
df["salary_min"] = df["salary_min"].fillna(df["salary_min"].median())
df["salary_max"] = df["salary_max"].fillna(df["salary_max"].median())

# ==============================
# 10. Handle is_remote column
# ==============================
if "is_remote" in df.columns:
    df["is_remote"] = (
        df["is_remote"]
        .astype(str)
        .str.lower()
        .map({"true": 1, "false": 0, "1": 1, "0": 0})
        .fillna(0)
        .astype(int)
    )
else:
    df["is_remote"] = 0

# ==============================
# 11. Parse date columns
# ==============================
for date_col in ["post_date", "scraped_date"]:
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

# ==============================
# 12. Select final clean columns
# ==============================
final_columns = [
    "title",
    "company",
    "location",
    "salary_avg",
    "salary_min",
    "salary_max",
    "is_remote",
    "seniority",
    "category",
    "skills",
    "post_date",
    "scraped_date",
]

existing_columns = [col for col in final_columns if col in df.columns]
clean_df = df[existing_columns].copy()

# ==============================
# 13. Save cleaned dataset
# ==============================
clean_df.to_csv("cleaned_jobs_simple.csv", index=False)

print(f"Saved {len(clean_df)} records to cleaned_jobs_simple.csv")

print(f"SUCCESS: Saved {len(clean_df)} rows to cleaned_jobs_simple.csv")
# ==============================