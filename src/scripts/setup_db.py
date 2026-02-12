#!/usr/bin/env python
import sys
import re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from src.storage.db import Database
from src.processing.cleaner import clean_jobs
from src import config

def clean_sql(sql: str) -> str:
    sql = re.sub(r'[^\x20-\x7E\n\t\r]', '', sql)
    sql = sql.replace('\r\n', '\n').replace('\r', '\n')
    sql = re.sub(r' +', ' ', sql)
    return sql.strip()

VIEWS = {
    "view_monthly_trends": clean_sql("""
        CREATE OR REPLACE VIEW view_monthly_trends AS
        SELECT
            YEAR(post_date) AS `job_year`,
            MONTH(post_date) AS `job_month`,
            DATE_FORMAT(post_date, '%Y-%m') AS `year_month`,
            COUNT(*) AS `job_count`,
            ROUND(AVG(salary_avg), 2) AS `avg_salary`,
            SUM(is_remote) AS `remote_jobs`
        FROM jobs
        WHERE post_date IS NOT NULL
        GROUP BY YEAR(post_date), MONTH(post_date), DATE_FORMAT(post_date, '%Y-%m')
        ORDER BY `job_year` DESC, `job_month` DESC
    """),
    "view_location_analysis": clean_sql("""
        CREATE OR REPLACE VIEW view_location_analysis AS
        SELECT
            location AS `location`,
            COUNT(*) AS `job_count`,
            ROUND(AVG(salary_avg), 2) AS `avg_salary`
        FROM jobs
        WHERE location != '' AND salary_avg IS NOT NULL
        GROUP BY location
        ORDER BY `job_count` DESC
    """),
    "view_company_analysis": clean_sql("""
        CREATE OR REPLACE VIEW view_company_analysis AS
        SELECT
            company AS `company`,
            COUNT(*) AS `job_count`,
            ROUND(AVG(salary_avg), 2) AS `avg_salary`,
            SUM(is_remote) AS `remote_jobs`
        FROM jobs
        WHERE company != '' AND salary_avg IS NOT NULL
        GROUP BY company
        HAVING `job_count` >= 2
        ORDER BY `job_count` DESC
    """),
}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Cleaned CSV to load")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables")
    parser.add_argument("--views-only", action="store_true", help="Only (re)create views, skip data load")
    args = parser.parse_args()

    db = Database(use_sqlite=False, mysql_config=config.DB_CONFIG)
    db.connect()

    if not args.views_only:
        if args.drop:
            cursor = db.conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS jobs")
            db.conn.commit()
            cursor.close()
            print("Dropped existing jobs table")

        db.create_tables()

        df = pd.read_csv(args.csv)
        print(f"CSV columns: {list(df.columns)}")

        if 'required_skills' in df.columns and 'skills' not in df.columns:
            df.rename(columns={'required_skills': 'skills'}, inplace=True)
            print("Renamed 'required_skills' column to 'skills'")

        df_clean = clean_jobs(df)
        jobs = df_clean.replace({np.nan: None}).to_dict(orient="records")
        inserted = db.insert_jobs(jobs)
        print(f"Inserted {inserted} records out of {len(jobs)}")
    else:
        print("Skipping data load, only (re)creating views...")

    cursor = db.conn.cursor()
    for name, sql in VIEWS.items():
        try:
            cursor.execute(sql)
            db.conn.commit()
            print(f"Created view: {name}")
        except Exception as e:
            print(f"Failed to create {name}: {e}")
    cursor.close()

    db.close()
    print("Done.")

if __name__ == "__main__":
    main()