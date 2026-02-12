import sqlite3
import math
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Optional

class Database:
    def __init__(self, use_sqlite=True, sqlite_path="job_data.db", mysql_config=None):
        self.use_sqlite = use_sqlite
        self.sqlite_path = sqlite_path
        self.mysql_config = mysql_config or {}
        self.conn = None

    def connect(self):
        if self.use_sqlite:
            self.conn = sqlite3.connect(self.sqlite_path)
        else:
            self.conn = mysql.connector.connect(**self.mysql_config)

    def close(self):
        if self.conn:
            self.conn.close()

    def create_tables(self):
        if self.use_sqlite:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT, source_id TEXT,
                    title TEXT, company TEXT, location TEXT,
                    salary_min REAL, salary_max REAL, salary_avg REAL,
                    is_remote INTEGER, seniority TEXT, category TEXT,
                    skills TEXT, post_date TEXT, scraped_date TEXT,
                    url TEXT,
                    UNIQUE(source, source_id)
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_post_date ON jobs(post_date)")
        else:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id INT AUTO_INCREMENT PRIMARY KEY,
                    source VARCHAR(50), source_id VARCHAR(100),
                    title VARCHAR(255), company VARCHAR(255), location VARCHAR(255),
                    salary_min DECIMAL(12,2), salary_max DECIMAL(12,2), salary_avg DECIMAL(12,2),
                    is_remote TINYINT DEFAULT 0,
                    seniority VARCHAR(50), category VARCHAR(100),
                    skills TEXT, post_date DATE, scraped_date DATETIME,
                    url TEXT,
                    UNIQUE KEY unique_job (source, source_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            try:
                cursor.execute("CREATE INDEX idx_post_date ON jobs(post_date)")
            except Error:
                pass
            self.conn.commit()
            cursor.close()

    def insert_jobs(self, jobs: List[Dict]) -> int:
        if not jobs:
            return 0
        inserted = 0

        if self.use_sqlite:
            for job in jobs:
                try:
                    self.conn.execute("""
                        INSERT OR IGNORE INTO jobs
                        (source, source_id, title, company, location,
                         salary_min, salary_max, salary_avg, is_remote,
                         seniority, category, skills, post_date, scraped_date, url)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, self._job_tuple(job))
                    inserted += 1
                except Exception:
                    continue
            self.conn.commit()
        else:
            cursor = self.conn.cursor()
            for idx, job in enumerate(jobs):
                try:
                    cursor.execute("""
                        INSERT IGNORE INTO jobs
                        (source, source_id, title, company, location,
                         salary_min, salary_max, salary_avg, is_remote,
                         seniority, category, skills, post_date, scraped_date, url)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, self._job_tuple(job))
                    inserted += 1
                except Exception as e:
                    if idx < 5:  # show first few errors
                        print(f"Insert failed for job {idx}: {e}")
                    continue
            self.conn.commit()
            cursor.close()
        return inserted

    def _job_tuple(self, job):
        # Handle skills: required_skills -> skills, NaN -> None
        skills = job.get("skills") or job.get("required_skills")
        if isinstance(skills, float) and math.isnan(skills):
            skills = None
        elif isinstance(skills, list):
            skills = ", ".join([str(s) for s in skills if s and not (isinstance(s, float) and math.isnan(s))])
        elif skills and not isinstance(skills, str):
            skills = str(skills)

        def clean_value(v):
            if isinstance(v, float) and math.isnan(v):
                return None
            return v

        return (
            clean_value(job.get("source")),
            clean_value(job.get("source_id")),
            clean_value(job.get("title")),
            clean_value(job.get("company")),
            clean_value(job.get("location")),
            clean_value(job.get("salary_min")),
            clean_value(job.get("salary_max")),
            clean_value(job.get("salary_avg")),
            clean_value(job.get("is_remote", 0)),
            clean_value(job.get("seniority")),
            clean_value(job.get("category")),
            skills,
            clean_value(job.get("post_date")),
            clean_value(job.get("scraped_at")),
            clean_value(job.get("url"))
        )