#!/usr/bin/env python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.db import Database
from src import config

PROTECTED_VIEWS = {
    "view_monthly_trends", "view_date_dimension",
    "view_location_analysis", "view_company_analysis",
    "view_job_summary", "view_job_market_overview"
}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-backups", action="store_true", help="Delete backup views")
    parser.add_argument("--clean-old", action="store_true", help="Delete views older than N months")
    parser.add_argument("--months", type=int, default=6)
    args = parser.parse_args()

    db = Database(use_sqlite=False, mysql_config=config.DB_CONFIG)
    db.connect()
    cursor = db.conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT TABLE_NAME view_name, CREATE_TIME created
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'VIEW'
    """, (config.DB_CONFIG["database"],))
    all_views = cursor.fetchall()

    if args.clean_backups:
        for v in all_views:
            if v["view_name"].startswith("view_") and "_backup_" in v["view_name"]:
                cursor.execute(f"DROP VIEW IF EXISTS {v['view_name']}")
                print(f"Dropped backup view: {v['view_name']}")
        db.conn.commit()

    if args.clean_old:
        import datetime
        cutoff = datetime.date.today() - datetime.timedelta(days=30*args.months)
        for v in all_views:
            name = v["view_name"]
            if name in PROTECTED_VIEWS or "_backup_" in name:
                continue
            created = v["created"].date() if v["created"] else None
            if created and created < cutoff:
                resp = input(f"Drop {name} (created {created})? [y/N]: ")
                if resp.lower() == "y":
                    cursor.execute(f"DROP VIEW IF EXISTS {name}")
                    print(f"Dropped {name}")
        db.conn.commit()

    db.close()

if __name__ == "__main__":
    main()