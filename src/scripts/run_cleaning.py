#!/usr/bin/env python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.processing.cleaner import clean_jobs
from src.storage.exporter import Exporter

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to raw CSV")
    parser.add_argument("-o", "--output", help="Output CSV path")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    df_clean = clean_jobs(df)

    exporter = Exporter()
    out_path = args.output or exporter.to_csv(df_clean.to_dict(orient="records"), filename=None)
    print(f"Cleaned {len(df_clean)} rows -> {out_path}")

if __name__ == "__main__":
    main()