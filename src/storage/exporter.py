import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

class Exporter:
    def __init__(self, output_dir: str = "data/exports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def to_csv(self, jobs: List[Dict], filename: Optional[str] = None) -> str:
        if not jobs:
            return ""
        df = pd.DataFrame(jobs)
        # flatten lists
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x)
        if not filename:
            filename = f"jobs_{datetime.now():%Y%m%d_%H%M%S}.csv"
        path = self.output_dir / filename
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return str(path)

    def to_json(self, jobs: List[Dict], filename: Optional[str] = None) -> str:
        if not jobs:
            return ""
        if not filename:
            filename = f"jobs_{datetime.now():%Y%m%d_%H%M%S}.json"
        path = self.output_dir / filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, default=str, ensure_ascii=False)
        return str(path)