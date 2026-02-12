#!/usr/bin/env python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline import JobPipeline

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("keywords", nargs="?", default="data scientist")
    parser.add_argument("--max", type=int, default=500)
    args = parser.parse_args()

    pipeline = JobPipeline(args.keywords, args.max)
    result = pipeline.run()

    print("\n--- Pipeline finished ---")
    print(f"Jobs collected: {result.get('total', 0)}")
    print(f"Stored in DB: {result.get('stored', 0)}")
    print(f"Exported to: {result.get('csv')}")

if __name__ == "__main__":
    main()