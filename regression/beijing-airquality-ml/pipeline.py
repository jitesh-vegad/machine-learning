"""
Entry point: run once to merge 12 raw CSVs → data/processed/combined.csv
Usage: python pipeline.py
"""

from src.data.data_loader import load_raw_data, save_merged_csv

if __name__ == "__main__":
    df = load_raw_data()
    save_merged_csv(df)
    print("Pipeline complete.")
