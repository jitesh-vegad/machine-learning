"""
Functions to load raw CSVs, merge them, and load the processed dataset.
"""

import pandas as pd
from pathlib import Path

RAW_DIR = Path(__file__).parents[2] / "data" / "raw"
PROCESSED_DIR = Path(__file__).parents[2] / "data" / "processed"
COMBINED_CSV = PROCESSED_DIR / "combined.csv"


def load_raw_data() -> pd.DataFrame:
    """
    Reads all 12 raw station CSVs and concatenates them into one DataFrame.
    Takes nothing — paths are resolved relative to this file.
    Returns a single DataFrame with a 'station' column identifying each source file.
    """
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

    frames = []
    for f in files:
        df = pd.read_csv(f)
        # Station name is embedded in filename: PRSA_Data_<StationName>_...csv
        station_name = f.stem.split("_")[2]
        df["station"] = station_name
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    print(f"Loaded {len(files)} files — {len(combined):,} rows total")
    return combined


def save_merged_csv(df: pd.DataFrame) -> None:
    """
    Saves the merged DataFrame to data/processed/combined.csv.
    Takes the combined DataFrame.
    Returns nothing — writes to disk.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(COMBINED_CSV, index=False)
    print(f"Saved → {COMBINED_CSV}")


def load_processed_data() -> pd.DataFrame:
    """
    Loads combined.csv from data/processed/.
    Takes nothing — path is fixed.
    Returns the processed DataFrame for use in all downstream stages.
    """
    if not COMBINED_CSV.exists():
        raise FileNotFoundError("combined.csv not found. Run pipeline.py first.")
    return pd.read_csv(COMBINED_CSV)
