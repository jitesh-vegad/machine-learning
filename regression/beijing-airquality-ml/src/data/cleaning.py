"""
Cleaning functions: nulls, types, outliers.
All functions take and return a DataFrame — no side effects.
"""

import pandas as pd


def parse_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combines year/month/day/hour columns into a single datetime column.
    Takes the raw combined DataFrame.
    Returns DataFrame with a new 'datetime' column and original time columns dropped.
    """
    df = df.copy()
    df["datetime"] = pd.to_datetime(df[["year", "month", "day", "hour"]])
    df = df.drop(columns=["year", "month", "day", "hour", "No"], errors="ignore")
    return df


def forward_fill_by_station(df: pd.DataFrame) -> pd.DataFrame:
    """
    Forward-fills missing sensor readings within each station group.
    Takes the DataFrame sorted by station and datetime.
    Returns DataFrame with nulls filled where a prior reading exists.
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    df = df.sort_values(["station", "datetime"])
    df[numeric_cols] = df.groupby("station")[numeric_cols].ffill()
    return df


def drop_remaining_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows where PM2.5 (the target) is still null after forward-fill.
    Takes the forward-filled DataFrame.
    Returns a clean DataFrame with no missing target values.
    """
    before = len(df)
    df = df.dropna(subset=["PM2.5"])
    dropped = before - len(df)
    print(f"Dropped {dropped:,} rows with null PM2.5 ({dropped/before:.1%})")
    return df
