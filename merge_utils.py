from __future__ import annotations

from io import BytesIO
from typing import Dict, Iterable, List, Literal, Optional, Tuple

import pandas as pd


def load_file(uploaded_file) -> pd.DataFrame:
    """
    Load a CSV or Excel file-like object into a pandas DataFrame.

    - Supports .csv and .xlsx (by extension).
    - Tries to infer encoding for CSV using pandas defaults.
    """
    if uploaded_file is None:
        raise ValueError("No file provided")

    name = getattr(uploaded_file, "name", "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file)

    # Fallback: try CSV first, then Excel
    try:
        uploaded_file.seek(0)
        return pd.read_csv(uploaded_file)
    except Exception:
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file)


def do_merge(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_a: str,
    key_b: str,
    how: Literal["inner", "left", "right", "outer"],
    suffixes: Tuple[str, str] = ("_A", "_B"),
) -> pd.DataFrame:
    """Perform a standard pandas merge with suffix handling."""
    if any(x is None or x == "" for x in [key_a, key_b]):
        raise ValueError("Both key_a and key_b must be provided")

    merged = pd.merge(
        df_a,
        df_b,
        left_on=key_a,
        right_on=key_b,
        how=how,
        suffixes=suffixes,
    )
    return merged


def anti_join(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_a: str,
    key_b: str,
    direction: Literal["A_not_in_B", "B_not_in_A"] = "A_not_in_B",
) -> pd.DataFrame:
    """
    Return rows in A not present in B (or vice versa) by the selected keys.
    """
    if direction == "A_not_in_B":
        mask = ~df_a[key_a].isin(df_b[key_b])
        return df_a.loc[mask].copy()
    elif direction == "B_not_in_A":
        mask = ~df_b[key_b].isin(df_a[key_a])
        return df_b.loc[mask].copy()
    else:
        raise ValueError("direction must be 'A_not_in_B' or 'B_not_in_A'")


def _with_suffix(column: str, suffix: str) -> str:
    return f"{column}{suffix}"


def filter_columns(
    df_merged: pd.DataFrame,
    cols_from_a: Optional[List[str]],
    cols_from_b: Optional[List[str]],
    key_a: Optional[str] = None,
    key_b: Optional[str] = None,
    suffixes: Tuple[str, str] = ("_A", "_B"),
) -> pd.DataFrame:
    """
    After a merge with suffixes, keep only selected columns.

    - For overlapping names, pandas adds suffixes. We mirror that here by
      first trying exact column names (already suffixed by merge), and if not
      found, we attempt to add the appropriate suffix.
    - Keys are included if they belong to the selected side.
    """
    if cols_from_a is None:
        cols_from_a = []
    if cols_from_b is None:
        cols_from_b = []

    suffix_a, suffix_b = suffixes

    selected_cols: List[str] = []

    def resolve(col: str, prefer_suffix: str) -> Optional[str]:
        # If already present as-is
        if col in df_merged.columns:
            return col
        # Try with suffix
        candidate = _with_suffix(col, prefer_suffix)
        if candidate in df_merged.columns:
            return candidate
        return None

    # A side
    for c in cols_from_a:
        resolved = resolve(c, suffix_a)
        if resolved is not None:
            selected_cols.append(resolved)

    # B side
    for c in cols_from_b:
        resolved = resolve(c, suffix_b)
        if resolved is not None:
            selected_cols.append(resolved)

    # Include keys when explicitly part of selections
    if key_a and key_a in cols_from_a:
        k = resolve(key_a, suffix_a)
        if k and k not in selected_cols:
            selected_cols.append(k)
    if key_b and key_b in cols_from_b:
        k = resolve(key_b, suffix_b)
        if k and k not in selected_cols:
            selected_cols.append(k)

    # Fallback: if nothing selected, keep all
    if not selected_cols:
        return df_merged

    return df_merged.loc[:, [c for c in selected_cols if c in df_merged.columns]].copy()


def build_summary_stats(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_a: str,
    key_b: str,
    how: Literal[
        "inner",
        "left",
        "right",
        "outer",
        "anti_A_vs_B",
        "anti_B_vs_A",
    ],
    result_df: pd.DataFrame,
) -> Dict[str, int]:
    """
    Build a dictionary of metrics for reporting.

    - Keys matched are counted at the key level (unique keys), not row-level.
    - For anti joins, report excluded rows as the count of rows in the excluded side.
    """
    rows_a = int(len(df_a))
    rows_b = int(len(df_b))
    rows_result = int(len(result_df))

    unique_keys_a = int(df_a[key_a].dropna().nunique())
    unique_keys_b = int(df_b[key_b].dropna().nunique())

    set_a = set(df_a[key_a].dropna().unique())
    set_b = set(df_b[key_b].dropna().unique())
    keys_intersection = len(set_a & set_b)

    excluded_rows = 0
    if how == "anti_A_vs_B":
        excluded_rows = int((~df_a[key_a].isin(df_b[key_b])).sum())
    elif how == "anti_B_vs_A":
        excluded_rows = int((~df_b[key_b].isin(df_a[key_a])).sum())

    return {
        "rows_a": rows_a,
        "rows_b": rows_b,
        "rows_result": rows_result,
        "unique_keys_a": unique_keys_a,
        "unique_keys_b": unique_keys_b,
        "keys_matched": int(keys_intersection),
        "excluded_rows": int(excluded_rows),
    }


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Return the Excel bytes for the given DataFrame (in-memory)."""
    buffer = BytesIO()
    # Use openpyxl engine
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer.read()


