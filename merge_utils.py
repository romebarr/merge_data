from __future__ import annotations

import logging
import re
from io import BytesIO
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple, Union

import pandas as pd

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_file_size(uploaded_file, max_size_mb: int = 100) -> bool:
    """Validate file size doesn't exceed maximum."""
    if uploaded_file is None:
        return False
    size_bytes = uploaded_file.size
    size_mb = size_bytes / (1024 * 1024)
    if size_mb > max_size_mb:
        raise ValueError(f"El archivo es demasiado grande ({size_mb:.2f} MB). Máximo permitido: {max_size_mb} MB")
    return True


def load_file(uploaded_file, max_size_mb: int = 100, preserve_format: bool = False) -> pd.DataFrame:
    """
    Load a CSV or Excel file-like object into a pandas DataFrame.

    - Supports .csv and .xlsx (by extension).
    - Tries to infer encoding for CSV using pandas defaults.
    - Validates file size.
    
    Args:
        uploaded_file: File-like object to read
        max_size_mb: Maximum file size in MB
        preserve_format: If True, reads all columns as text to preserve original formats.
                        If False, pandas will infer data types automatically.
    """
    if uploaded_file is None:
        raise ValueError("No file provided")

    # Validar tamaño
    validate_file_size(uploaded_file, max_size_mb)
    
    # Validar extensión
    name = getattr(uploaded_file, "name", "").lower()
    if not (name.endswith((".csv", ".xlsx", ".xls"))):
        raise ValueError("El archivo debe ser CSV o Excel (.csv, .xlsx, .xls)")

    try:
        if name.endswith(".csv"):
            if preserve_format:
                # Leer todo como texto para preservar formatos
                df = pd.read_csv(
                    uploaded_file, 
                    encoding='utf-8',
                    dtype=str,
                    keep_default_na=False  # No convertir strings vacíos a NaN
                )
                # Convertir strings vacíos a NaN después de leer
                df = df.replace('', pd.NA)
            else:
                df = pd.read_csv(uploaded_file, encoding='utf-8')
        elif name.endswith((".xlsx", ".xls")):
            if preserve_format:
                # Para Excel, leer todo como texto
                df = pd.read_excel(
                    uploaded_file,
                    dtype=str,
                    na_values=[''],  # Tratar strings vacíos como NaN
                    keep_default_na=False
                )
                # Convertir strings vacíos a NaN después de leer
                df = df.replace('', pd.NA)
            else:
                df = pd.read_excel(uploaded_file)
        else:
            # Fallback: try CSV first, then Excel
            uploaded_file.seek(0)
            try:
                if preserve_format:
                    df = pd.read_csv(
                        uploaded_file, 
                        encoding='utf-8',
                        dtype=str,
                        keep_default_na=False
                    )
                    df = df.replace('', pd.NA)
                else:
                    df = pd.read_csv(uploaded_file, encoding='utf-8')
            except Exception:
                uploaded_file.seek(0)
                if preserve_format:
                    df = pd.read_excel(
                        uploaded_file,
                        dtype=str,
                        keep_default_na=False
                    )
                    df = df.replace('', pd.NA)
                else:
                    df = pd.read_excel(uploaded_file)
        
        logger.info(f"Archivo cargado: {len(df)} filas, {len(df.columns)} columnas (preserve_format={preserve_format})")
        return df
    except Exception as e:
        logger.error(f"Error cargando archivo: {e}")
        raise ValueError(f"Error al leer el archivo: {str(e)}. Verifica que el formato sea válido.")


def do_merge(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_a: Union[str, List[str]],
    key_b: Union[str, List[str]],
    how: Literal["inner", "left", "right", "outer"],
    suffixes: Tuple[str, str] = ("_A", "_B"),
) -> pd.DataFrame:
    """Perform a standard pandas merge with suffix handling. Supports single or multiple keys."""
    if key_a is None or key_b is None:
        raise ValueError("Both key_a and key_b must be provided")
    
    # Convert to list if single key
    if isinstance(key_a, str):
        key_a = [key_a]
    if isinstance(key_b, str):
        key_b = [key_b]
    
    if len(key_a) != len(key_b):
        raise ValueError("key_a y key_b deben tener el mismo número de columnas")
    
    if any(x is None or x == "" for x in key_a + key_b):
        raise ValueError("Todas las columnas clave deben ser válidas")

    # pandas merge can handle both single string or list
    left_key = key_a if len(key_a) > 1 else key_a[0]
    right_key = key_b if len(key_b) > 1 else key_b[0]
    
    merged = pd.merge(
        df_a,
        df_b,
        left_on=left_key,
        right_on=right_key,
        how=how,
        suffixes=suffixes,
    )
    logger.info(f"Merge completado: {len(merged)} filas resultado")
    return merged


def anti_join(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_a: Union[str, List[str]],
    key_b: Union[str, List[str]],
    direction: Literal["A_not_in_B", "B_not_in_A"] = "A_not_in_B",
) -> pd.DataFrame:
    """
    Return rows in A not present in B (or vice versa) by the selected keys.
    Supports single or multiple keys.
    """
    # Convert to list if single key
    if isinstance(key_a, str):
        key_a = [key_a]
    if isinstance(key_b, str):
        key_b = [key_b]
    
    if len(key_a) != len(key_b):
        raise ValueError("key_a y key_b deben tener el mismo número de columnas")
    
    if direction == "A_not_in_B":
        # Create a set of tuples from B for efficient lookup
        if len(key_b) == 1:
            b_keys = set(df_b[key_b[0]].dropna().unique())
            mask = ~df_a[key_a[0]].isin(b_keys)
        else:
            # Multiple keys: create set of tuples
            b_keys = set(df_b[key_b].dropna().apply(tuple, axis=1).unique())
            mask = ~df_a[key_a].apply(tuple, axis=1).isin(b_keys)
        return df_a.loc[mask].copy()
    elif direction == "B_not_in_A":
        # Create a set of tuples from A for efficient lookup
        if len(key_a) == 1:
            a_keys = set(df_a[key_a[0]].dropna().unique())
            mask = ~df_b[key_b[0]].isin(a_keys)
        else:
            # Multiple keys: create set of tuples
            a_keys = set(df_a[key_a].dropna().apply(tuple, axis=1).unique())
            mask = ~df_b[key_b].apply(tuple, axis=1).isin(a_keys)
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


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Return the CSV bytes for the given DataFrame (in-memory)."""
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    return buffer.read()


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove invalid characters."""
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    return filename


def detect_key_columns(df: pd.DataFrame) -> List[str]:
    """
    Detect potential key columns by looking for common patterns.
    Returns list of column names sorted by likelihood of being a key.
    """
    candidates = []
    
    # Common key column names (case insensitive)
    key_patterns = [
        r'^id$', r'^id_', r'_id$', r'^codigo', r'^cod', r'^key', r'_key$',
        r'^email', r'^mail', r'^dni', r'^nif', r'^cedula', r'^passport',
        r'^sku', r'^producto_id', r'^cliente_id', r'^usuario_id'
    ]
    
    for col in df.columns:
        col_lower = str(col).lower()
        score = 0
        
        # Check for key patterns
        for pattern in key_patterns:
            if re.search(pattern, col_lower):
                score += 10
                break
        
        # Prefer columns with high uniqueness
        unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
        if unique_ratio > 0.9:  # High uniqueness
            score += 5
        elif unique_ratio > 0.7:
            score += 2
        
        # Prefer non-null columns
        null_ratio = df[col].isna().sum() / len(df) if len(df) > 0 else 1
        if null_ratio < 0.1:  # Low null ratio
            score += 3
        
        candidates.append((col, score))
    
    # Sort by score descending
    candidates.sort(key=lambda x: x[1], reverse=True)
    return [col for col, score in candidates if score > 0]


def validate_data_before_merge(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_a: Union[str, List[str]],
    key_b: Union[str, List[str]],
) -> Dict[str, Any]:
    """
    Validate data quality before merge and return warnings/issues.
    """
    warnings = []
    errors = []
    info = {}
    
    # Convert to list
    if isinstance(key_a, str):
        key_a = [key_a]
    if isinstance(key_b, str):
        key_b = [key_b]
    
    # Check if keys exist
    for key in key_a:
        if key not in df_a.columns:
            errors.append(f"Columna clave '{key}' no existe en Base A")
    for key in key_b:
        if key not in df_b.columns:
            errors.append(f"Columna clave '{key}' no existe en Base B")
    
    if errors:
        return {"errors": errors, "warnings": warnings, "info": info}
    
    # Check for duplicates in keys
    if len(key_a) == 1:
        dup_a = df_a[key_a[0]].duplicated().sum()
        if dup_a > 0:
            warnings.append(f"Base A tiene {dup_a:,} valores duplicados en la columna clave")
            info["duplicates_a"] = dup_a
    
    if len(key_b) == 1:
        dup_b = df_b[key_b[0]].duplicated().sum()
        if dup_b > 0:
            warnings.append(f"Base B tiene {dup_b:,} valores duplicados en la columna clave")
            info["duplicates_b"] = dup_b
    
    # Check for nulls in keys
    if len(key_a) == 1:
        nulls_a = df_a[key_a[0]].isna().sum()
        if nulls_a > 0:
            warnings.append(f"Base A tiene {nulls_a:,} valores nulos en la columna clave")
            info["nulls_a"] = nulls_a
    
    if len(key_b) == 1:
        nulls_b = df_b[key_b[0]].isna().sum()
        if nulls_b > 0:
            warnings.append(f"Base B tiene {nulls_b:,} valores nulos en la columna clave")
            info["nulls_b"] = nulls_b
    
    # Check data types compatibility
    if len(key_a) == 1 and len(key_b) == 1:
        type_a = df_a[key_a[0]].dtype
        type_b = df_b[key_b[0]].dtype
        if type_a != type_b:
            # Try to check if they're compatible
            try:
                # Sample check
                sample_a = df_a[key_a[0]].dropna().head(100)
                sample_b = df_b[key_b[0]].dropna().head(100)
                if len(sample_a) > 0 and len(sample_b) > 0:
                    # Try conversion
                    pd.to_numeric(sample_a, errors='coerce')
                    pd.to_numeric(sample_b, errors='coerce')
                    warnings.append(f"Tipos de datos diferentes en columnas clave (A: {type_a}, B: {type_b}). Puede afectar el merge.")
            except:
                pass
    
    # Calculate overlap
    if len(key_a) == 1 and len(key_b) == 1:
        set_a = set(df_a[key_a[0]].dropna().unique())
        set_b = set(df_b[key_b[0]].dropna().unique())
        overlap = len(set_a & set_b)
        info["overlap"] = overlap
        info["unique_a"] = len(set_a)
        info["unique_b"] = len(set_b)
        
        if overlap == 0:
            warnings.append("No hay coincidencias entre las columnas clave. El inner join resultará vacío.")
    
    return {"errors": errors, "warnings": warnings, "info": info}


def normalize_data(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Normalize data: strip whitespace, handle case, etc.
    """
    df = df.copy()
    
    if columns is None:
        columns = df.columns.tolist()
    
    for col in columns:
        if col in df.columns:
            # Convert to string and strip whitespace
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                # Replace empty strings with NaN
                df[col] = df[col].replace('', pd.NA)
    
    return df


def detect_duplicates(df: pd.DataFrame, key: Union[str, List[str]]) -> Dict[str, Any]:
    """Detect and return information about duplicates in key columns."""
    if isinstance(key, str):
        key = [key]
    
    duplicates = df[df.duplicated(subset=key, keep=False)].copy()
    
    return {
        "has_duplicates": len(duplicates) > 0,
        "duplicate_count": len(duplicates),
        "duplicate_rows": duplicates,
        "duplicate_keys": duplicates[key].drop_duplicates() if len(key) == 1 else duplicates[key].drop_duplicates()
    }


def analyze_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze data quality metrics for a dataframe."""
    quality = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "null_percentages": (df.isnull().sum() / len(df) * 100).to_dict(),
        "dtypes": df.dtypes.to_dict(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
    }
    
    # Numeric columns stats
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        quality["numeric_stats"] = df[numeric_cols].describe().to_dict()
    
    return quality


def aggregate_columns(
    df: pd.DataFrame,
    group_by: Union[str, List[str]],
    agg_dict: Dict[str, Union[str, List[str]]]
) -> pd.DataFrame:
    """Aggregate columns using specified functions."""
    if isinstance(group_by, str):
        group_by = [group_by]
    
    return df.groupby(group_by).agg(agg_dict).reset_index()


