"""
Tests básicos para merge_utils.py
"""
import pytest
import pandas as pd
from merge_utils import (
    detect_key_columns,
    validate_data_before_merge,
    normalize_data,
    detect_duplicates,
    analyze_data_quality,
    do_merge,
    anti_join,
)


def test_detect_key_columns():
    """Test detección de columnas clave."""
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['A', 'B', 'C', 'D', 'E'],
        'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com', 'e@test.com'],
        'other': ['X', 'Y', 'Z', 'W', 'V']
    })
    
    detected = detect_key_columns(df)
    # 'id' y 'email' deberían estar en las primeras opciones
    assert len(detected) > 0
    assert 'id' in detected or 'email' in detected


def test_validate_data_before_merge():
    """Test validación de datos antes del merge."""
    df_a = pd.DataFrame({
        'key': [1, 2, 3, 4],
        'value_a': ['A', 'B', 'C', 'D']
    })
    
    df_b = pd.DataFrame({
        'key': [2, 3, 4, 5],
        'value_b': ['X', 'Y', 'Z', 'W']
    })
    
    result = validate_data_before_merge(df_a, df_b, 'key', 'key')
    
    assert 'errors' in result
    assert 'warnings' in result
    assert 'info' in result
    assert len(result['errors']) == 0  # No debería haber errores
    assert 'overlap' in result['info']
    assert result['info']['overlap'] == 3  # 2, 3, 4 están en ambas


def test_normalize_data():
    """Test normalización de datos."""
    df = pd.DataFrame({
        'col1': ['  A  ', '  B  ', '  C  '],
        'col2': ['X', 'Y', 'Z']
    })
    
    normalized = normalize_data(df, ['col1'])
    
    assert normalized['col1'].iloc[0] == 'A'
    assert normalized['col1'].iloc[1] == 'B'
    assert normalized['col1'].iloc[2] == 'C'


def test_detect_duplicates():
    """Test detección de duplicados."""
    df = pd.DataFrame({
        'key': [1, 2, 2, 3, 3, 3],
        'value': ['A', 'B', 'C', 'D', 'E', 'F']
    })
    
    result = detect_duplicates(df, 'key')
    
    assert result['has_duplicates'] == True
    assert result['duplicate_count'] == 5  # 2 aparece 2 veces, 3 aparece 3 veces = 5 duplicados


def test_analyze_data_quality():
    """Test análisis de calidad de datos."""
    df = pd.DataFrame({
        'col1': [1, 2, 3, None, 5],
        'col2': ['A', 'B', None, 'D', 'E'],
        'col3': [10.5, 20.3, 30.1, 40.2, 50.0]
    })
    
    quality = analyze_data_quality(df)
    
    assert quality['total_rows'] == 5
    assert quality['total_columns'] == 3
    assert 'null_counts' in quality
    assert quality['null_counts']['col1'] == 1
    assert quality['null_counts']['col2'] == 1


def test_do_merge():
    """Test merge básico."""
    df_a = pd.DataFrame({
        'key': [1, 2, 3],
        'value_a': ['A', 'B', 'C']
    })
    
    df_b = pd.DataFrame({
        'key': [2, 3, 4],
        'value_b': ['X', 'Y', 'Z']
    })
    
    result = do_merge(df_a, df_b, 'key', 'key', how='inner')
    
    assert len(result) == 2  # Solo 2 y 3 coinciden
    assert 'value_a' in result.columns
    assert 'value_b' in result.columns


def test_anti_join():
    """Test anti join."""
    df_a = pd.DataFrame({
        'key': [1, 2, 3, 4],
        'value_a': ['A', 'B', 'C', 'D']
    })
    
    df_b = pd.DataFrame({
        'key': [2, 3],
        'value_b': ['X', 'Y']
    })
    
    result = anti_join(df_a, df_b, 'key', 'key', direction='A_not_in_B')
    
    assert len(result) == 2  # 1 y 4 no están en B
    assert all(result['key'].isin([1, 4]))


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

