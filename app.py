from __future__ import annotations

import streamlit as st
import pandas as pd

from merge_utils import (
    load_file,
    do_merge,
    anti_join,
    filter_columns,
    build_summary_stats,
    to_excel_bytes,
)


st.set_page_config(page_title="Data Merge Tool", layout="wide")


def init_state():
    defaults = {
        "df_a": None,
        "df_b": None,
        "join_key_a": None,
        "join_key_b": None,
        "join_type": "inner",
        "cols_from_a": None,
        "cols_from_b": None,
        "resultado": None,
        "suffixes": ("_A", "_B"),
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_state()


st.title("App de Cruce de Bases (Streamlit + pandas)")


# Paso 1: Subida de archivos
st.header("Paso 1: Subir bases de datos")
col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Base A")
    uploaded_a = st.file_uploader(
        "Sube archivo A (.csv o .xlsx)", type=["csv", "xlsx"], key="upload_a"
    )
    if uploaded_a is not None:
        try:
            df_a = load_file(uploaded_a)
            st.session_state["df_a"] = df_a
            st.write(f"Filas: {len(df_a):,}")
            st.write("Vista previa (10 filas):")
            st.dataframe(df_a.head(10))
            st.caption("Columnas disponibles en A:")
            st.write(list(df_a.columns))
        except Exception as e:
            st.error(f"Error leyendo Base A: {e}")

with col_b:
    st.subheader("Base B")
    uploaded_b = st.file_uploader(
        "Sube archivo B (.csv o .xlsx)", type=["csv", "xlsx"], key="upload_b"
    )
    if uploaded_b is not None:
        try:
            df_b = load_file(uploaded_b)
            st.session_state["df_b"] = df_b
            st.write(f"Filas: {len(df_b):,}")
            st.write("Vista previa (10 filas):")
            st.dataframe(df_b.head(10))
            st.caption("Columnas disponibles en B:")
            st.write(list(df_b.columns))
        except Exception as e:
            st.error(f"Error leyendo Base B: {e}")


# Paso 2: Selección de llaves
st.header("Paso 2: Seleccionar columnas clave")
df_a = st.session_state.get("df_a")
df_b = st.session_state.get("df_b")

col1, col2 = st.columns(2)
with col1:
    key_a = None
    if df_a is not None:
        key_a = st.selectbox(
            "¿Cuál es la columna clave de Base A?",
            options=list(df_a.columns),
            index=0 if len(df_a.columns) > 0 else None,
            key="select_key_a",
        )
        st.session_state["join_key_a"] = key_a
    else:
        st.info("Sube Base A para elegir su columna clave.")

with col2:
    key_b = None
    if df_b is not None:
        key_b = st.selectbox(
            "¿Cuál es la columna clave de Base B?",
            options=list(df_b.columns),
            index=0 if len(df_b.columns) > 0 else None,
            key="select_key_b",
        )
        st.session_state["join_key_b"] = key_b
    else:
        st.info("Sube Base B para elegir su columna clave.")


# Paso 3: Tipo de cruce
st.header("Paso 3: Elegir tipo de join")
join_options = [
    "inner",
    "left",
    "right",
    "outer",
    "anti A vs B",
    "anti B vs A",
]
selected_join = st.selectbox("Tipo de join", options=join_options, key="join_type_select")
st.session_state["join_type"] = selected_join


# Paso 4: Selección de columnas
st.header("Paso 4: Seleccionar columnas a conservar")
cols_a_default = list(df_a.columns) if df_a is not None else []
cols_b_default = list(df_b.columns) if df_b is not None else []

c1, c2 = st.columns(2)
with c1:
    cols_from_a = st.multiselect(
        "¿Qué columnas quieres conservar de Base A?",
        options=cols_a_default,
        default=cols_a_default,
        key="cols_from_a",
    )
with c2:
    cols_from_b = st.multiselect(
        "¿Qué columnas quieres conservar de Base B?",
        options=cols_b_default,
        default=cols_b_default,
        key="cols_from_b",
    )


# Paso 5: Generar resultado
st.header("Paso 5: Generar resultado")
generate = st.button("Generar resultado")

if generate:
    # Validaciones
    if df_a is None or df_b is None:
        st.error("Debes cargar Base A y Base B.")
    elif not st.session_state.get("join_key_a") or not st.session_state.get("join_key_b"):
        st.error("Debes seleccionar las columnas clave para A y B.")
    else:
        key_a = st.session_state["join_key_a"]
        key_b = st.session_state["join_key_b"]
        how = st.session_state["join_type"]
        suffixes = st.session_state.get("suffixes", ("_A", "_B"))

        try:
            if how in {"inner", "left", "right", "outer"}:
                merged = do_merge(df_a, df_b, key_a, key_b, how=how, suffixes=suffixes)
                filtered = filter_columns(
                    merged,
                    cols_from_a=cols_from_a,
                    cols_from_b=cols_from_b,
                    key_a=key_a,
                    key_b=key_b,
                    suffixes=suffixes,
                )
                st.session_state["resultado"] = filtered
                stats = build_summary_stats(df_a, df_b, key_a, key_b, how, filtered)
                st.session_state["stats"] = stats
            elif how == "anti A vs B":
                result = anti_join(df_a, df_b, key_a, key_b, direction="A_not_in_B")
                # En anti join, columnas seleccionadas sólo del lado correspondiente
                filtered = result.loc[:, [c for c in cols_from_a if c in result.columns]]
                st.session_state["resultado"] = filtered
                stats = build_summary_stats(df_a, df_b, key_a, key_b, "anti_A_vs_B", filtered)
                st.session_state["stats"] = stats
            elif how == "anti B vs A":
                result = anti_join(df_a, df_b, key_a, key_b, direction="B_not_in_A")
                filtered = result.loc[:, [c for c in cols_from_b if c in result.columns]]
                st.session_state["resultado"] = filtered
                stats = build_summary_stats(df_a, df_b, key_a, key_b, "anti_B_vs_A", filtered)
                st.session_state["stats"] = stats
        except Exception as e:
            st.error(f"Error generando el resultado: {e}")


# Paso 6: Resumen y vista previa
if st.session_state.get("resultado") is not None:
    st.header("Paso 6: Resumen y vista previa")
    stats = st.session_state.get("stats", {})

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Filas en Base A", f"{stats.get('rows_a', 0):,}")
    m2.metric("Filas en Base B", f"{stats.get('rows_b', 0):,}")
    m3.metric("Filas resultado", f"{stats.get('rows_result', 0):,}")
    m4.metric("Llaves A ∩ B", f"{stats.get('keys_matched', 0):,}")

    m5, m6 = st.columns(2)
    m5.metric("Llaves únicas en A", f"{stats.get('unique_keys_a', 0):,}")
    m6.metric("Llaves únicas en B", f"{stats.get('unique_keys_b', 0):,}")

    if st.session_state.get("join_type") in {"anti A vs B", "anti B vs A"}:
        st.metric("Registros excluidos", f"{stats.get('excluded_rows', 0):,}")

    st.subheader("Vista previa del resultado")
    df_result = st.session_state["resultado"]
    st.write(f"Total de filas: {len(df_result):,}")
    st.dataframe(df_result.head(50))

    # Paso 7: Descarga
    st.subheader("Descargar resultado")
    try:
        excel_bytes = to_excel_bytes(df_result)
        st.download_button(
            label="Descargar Excel",
            data=excel_bytes,
            file_name="resultado_merge.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.error(f"No se pudo preparar el archivo Excel: {e}")


st.caption(
    "Nota: Los merges manejan colisiones de columnas con sufijos _A y _B para evitar choques de nombres."
)


