from __future__ import annotations

import json
import streamlit as st
import pandas as pd

from merge_utils import (
    load_file,
    do_merge,
    anti_join,
    filter_columns,
    build_summary_stats,
    to_excel_bytes,
    to_csv_bytes,
    sanitize_filename,
    detect_key_columns,
    validate_data_before_merge,
    normalize_data,
    detect_duplicates,
    analyze_data_quality,
    aggregate_columns,
)


st.set_page_config(page_title="Data Merge Tool", layout="wide", page_icon="🔗")


def init_state():
    defaults = {
        "df_a": None,
        "df_b": None,
        "join_key_a": None,
        "join_key_b": None,
        "join_keys_a": [],  # Múltiples columnas
        "join_keys_b": [],  # Múltiples columnas
        "use_multiple_keys": False,
        "join_type": "inner",
        "cols_from_a": None,
        "cols_from_b": None,
        "resultado": None,
        "suffixes": ("_A", "_B"),
        "normalize_data": False,
        "normalize_columns_a": [],
        "normalize_columns_b": [],
        "merge_history": [],
        "data_quality_a": None,
        "data_quality_b": None,
        "validation_results": None,
        "should_generate": False,
        "preserve_format": True,  # Por defecto preservar formatos
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_state()


# Sidebar para configuración avanzada
with st.sidebar:
    st.header("⚙️ Configuración")
    
    max_file_size = st.number_input("Tamaño máximo de archivo (MB)", min_value=10, max_value=500, value=100, step=10)
    
    preserve_format_flag = st.checkbox(
        "Preservar formatos originales (leer todo como texto)", 
        value=st.session_state.get("preserve_format", True), 
        key="preserve_format_checkbox",
        help="Si está activado, todos los valores se leerán como texto para preservar el formato original. Si está desactivado, pandas intentará inferir automáticamente los tipos de datos."
    )
    st.session_state["preserve_format"] = preserve_format_flag
    
    normalize_data_flag = st.checkbox("Normalizar datos (quitar espacios)", value=st.session_state.get("normalize_data", False), key="normalize_data_checkbox")
    st.session_state["normalize_data"] = normalize_data_flag
    
    st.divider()
    st.header("💾 Guardar/Cargar")
    
    # Guardar configuración
    if st.button("💾 Guardar configuración"):
        config = {
            "join_key_a": st.session_state.get("join_key_a"),
            "join_key_b": st.session_state.get("join_key_b"),
            "join_keys_a": st.session_state.get("join_keys_a", []),
            "join_keys_b": st.session_state.get("join_keys_b", []),
            "use_multiple_keys": st.session_state.get("use_multiple_keys", False),
            "join_type": st.session_state.get("join_type"),
            "suffixes": st.session_state.get("suffixes"),
        }
        st.download_button(
            label="📥 Descargar configuración",
            data=json.dumps(config, indent=2),
            file_name="merge_config.json",
            mime="application/json",
        )
        st.success("Configuración lista para descargar")
    
    # Cargar configuración
    uploaded_config = st.file_uploader("Cargar configuración", type=["json"], key="config_upload")
    if uploaded_config is not None:
        try:
            config = json.load(uploaded_config)
            for key in ["join_key_a", "join_key_b", "join_keys_a", "join_keys_b", 
                       "use_multiple_keys", "join_type", "suffixes"]:
                if key in config:
                    st.session_state[key] = config[key]
            st.success("Configuración cargada")
            st.rerun()
        except Exception as e:
            st.error(f"Error cargando configuración: {e}")


st.title("🔗 App de Cruce de Bases de Datos")
st.caption("Herramienta avanzada para combinar y analizar bases de datos")

# Tabs para organizar mejor
tab1, tab2, tab3, tab4 = st.tabs(["📊 Cargar Datos", "🔑 Configurar Merge", "📈 Resultado", "📋 Historial"])

with tab1:
    st.header("Paso 1: Subir bases de datos")
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Base A")
        uploaded_a = st.file_uploader(
            "Sube archivo A (.csv o .xlsx)", type=["csv", "xlsx", "xls"], key="upload_a"
        )
        if uploaded_a is not None:
            try:
                with st.spinner("Cargando Base A..."):
                    preserve_format = st.session_state.get("preserve_format", True)
                    df_a = load_file(uploaded_a, max_size_mb=max_file_size, preserve_format=preserve_format)
                    st.session_state["df_a"] = df_a
                    
                    # Analizar calidad de datos
                    st.session_state["data_quality_a"] = analyze_data_quality(df_a)
                    
                preserve_status = "con formatos preservados" if preserve_format else "con tipos inferidos"
                st.success(f"✅ Base A cargada: {len(df_a):,} filas × {len(df_a.columns)} columnas ({preserve_status})")
                
                # Mostrar información de calidad
                quality_a = st.session_state["data_quality_a"]
                with st.expander("📊 Análisis de calidad de Base A"):
                    st.write(f"**Memoria:** {quality_a['memory_usage_mb']:.2f} MB")
                    st.write(f"**Columnas con nulos:**")
                    null_cols = {k: f"{v:.1f}%" for k, v in quality_a['null_percentages'].items() if v > 0}
                    if null_cols:
                        st.json(null_cols)
                    else:
                        st.info("No hay columnas con valores nulos")
                
                st.write("**Vista previa (10 filas):**")
                st.dataframe(df_a.head(10), use_container_width=True)
                
                st.caption(f"Columnas disponibles ({len(df_a.columns)}):")
                st.write(list(df_a.columns))
                
                # Detectar columnas clave automáticamente
                detected_keys = detect_key_columns(df_a)
                if detected_keys:
                    with st.expander("🔍 Columnas clave sugeridas"):
                        st.write("Columnas detectadas como posibles claves (ordenadas por probabilidad):")
                        for i, col in enumerate(detected_keys[:5], 1):
                            st.write(f"{i}. {col}")
                
            except Exception as e:
                st.error(f"❌ Error leyendo Base A: {e}")
                st.session_state["df_a"] = None
    
    with col_b:
        st.subheader("Base B")
        uploaded_b = st.file_uploader(
            "Sube archivo B (.csv o .xlsx)", type=["csv", "xlsx", "xls"], key="upload_b"
        )
        if uploaded_b is not None:
            try:
                with st.spinner("Cargando Base B..."):
                    preserve_format = st.session_state.get("preserve_format", True)
                    df_b = load_file(uploaded_b, max_size_mb=max_file_size, preserve_format=preserve_format)
                    st.session_state["df_b"] = df_b
                    
                    # Analizar calidad de datos
                    st.session_state["data_quality_b"] = analyze_data_quality(df_b)
                    
                preserve_status = "con formatos preservados" if preserve_format else "con tipos inferidos"
                st.success(f"✅ Base B cargada: {len(df_b):,} filas × {len(df_b.columns)} columnas ({preserve_status})")
                
                # Mostrar información de calidad
                quality_b = st.session_state["data_quality_b"]
                with st.expander("📊 Análisis de calidad de Base B"):
                    st.write(f"**Memoria:** {quality_b['memory_usage_mb']:.2f} MB")
                    st.write(f"**Columnas con nulos:**")
                    null_cols = {k: f"{v:.1f}%" for k, v in quality_b['null_percentages'].items() if v > 0}
                    if null_cols:
                        st.json(null_cols)
                    else:
                        st.info("No hay columnas con valores nulos")
                
                st.write("**Vista previa (10 filas):**")
                st.dataframe(df_b.head(10), use_container_width=True)
                
                st.caption(f"Columnas disponibles ({len(df_b.columns)}):")
                st.write(list(df_b.columns))
                
                # Detectar columnas clave automáticamente
                detected_keys = detect_key_columns(df_b)
                if detected_keys:
                    with st.expander("🔍 Columnas clave sugeridas"):
                        st.write("Columnas detectadas como posibles claves (ordenadas por probabilidad):")
                        for i, col in enumerate(detected_keys[:5], 1):
                            st.write(f"{i}. {col}")
                
            except Exception as e:
                st.error(f"❌ Error leyendo Base B: {e}")
                st.session_state["df_b"] = None
    
    # Comparación visual de las bases
    df_a = st.session_state.get("df_a")
    df_b = st.session_state.get("df_b")
    
    if df_a is not None and df_b is not None:
        st.divider()
        st.subheader("🔍 Comparación de Bases")
        comp_col1, comp_col2, comp_col3 = st.columns(3)
        with comp_col1:
            st.metric("Filas Base A", f"{len(df_a):,}")
        with comp_col2:
            st.metric("Filas Base B", f"{len(df_b):,}")
        with comp_col3:
            diff = abs(len(df_a) - len(df_b))
            st.metric("Diferencia", f"{diff:,}")
        
        # Columnas comunes
        common_cols = set(df_a.columns) & set(df_b.columns)
        unique_a = set(df_a.columns) - set(df_b.columns)
        unique_b = set(df_b.columns) - set(df_a.columns)
        
        comp_col4, comp_col5, comp_col6 = st.columns(3)
        with comp_col4:
            st.info(f"Columnas comunes: {len(common_cols)}")
            if len(common_cols) > 0:
                with st.expander("Ver columnas comunes"):
                    st.write(list(common_cols))
        with comp_col5:
            st.warning(f"Solo en A: {len(unique_a)}")
            if len(unique_a) > 0:
                with st.expander("Ver columnas únicas de A"):
                    st.write(list(unique_a))
        with comp_col6:
            st.warning(f"Solo en B: {len(unique_b)}")
            if len(unique_b) > 0:
                with st.expander("Ver columnas únicas de B"):
                    st.write(list(unique_b))


with tab2:
    st.header("Paso 2: Configurar Merge")
    
    df_a = st.session_state.get("df_a")
    df_b = st.session_state.get("df_b")
    
    if df_a is None or df_b is None:
        st.warning("⚠️ Por favor, carga ambas bases de datos en la pestaña 'Cargar Datos'")
    else:
        # Normalización de datos
        if st.session_state.get("normalize_data", False):
            st.info("ℹ️ La normalización de datos está activada. Se eliminarán espacios en blanco.")
            normalize_cols_a = st.multiselect(
                "Columnas a normalizar en Base A (dejar vacío para todas)",
                options=list(df_a.columns),
                default=[],
                key="norm_cols_a"
            )
            normalize_cols_b = st.multiselect(
                "Columnas a normalizar en Base B (dejar vacío para todas)",
                options=list(df_b.columns),
                default=[],
                key="norm_cols_b"
            )
            st.session_state["normalize_columns_a"] = normalize_cols_a
            st.session_state["normalize_columns_b"] = normalize_cols_b
        
        # Selección de tipo de merge: simple o múltiple
        use_multiple = st.checkbox(
            "Usar múltiples columnas como clave compuesta",
            value=st.session_state.get("use_multiple_keys", False),
            key="use_multiple_keys"
        )
        # No necesitamos asignar manualmente, Streamlit lo hace automáticamente con key=
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Columnas clave - Base A")
            # Usar el valor del estado directamente
            if st.session_state.get("use_multiple_keys", False):
                keys_a = st.multiselect(
                    "Selecciona las columnas clave de Base A",
                    options=list(df_a.columns),
                    default=st.session_state.get("join_keys_a", []),
                    key="select_keys_a",
                )
                st.session_state["join_keys_a"] = keys_a
                st.session_state["join_key_a"] = keys_a[0] if keys_a else None
            else:
                detected = detect_key_columns(df_a)
                default_idx = 0
                if detected and detected[0] in df_a.columns:
                    try:
                        default_idx = list(df_a.columns).index(detected[0])
                    except:
                        pass
                
                key_a = st.selectbox(
                    "¿Cuál es la columna clave de Base A?",
                    options=list(df_a.columns),
                    index=default_idx,
                    key="select_key_a",
                    help="Selecciona la columna que identifica únicamente cada fila"
                )
                st.session_state["join_key_a"] = key_a
                st.session_state["join_keys_a"] = [key_a] if key_a else []
                
                # Mostrar información de la columna seleccionada
                if key_a:
                    unique_count = df_a[key_a].nunique()
                    null_count = df_a[key_a].isna().sum()
                    dup_count = df_a[key_a].duplicated().sum()
                    
                    info_col1, info_col2, info_col3 = st.columns(3)
                    with info_col1:
                        st.metric("Valores únicos", f"{unique_count:,}")
                    with info_col2:
                        st.metric("Valores nulos", f"{null_count:,}")
                    with info_col3:
                        st.metric("Duplicados", f"{dup_count:,}")
                    
                    if dup_count > 0:
                        st.warning(f"⚠️ Esta columna tiene {dup_count:,} valores duplicados. Esto puede causar filas duplicadas en el resultado.")
        
        with col2:
            st.subheader("Columnas clave - Base B")
            # Usar el valor del estado directamente
            if st.session_state.get("use_multiple_keys", False):
                keys_b = st.multiselect(
                    "Selecciona las columnas clave de Base B",
                    options=list(df_b.columns),
                    default=st.session_state.get("join_keys_b", []),
                    key="select_keys_b",
                )
                st.session_state["join_keys_b"] = keys_b
                st.session_state["join_key_b"] = keys_b[0] if keys_b else None
            else:
                detected = detect_key_columns(df_b)
                default_idx = 0
                if detected and detected[0] in df_b.columns:
                    try:
                        default_idx = list(df_b.columns).index(detected[0])
                    except:
                        pass
                
                key_b = st.selectbox(
                    "¿Cuál es la columna clave de Base B?",
                    options=list(df_b.columns),
                    index=default_idx,
                    key="select_key_b",
                    help="Selecciona la columna que identifica únicamente cada fila"
                )
                st.session_state["join_key_b"] = key_b
                st.session_state["join_keys_b"] = [key_b] if key_b else []
                
                # Mostrar información de la columna seleccionada
                if key_b:
                    unique_count = df_b[key_b].nunique()
                    null_count = df_b[key_b].isna().sum()
                    dup_count = df_b[key_b].duplicated().sum()
                    
                    info_col1, info_col2, info_col3 = st.columns(3)
                    with info_col1:
                        st.metric("Valores únicos", f"{unique_count:,}")
                    with info_col2:
                        st.metric("Valores nulos", f"{null_count:,}")
                    with info_col3:
                        st.metric("Duplicados", f"{dup_count:,}")
                    
                    if dup_count > 0:
                        st.warning(f"⚠️ Esta columna tiene {dup_count:,} valores duplicados. Esto puede causar filas duplicadas en el resultado.")
        
        # Validación antes de merge
        key_a = st.session_state.get("join_key_a")
        key_b = st.session_state.get("join_key_b")
        keys_a = st.session_state.get("join_keys_a", [])
        keys_b = st.session_state.get("join_keys_b", [])
        
        if (key_a and key_b) or (keys_a and keys_b):
            st.divider()
            st.subheader("🔍 Validación de Datos")
            
            use_multiple_flag = st.session_state.get("use_multiple_keys", False)
            validation_keys_a = keys_a if use_multiple_flag else [key_a]
            validation_keys_b = keys_b if use_multiple_flag else [key_b]
            
            with st.spinner("Validando datos..."):
                validation = validate_data_before_merge(df_a, df_b, validation_keys_a, validation_keys_b)
                st.session_state["validation_results"] = validation
            
            if validation["errors"]:
                st.error("❌ Errores encontrados:")
                for error in validation["errors"]:
                    st.error(f"  • {error}")
            
            if validation["warnings"]:
                st.warning("⚠️ Advertencias:")
                for warning in validation["warnings"]:
                    st.warning(f"  • {warning}")
            
            if not validation["errors"] and not validation["warnings"]:
                st.success("✅ Validación exitosa. Los datos están listos para el merge.")
            
            if validation["info"]:
                with st.expander("ℹ️ Información adicional"):
                    if "overlap" in validation["info"]:
                        st.metric("Coincidencias esperadas", f"{validation['info']['overlap']:,}")
                        st.metric("Llaves únicas en A", f"{validation['info']['unique_a']:,}")
                        st.metric("Llaves únicas en B", f"{validation['info']['unique_b']:,}")
        
        # Tipo de join
        st.divider()
        st.subheader("Paso 3: Elegir tipo de join")
        
        join_options = {
            "inner": "Solo filas que coinciden en ambas bases",
            "left": "Todas las filas de A + coincidencias de B",
            "right": "Todas las filas de B + coincidencias de A",
            "outer": "Todas las filas de ambas bases",
            "anti A vs B": "Filas en A que NO están en B",
            "anti B vs A": "Filas en B que NO están en A",
        }
        
        selected_join = st.selectbox(
            "Tipo de join",
            options=list(join_options.keys()),
            index=list(join_options.keys()).index(st.session_state.get("join_type", "inner")),
            key="join_type_select",
            format_func=lambda x: f"{x} - {join_options[x]}"
        )
        st.session_state["join_type"] = selected_join
        
        # Selección de columnas
        st.divider()
        st.subheader("Paso 4: Seleccionar columnas a conservar")
        
        cols_a_default = list(df_a.columns) if df_a is not None else []
        cols_b_default = list(df_b.columns) if df_b is not None else []
        
        c1, c2 = st.columns(2)
        with c1:
            # Obtener valores guardados y filtrar solo los válidos
            saved_cols_a = st.session_state.get("cols_from_a", [])
            if not isinstance(saved_cols_a, list):
                saved_cols_a = []
            
            # Filtrar para mantener solo columnas que existen en las opciones actuales
            valid_default_a = [col for col in saved_cols_a if col in cols_a_default]
            # Si no hay valores válidos guardados, usar todas las columnas por defecto
            if not valid_default_a and cols_a_default:
                valid_default_a = cols_a_default
            
            cols_from_a = st.multiselect(
                "¿Qué columnas quieres conservar de Base A?",
                options=cols_a_default,
                default=valid_default_a,
                key="cols_from_a_select",
            )
            # Guardar en session_state
            st.session_state["cols_from_a"] = cols_from_a
            
        with c2:
            # Obtener valores guardados y filtrar solo los válidos
            saved_cols_b = st.session_state.get("cols_from_b", [])
            if not isinstance(saved_cols_b, list):
                saved_cols_b = []
            
            # Filtrar para mantener solo columnas que existen en las opciones actuales
            valid_default_b = [col for col in saved_cols_b if col in cols_b_default]
            # Si no hay valores válidos guardados, usar todas las columnas por defecto
            if not valid_default_b and cols_b_default:
                valid_default_b = cols_b_default
            
            cols_from_b = st.multiselect(
                "¿Qué columnas quieres conservar de Base B?",
                options=cols_b_default,
                default=valid_default_b,
                key="cols_from_b_select",
            )
            # Guardar en session_state
            st.session_state["cols_from_b"] = cols_from_b
        
        # Botón de generar
        st.divider()
        col_gen1, col_gen2, col_gen3 = st.columns([1, 2, 1])
        with col_gen2:
            if st.button("🚀 Generar resultado", type="primary", use_container_width=True, key="generate_btn"):
                st.session_state["should_generate"] = True
                st.rerun()


with tab3:
    st.header("Paso 5: Resultado y Descarga")
    
    # Procesar merge cuando se presiona el botón
    if st.session_state.get("should_generate", False):
        st.session_state["should_generate"] = False  # Reset flag
        df_a = st.session_state.get("df_a")
        df_b = st.session_state.get("df_b")
        
        if df_a is None or df_b is None:
            st.error("Debes cargar Base A y Base B.")
        else:
            # Aplicar normalización si está activada
            if st.session_state.get("normalize_data", False):
                norm_cols_a = st.session_state.get("normalize_columns_a", [])
                norm_cols_b = st.session_state.get("normalize_columns_b", [])
                
                with st.spinner("Normalizando datos..."):
                    if norm_cols_a:
                        df_a = normalize_data(df_a, norm_cols_a)
                    else:
                        df_a = normalize_data(df_a)
                    
                    if norm_cols_b:
                        df_b = normalize_data(df_b, norm_cols_b)
                    else:
                        df_b = normalize_data(df_b)
                
                st.session_state["df_a"] = df_a
                st.session_state["df_b"] = df_b
            
            use_multiple_flag = st.session_state.get("use_multiple_keys", False)
            keys_a = st.session_state.get("join_keys_a", [])
            keys_b = st.session_state.get("join_keys_b", [])
            key_a = st.session_state.get("join_key_a")
            key_b = st.session_state.get("join_key_b")
            
            if use_multiple_flag:
                if not keys_a or not keys_b:
                    st.error("Debes seleccionar las columnas clave para A y B.")
                elif len(keys_a) != len(keys_b):
                    st.error("El número de columnas clave debe ser igual en ambas bases.")
                else:
                    merge_keys_a = keys_a
                    merge_keys_b = keys_b
            else:
                if not key_a or not key_b:
                    st.error("Debes seleccionar las columnas clave para A y B.")
                else:
                    merge_keys_a = key_a
                    merge_keys_b = key_b
            
            if (use_multiple_flag and keys_a and keys_b and len(keys_a) == len(keys_b)) or (not use_multiple_flag and key_a and key_b):
                how = st.session_state["join_type"]
                suffixes = st.session_state.get("suffixes", ("_A", "_B"))
                cols_from_a = st.session_state.get("cols_from_a", [])
                cols_from_b = st.session_state.get("cols_from_b", [])
                
                try:
                    with st.spinner("Procesando merge..."):
                        if how in {"inner", "left", "right", "outer"}:
                            merged = do_merge(df_a, df_b, merge_keys_a, merge_keys_b, how=how, suffixes=suffixes)
                            filtered = filter_columns(
                                merged,
                                cols_from_a=cols_from_a,
                                cols_from_b=cols_from_b,
                                key_a=merge_keys_a[0] if isinstance(merge_keys_a, list) else merge_keys_a,
                                key_b=merge_keys_b[0] if isinstance(merge_keys_b, list) else merge_keys_b,
                                suffixes=suffixes,
                            )
                            st.session_state["resultado"] = filtered
                            stats = build_summary_stats(
                                df_a, df_b,
                                merge_keys_a[0] if isinstance(merge_keys_a, list) else merge_keys_a,
                                merge_keys_b[0] if isinstance(merge_keys_b, list) else merge_keys_b,
                                how, filtered
                            )
                            st.session_state["stats"] = stats
                        elif how == "anti A vs B":
                            result = anti_join(df_a, df_b, merge_keys_a, merge_keys_b, direction="A_not_in_B")
                            filtered = result.loc[:, [c for c in cols_from_a if c in result.columns]]
                            st.session_state["resultado"] = filtered
                            stats = build_summary_stats(
                                df_a, df_b,
                                merge_keys_a[0] if isinstance(merge_keys_a, list) else merge_keys_a,
                                merge_keys_b[0] if isinstance(merge_keys_b, list) else merge_keys_b,
                                "anti_A_vs_B", filtered
                            )
                            st.session_state["stats"] = stats
                        elif how == "anti B vs A":
                            result = anti_join(df_a, df_b, merge_keys_a, merge_keys_b, direction="B_not_in_A")
                            filtered = result.loc[:, [c for c in cols_from_b if c in result.columns]]
                            st.session_state["resultado"] = filtered
                            stats = build_summary_stats(
                                df_a, df_b,
                                merge_keys_a[0] if isinstance(merge_keys_a, list) else merge_keys_a,
                                merge_keys_b[0] if isinstance(merge_keys_b, list) else merge_keys_b,
                                "anti_B_vs_A", filtered
                            )
                            st.session_state["stats"] = stats
                    
                    # Guardar en historial
                    history_entry = {
                        "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "join_type": how,
                        "keys_a": merge_keys_a,
                        "keys_b": merge_keys_b,
                        "rows_result": len(st.session_state["resultado"]),
                    }
                    st.session_state["merge_history"].append(history_entry)
                    
                    st.success("✅ Merge completado exitosamente!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error generando el resultado: {e}")
                    import traceback
                    with st.expander("Detalles del error"):
                        st.code(traceback.format_exc())
    
    # Mostrar resultado
    if st.session_state.get("resultado") is not None:
        st.subheader("📊 Resumen y Estadísticas")
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
        
        st.divider()
        st.subheader("🔍 Vista Previa y Búsqueda")
        
        df_result = st.session_state["resultado"]
        
        # Búsqueda y filtrado
        search_col1, search_col2 = st.columns([2, 1])
        with search_col1:
            search_term = st.text_input("🔍 Buscar en los datos", key="search_term")
        with search_col2:
            max_rows_preview = st.number_input("Filas a mostrar", min_value=10, max_value=1000, value=50, step=10)
        
        # Filtrar por término de búsqueda
        df_display = df_result.copy()
        if search_term:
            # Buscar en todas las columnas de tipo string
            mask = pd.Series([False] * len(df_display))
            for col in df_display.columns:
                if df_display[col].dtype == 'object':
                    mask = mask | df_display[col].astype(str).str.contains(search_term, case=False, na=False)
            df_display = df_display[mask]
            st.info(f"Mostrando {len(df_display):,} filas que coinciden con '{search_term}'")
        
        st.write(f"**Total de filas:** {len(df_result):,}")
        if len(df_display) > 0:
            st.dataframe(df_display.head(max_rows_preview), use_container_width=True)
        else:
            st.warning("No hay resultados que coincidan con la búsqueda")
        
        st.divider()
        st.subheader("💾 Descargar Resultado")
        
        download_col1, download_col2 = st.columns(2)
        
        with download_col1:
            try:
                excel_bytes = to_excel_bytes(df_result)
                filename = sanitize_filename("resultado_merge.xlsx")
                st.download_button(
                    label="📥 Descargar Excel",
                    data=excel_bytes,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"No se pudo preparar el archivo Excel: {e}")
        
        with download_col2:
            try:
                csv_bytes = to_csv_bytes(df_result)
                filename = sanitize_filename("resultado_merge.csv")
                st.download_button(
                    label="📥 Descargar CSV",
                    data=csv_bytes,
                    file_name=filename,
                    mime="text/csv",
                    use_container_width=True,
                )
            except Exception as e:
                st.error(f"No se pudo preparar el archivo CSV: {e}")
    else:
        st.info("💡 Ve a la pestaña 'Configurar Merge' y genera un resultado para verlo aquí.")


with tab4:
    st.header("📋 Historial de Operaciones")
    
    history = st.session_state.get("merge_history", [])
    
    if history:
        st.write(f"Total de operaciones: {len(history)}")
        
        for i, entry in enumerate(reversed(history), 1):
            with st.expander(f"Operación #{len(history) - i + 1} - {entry['timestamp']}"):
                st.write(f"**Tipo de join:** {entry['join_type']}")
                st.write(f"**Llaves A:** {entry['keys_a']}")
                st.write(f"**Llaves B:** {entry['keys_b']}")
                st.write(f"**Filas resultado:** {entry['rows_result']:,}")
        
        if st.button("🗑️ Limpiar historial"):
            st.session_state["merge_history"] = []
            st.rerun()
    else:
        st.info("No hay operaciones en el historial aún.")


# Footer
st.divider()
st.caption(
    "💡 **Consejos:** Los merges manejan colisiones de columnas con sufijos _A y _B. "
    "Usa la validación de datos para detectar problemas antes del merge."
)
