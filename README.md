# Data Merge Tool

Herramienta web interactiva para cruzar y combinar bases de datos (CSV o Excel) mediante una interfaz gráfica intuitiva. Desarrollada con Streamlit y pandas.

## ¿Qué hace?

Esta aplicación permite realizar operaciones de merge (cruce) entre dos bases de datos de forma sencilla y visual, sin necesidad de escribir código. Ideal para analistas de datos, investigadores o cualquier persona que necesite combinar información de múltiples fuentes.

### Características principales

- **Subida de archivos**: Soporta archivos CSV y Excel (.xlsx, .xls)
- **Tipos de join**: Inner, Left, Right, Outer y Anti joins
- **Selección de columnas**: Elige qué columnas conservar del resultado
- **Estadísticas detalladas**: Métricas de filas, llaves únicas y coincidencias
- **Vista previa**: Visualiza el resultado antes de descargarlo
- **Exportación**: Descarga el resultado en formato Excel

## Requisitos

- Python 3.8 o superior
- pip (gestor de paquetes de Python)

## 🚀 Despliegue Rápido en Streamlit Cloud

**Tu app ya está lista para desplegarse:**

1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Conecta tu repositorio: `romebarr/merge_data`
3. Selecciona `main` como rama
4. Especifica `app.py` como archivo principal
5. ¡Despliega!

Ver [DEPLOY.md](DEPLOY.md) para instrucciones detalladas.

## Instalación Local

1. Clona el repositorio:
```bash
git clone https://github.com/romebarr/merge_data.git
cd merge_data
```

2. Instala las dependencias:
```bash
pip install -r requirements.txt
```

## Uso

1. Ejecuta la aplicación:
```bash
streamlit run app.py
```

2. La aplicación se abrirá automáticamente en tu navegador (normalmente en `http://localhost:8501`)

3. Sigue los pasos en la interfaz:
   - **Paso 1**: Sube tus dos archivos (Base A y Base B)
   - **Paso 2**: Selecciona las columnas clave para cada base
   - **Paso 3**: Elige el tipo de join (inner, left, right, outer, anti)
   - **Paso 4**: Selecciona qué columnas quieres conservar
   - **Paso 5**: Genera el resultado
   - **Paso 6**: Revisa las estadísticas y vista previa
   - **Paso 7**: Descarga el resultado en Excel

## Tipos de Join

- **Inner**: Solo filas que coinciden en ambas bases
- **Left**: Todas las filas de A + coincidencias de B
- **Right**: Todas las filas de B + coincidencias de A
- **Outer**: Todas las filas de ambas bases
- **Anti A vs B**: Filas que están en A pero no en B
- **Anti B vs A**: Filas que están en B pero no en A

## Estructura del proyecto

```
merge_data/
├── app.py              # Aplicación principal Streamlit
├── merge_utils.py      # Funciones utilitarias para merges
├── requirements.txt    # Dependencias del proyecto
├── README.md          # Este archivo
└── LICENSE            # Licencia MIT
```

## Tecnologías utilizadas

- **Streamlit**: Framework para crear aplicaciones web interactivas en Python
- **pandas**: Biblioteca para manipulación y análisis de datos
- **openpyxl**: Motor para leer/escribir archivos Excel

## Notas importantes

- Los merges manejan automáticamente colisiones de nombres de columnas usando sufijos `_A` y `_B`
- Los archivos se procesan en memoria, ten en cuenta el tamaño de tus datos
- Se recomienda usar archivos de tamaño razonable para mejor rendimiento

## Contribuir

1. Crea un branch: `git checkout -b feature/mi-cambio`
2. Haz commits pequeños y claros
3. Abre un Pull Request

## Licencia

Este proyecto está bajo la licencia MIT. Ver `LICENSE`.
