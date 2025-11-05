# Guía de Despliegue en Streamlit Cloud

## Tu repositorio ya está en GitHub
- **URL del repositorio:** https://github.com/romebarr/merge_data

## Pasos para desplegar en Streamlit Cloud

### 1. Accede a Streamlit Cloud
1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Inicia sesión con tu cuenta de GitHub

### 2. Conecta tu repositorio
1. Haz clic en "New app"
2. Selecciona tu repositorio: `romebarr/merge_data`
3. Selecciona la rama: `main`
4. Especifica el archivo principal: `app.py`

### 3. Configuración
- **App URL:** (Streamlit generará una automáticamente, ej: `merge-data.streamlit.app`)
- **Main file path:** `app.py`
- **Python version:** 3.8 o superior (Streamlit Cloud lo detecta automáticamente)

### 4. Despliega
1. Haz clic en "Deploy"
2. Streamlit Cloud instalará automáticamente las dependencias desde `requirements.txt`
3. Tu app estará disponible en unos minutos

## Notas importantes

- ✅ **requirements.txt** ya está configurado con todas las dependencias necesarias
- ✅ **.streamlit/config.toml** está configurado para producción
- ✅ El código está listo para producción

## Si necesitas hacer cambios

1. Haz cambios en tu código local
2. Haz commit: `git commit -m "descripción"`
3. Haz push: `git push origin main`
4. Streamlit Cloud detectará los cambios y redesplegará automáticamente

## Verificación

Una vez desplegado, verifica que:
- ✅ La app carga correctamente
- ✅ Puedes subir archivos CSV/Excel
- ✅ La detección automática de columnas funciona
- ✅ Los merges se ejecutan correctamente
- ✅ Las descargas funcionan

## Solución de problemas

Si encuentras errores:
1. Revisa los logs en Streamlit Cloud
2. Verifica que todas las dependencias estén en `requirements.txt`
3. Asegúrate de que `app.py` sea el punto de entrada correcto

