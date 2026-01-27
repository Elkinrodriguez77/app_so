# SBD Sell-Out Tool (Python Version)

Aplicación profesional para la carga y gestión de reportes de ventas (Sell-Out) de Stanley Black & Decker, migrada a Python para máximo rendimiento y exactitud.

## Despliegue en Render

1. **GitHub**: Sube este proyecto a un repositorio.
2. **Web Service**: En Render, crea un nuevo Web Service conectado a ese repositorio.
3. **Entorno**: Python.
4. **Build Command**: `pip install -r requirements.txt`
5. **Start Command**: `gunicorn app:app`
6. **Variables de Entorno**:
   - `DATABASE_URL`: Tu URL de conexión a PostgreSQL (Render).

## Características
- **Procesamiento con Pandas**: Carga de archivos Excel/CSV ultra-rápida y exacta.
- **Limpieza Inteligente**: Manejo automático de formatos de moneda latino/anglo.
- **Dashboard**: Consultas agrupadas por Canal, Vendedor, SKU, Cliente y Mes-Año.
- **Gestión de Datos**: Filtros avanzados y eliminación por períodos para evitar duplicados.

## Requisitos Locales
- Python 3.9+
- PostgreSQL
- `pip install -r requirements.txt`
