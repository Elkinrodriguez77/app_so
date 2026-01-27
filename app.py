import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy import create_engine, text, func, extract
from werkzeug.utils import secure_filename
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = "sbd_sellout_secret_key"
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max upload

# Configuración de Base de Datos
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    # Esto es solo para que no falle si olvidas configurar la variable
    raise RuntimeError("ERROR: La variable DATABASE_URL no está configurada.")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)

# Asegurar que la carpeta de uploads exista
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

REQUIRED_COLUMNS = {
    'cantidad_vendida': {
        'label': 'Cantidad Vendida',
        'desc': 'Ventas reportadas en unidades por el distribuidor.',
        'required': False
    },
    'sku_sbd': {
        'label': 'SKU SBD',
        'desc': 'SKU o código interno de producto de Stanley Black & Decker (nuestro código).',
        'required': True
    },
    'total_venta_costo': {
        'label': 'Total Venta Costo',
        'desc': 'Venta reportada por el distribuidor a COSTO.',
        'required': True
    },
    'fecha': {
        'label': 'Fecha',
        'desc': 'Fecha de la venta reportada por el distribuidor.',
        'required': True
    },
    'codigo_cliente': {
        'label': 'Código Cliente',
        'desc': 'Código SAP interno del cliente en Stanley Black & Decker.',
        'required': True
    },
    'vendedor_distribuidor': {
        'label': 'Vendedor Distribuidor',
        'desc': 'Nombre del vendedor del distribuidor asociado a la venta.',
        'required': False
    },
    'total_venta': {
        'label': 'Total Venta',
        'desc': 'Venta sin descontar costo reportada por el distribuidor.',
        'required': False
    },
    'canal_venta': {
        'label': 'Canal de Venta',
        'desc': 'Canal de venta (Comercio, Ecommerce, Tradicional, etc.).',
        'required': False
    },
    'nit_cliente_venta': {
        'label': 'NIT Cliente Venta',
        'desc': 'Código o NIT del cliente del distribuidor.',
        'required': False
    }
}

def clean_money(value):
    if pd.isna(value) or value == '':
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    # Limpieza de strings (formato latino/anglo)
    s = str(value).strip().replace('$', '').replace(' ', '')
    # Si hay puntos y comas, el último suele ser el decimal
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    elif ',' in s:
        # Solo comas: si hay una sola y tiene 3 dígitos después, puede ser miles en SBD
        # Pero Pandas suele manejarlo bien si reemplazamos , por .
        if s.count(',') == 1 and len(s.split(',')[1]) == 3:
            s = s.replace(',', '')
        else:
            s = s.replace(',', '.')
    
    try:
        return float(s)
    except:
        return 0.0

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process_upload', methods=['POST'])
def process_upload():
    if 'excel_file' not in request.files:
        flash('No se seleccionó ningún archivo')
        return redirect(url_for('index'))
    
    file = request.files['excel_file']
    if file.filename == '':
        flash('No se seleccionó ningún archivo')
        return redirect(url_for('index'))

    filename = secure_filename(f"{int(datetime.now().timestamp())}_{file.filename}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    ext = filename.rsplit('.', 1)[1].lower()
    
    if ext == 'csv':
        return redirect(url_for('mapping', filepath=filepath, sheet='csv'))
    else:
        try:
            xl = pd.ExcelFile(filepath)
            sheets = xl.sheet_names
            if len(sheets) == 1:
                return redirect(url_for('mapping', filepath=filepath, sheet=sheets[0]))
            return render_template('select_sheet.html', filepath=filepath, sheets=sheets)
        except Exception as e:
            flash(f"Error al leer Excel: {str(e)}")
            return redirect(url_for('index'))

@app.route('/mapping')
def mapping():
    filepath = request.args.get('filepath')
    sheet = request.args.get('sheet')
    
    try:
        if sheet == 'csv':
            # Intentar detectar delimitador
            df = pd.read_csv(filepath, nrows=0)
        else:
            df = pd.read_excel(filepath, sheet_name=sheet, nrows=0)
        
        headers = df.columns.tolist()
        return render_template('mapping.html', headers=headers, filepath=filepath, sheet=sheet, required_columns=REQUIRED_COLUMNS)
    except Exception as e:
        flash(f"Error al procesar encabezados: {str(e)}")
        return redirect(url_for('index'))

@app.route('/import_data', methods=['POST'])
def import_data():
    filepath = request.form.get('filepath')
    sheet = request.form.get('sheet')
    mapping = {k: request.form.get(f"mapping[{k}]") for k in REQUIRED_COLUMNS.keys()}
    
    try:
        if sheet == 'csv':
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath, sheet_name=sheet)

        # Filtrar solo columnas mapeadas
        final_mapping = {v: k for k, v in mapping.items() if v}
        df = df[list(final_mapping.keys())].rename(columns=final_mapping)

        # Aplicar limpieza de datos
        if 'cantidad_vendida' in df.columns:
            df['cantidad_vendida'] = df['cantidad_vendida'].apply(clean_money)
        if 'total_venta_costo' in df.columns:
            df['total_venta_costo'] = df['total_venta_costo'].apply(clean_money)
        if 'total_venta' in df.columns:
            df['total_venta'] = df['total_venta'].apply(clean_money)
        if 'fecha' in df.columns:
            df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.date
        if 'codigo_cliente' in df.columns:
            df['codigo_cliente'] = pd.to_numeric(df['codigo_cliente'], errors='coerce').fillna(0).astype(int)

        # Insertar en DB usando SQLAlchemy (muy rápido con to_sql)
        df.to_sql('ventas', engine, if_exists='append', index=False)
        
        # Limpiar archivo
        if os.path.exists(filepath):
            os.remove(filepath)

        return jsonify({'status': 'success', 'count': len(df)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/dashboard')
def dashboard():
    fecha_inicio = request.args.get('fecha_inicio', datetime.now().strftime('%Y-%m-01'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    agrupar_por = request.args.get('agrupar_por', 'canal_venta')
    filtro_cliente = request.args.get('filtro_cliente', '')

    query_params = {'inicio': fecha_inicio, 'fin': fecha_fin}
    where_sql = "fecha BETWEEN :inicio AND :fin"
    
    if filtro_cliente:
        where_sql += " AND codigo_cliente = :cod_cliente"
        query_params['cod_cliente'] = int(filtro_cliente)

    group_col = agrupar_por
    if agrupar_por == 'mes_anio':
        group_col = "TO_CHAR(fecha, 'YYYY-MM')"

    sql = f"""
        SELECT {group_col} as etiqueta, 
               SUM(cantidad_vendida) as total_cantidad, 
               SUM(total_venta_costo) as total_monto 
        FROM ventas 
        WHERE {where_sql}
        GROUP BY etiqueta 
        ORDER BY etiqueta ASC
    """
    
    with engine.connect() as conn:
        resumen = conn.execute(text(sql), query_params).mappings().all()
        recent = conn.execute(text("SELECT * FROM ventas ORDER BY id DESC LIMIT 10")).mappings().all()

    gran_total_cant = sum(r['total_cantidad'] for r in resumen)
    gran_total_monto = sum(r['total_monto'] for r in resumen)

    return render_template('dashboard.html', 
                           resumen=resumen, 
                           recent=recent, 
                           fecha_inicio=fecha_inicio, 
                           fecha_fin=fecha_fin, 
                           agrupar_por=agrupar_por,
                           filtro_cliente=filtro_cliente,
                           gran_total_cant=gran_total_cant,
                           gran_total_monto=gran_total_monto)

@app.route('/delete_records', methods=['POST'])
def delete_records():
    inicio = request.form.get('del_inicio')
    fin = request.form.get('del_fin')
    cliente = request.form.get('del_cliente')

    sql = "DELETE FROM ventas WHERE fecha BETWEEN :inicio AND :fin"
    params = {'inicio': inicio, 'fin': fin}
    
    if cliente:
        sql += " AND codigo_cliente = :cod_cliente"
        params['cod_cliente'] = int(cliente)

    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        deleted_count = result.rowcount

    flash(f"Se eliminaron {deleted_count} registros correctamente.")
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
