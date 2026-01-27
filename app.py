import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from sqlalchemy import create_engine, text
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "sbd_secret_key_9988_prod")
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

# --- CONFIGURACIÓN DB ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no configurada")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
engine = create_engine(DATABASE_URL)

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# --- CONSTANTES ---
CANALES_SBD = ["Comercio", "Industría", "E-Commerce", "Mostrador", "Moderno"]

REQUIRED_COLUMNS = {
    'cantidad_vendida': {'label': 'Cantidad Vendida', 'desc': 'Unidades reportadas.', 'required': False},
    'sku_sbd': {'label': 'SKU SBD', 'desc': 'Nuestro código interno.', 'required': True},
    'total_venta_costo': {'label': 'Total Venta Costo', 'desc': 'Venta a costo.', 'required': True},
    'fecha': {'label': 'Fecha', 'desc': 'Fecha de venta.', 'required': True},
    'codigo_cliente': {'label': 'Código Cliente', 'desc': 'Código SAP interno.', 'required': True},
    'vendedor_distribuidor': {'label': 'Vendedor Distribuidor', 'desc': 'Vendedor del socio.', 'required': False},
    'total_venta': {'label': 'Total Venta', 'desc': 'Venta sin costo.', 'required': False},
    'canal_venta': {'label': 'Canal de Venta', 'desc': 'Canal comercial.', 'required': False},
    'nit_cliente_venta': {'label': 'NIT Cliente Venta', 'desc': 'NIT del cliente final.', 'required': False}
}

# --- SEGURIDAD ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def clean_money(value):
    if pd.isna(value) or value == '': return 0.0
    if isinstance(value, (int, float)): return float(value)
    s = str(value).strip().replace('$', '').replace(' ', '')
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'): s = s.replace('.', '').replace(',', '.')
        else: s = s.replace(',', '')
    elif ',' in s:
        if s.count(',') == 1 and len(s.split(',')[1]) == 3: s = s.replace(',', '')
        else: s = s.replace(',', '.')
    elif '.' in s:
        if s.count('.') == 1 and len(s.split('.')[1]) == 3: s = s.replace('.', '')
    try: return float(s)
    except: return 0.0

# Helper para leer CSV con separador automático
def read_csv_smart(path, **kwargs):
    return pd.read_csv(path, sep=None, engine='python', encoding='utf-8-sig', **kwargs)

# --- RUTAS ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        u, p = request.form.get('username'), request.form.get('password')
        with engine.connect() as conn:
            user = conn.execute(text("SELECT * FROM usuarios WHERE username = :u"), {"u": u}).mappings().first()
        if user and check_password_hash(user['password_hash'], p):
            with engine.begin() as conn:
                conn.execute(text("UPDATE usuarios SET ultimo_acceso = :now WHERE id = :id"), 
                             {"now": datetime.now(), "id": user['id']})
            session['user_id'], session['username'] = user['id'], user['username']
            return redirect(url_for('index'))
        flash('Usuario o contraseña incorrectos')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('Sesión cerrada correctamente')
    return redirect(url_for('login'))

@app.route('/perfil', methods=['GET', 'POST'])
@login_required
def perfil():
    if request.method == 'POST':
        nueva_p = request.form.get('password')
        if not nueva_p or len(nueva_p) < 6:
            flash('La contraseña debe tener al menos 6 caracteres')
            return redirect(url_for('perfil'))
        with engine.begin() as conn:
            conn.execute(text("UPDATE usuarios SET password_hash = :h WHERE id = :id"), 
                         {"h": generate_password_hash(nueva_p), "id": session['user_id']})
        flash('Contraseña actualizada correctamente')
        return redirect(url_for('index'))
    return render_template('perfil.html')

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/process_upload', methods=['POST'])
@login_required
def process_upload():
    if 'excel_file' not in request.files: return redirect(url_for('index'))
    file = request.files['excel_file']
    filename = secure_filename(f"{int(datetime.now().timestamp())}_{file.filename}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    ext = filename.rsplit('.', 1)[1].lower()
    if ext == 'csv': return redirect(url_for('mapping', filepath=filepath, sheet='csv'))
    xl = pd.ExcelFile(filepath)
    sheets = xl.sheet_names
    if len(sheets) == 1: return redirect(url_for('mapping', filepath=filepath, sheet=sheets[0]))
    return render_template('select_sheet.html', filepath=filepath, sheets=sheets)

@app.route('/mapping')
@login_required
def mapping():
    filepath, sheet = request.args.get('filepath'), request.args.get('sheet')
    try:
        df = read_csv_smart(filepath, nrows=0) if sheet == 'csv' else pd.read_excel(filepath, sheet_name=sheet, nrows=0)
        return render_template('mapping.html', headers=df.columns.tolist(), filepath=filepath, sheet=sheet, required_columns=REQUIRED_COLUMNS)
    except Exception as e:
        flash(f"Error al leer archivo: {str(e)}")
        return redirect(url_for('index'))

@app.route('/step_homologar', methods=['POST'])
@login_required
def step_homologar():
    filepath = request.form.get('filepath')
    sheet = request.form.get('sheet')
    mapping = {k: request.form.get(f"mapping[{k}]") for k in REQUIRED_COLUMNS.keys()}
    session['temp_upload'] = {'filepath': filepath, 'sheet': sheet, 'mapping': mapping}
    
    canal_col = mapping.get('canal_venta')
    if not canal_col:
        return redirect(url_for('step_validar_skus'))

    try:
        df = read_csv_smart(filepath, usecols=[canal_col]) if sheet == 'csv' else pd.read_excel(filepath, sheet_name=sheet, usecols=[canal_col])
        valores_unicos = df[canal_col].dropna().unique().tolist()
        return render_template('homologar.html', valores_unicos=valores_unicos, canales_sbd=CANALES_SBD)
    except Exception as e:
        flash(f"Error al leer canales: {str(e)}")
        return redirect(url_for('index'))

@app.route('/step_validar_skus', methods=['POST', 'GET'])
@login_required
def step_validar_skus():
    if request.method == 'POST':
        session['temp_upload']['homologacion'] = request.form.to_dict(flat=True)
    
    data = session.get('temp_upload')
    sku_col = data['mapping']['sku_sbd']
    filepath, sheet = data['filepath'], data['sheet']

    try:
        df = read_csv_smart(filepath, usecols=[sku_col]) if sheet == 'csv' else pd.read_excel(filepath, sheet_name=sheet, usecols=[sku_col])
        skus_archivo = set(df[sku_col].astype(str).unique())

        with engine.connect() as conn:
            res = conn.execute(text('SELECT DISTINCT "Product" FROM sap_byd_ventas')).fetchall()
            skus_sap = set(str(r[0]) for r in res)

        skus_invalidos = sorted(list(skus_archivo - skus_sap))
        if not skus_invalidos:
            return render_template('confirmar_final.html')

        return render_template('corregir_skus.html', skus_invalidos=skus_invalidos)
    except Exception as e:
        flash(f"Error al validar SKUs: {str(e)}")
        return redirect(url_for('index'))

@app.route('/final_import', methods=['POST'])
@login_required
def final_import():
    data = session.get('temp_upload')
    sku_corrections = request.form.to_dict(flat=True)
    
    try:
        df = read_csv_smart(data['filepath']) if data['sheet'] == 'csv' else pd.read_excel(data['filepath'], sheet_name=data['sheet'])
        final_m = {v: k for k, v in data['mapping'].items() if v}
        df = df[list(final_m.keys())].rename(columns=final_m)

        if 'canal_venta' in df.columns and 'homologacion' in data:
            df['canal_venta'] = df['canal_venta'].map(data['homologacion'])

        df['sku_sbd'] = df['sku_sbd'].astype(str).replace(sku_corrections)
        df['usuario_carga'] = session['username']

        for col in ['cantidad_vendida', 'total_venta_costo', 'total_venta']:
            if col in df.columns: df[col] = df[col].apply(clean_money)
        if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.date
        if 'codigo_cliente' in df.columns: df['codigo_cliente'] = pd.to_numeric(df['codigo_cliente'], errors='coerce').fillna(0).astype(int)

        df.to_sql('ventas', engine, if_exists='append', index=False)
        if os.path.exists(data['filepath']): os.remove(data['filepath'])
        session.pop('temp_upload', None)
        return jsonify({'status': 'success', 'count': len(df)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/dashboard')
@login_required
def dashboard():
    fi = request.args.get('fecha_inicio', datetime.now().strftime('%Y-%m-01'))
    ff = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    ap = request.args.get('agrupar_por', 'canal_venta')
    fc = request.args.get('filtro_cliente', '')
    with engine.connect() as conn:
        u_info = conn.execute(text("SELECT ultimo_acceso FROM usuarios WHERE id = :id"), {"id": session['user_id']}).mappings().first()
        ultimo_acceso = u_info['ultimo_acceso'].strftime('%d/%m/%Y %H:%M') if u_info and u_info['ultimo_acceso'] else "Primera vez"
    q_params = {'inicio': fi, 'fin': ff}
    where = "fecha BETWEEN :inicio AND :fin"
    if fc: where += " AND codigo_cliente = :cod"; q_params['cod'] = int(fc)
    gc = ap if ap != 'mes_anio' else "TO_CHAR(fecha, 'YYYY-MM')"
    sql = f"SELECT {gc} as etiqueta, SUM(cantidad_vendida) as total_cantidad, SUM(total_venta_costo) as total_monto FROM ventas WHERE {where} GROUP BY etiqueta ORDER BY etiqueta ASC"
    with engine.connect() as conn:
        resumen = conn.execute(text(sql), q_params).mappings().all()
        recent = conn.execute(text("SELECT * FROM ventas ORDER BY id DESC LIMIT 10")).mappings().all()
    return render_template('dashboard.html', resumen=resumen, recent=recent, fecha_inicio=fi, fecha_fin=ff, ap=ap, fc=fc, ultimo_acceso=ultimo_acceso, gran_total_cant=sum(r['total_cantidad'] for r in resumen), gran_total_monto=sum(r['total_monto'] for r in resumen))

@app.route('/delete_records', methods=['POST'])
@login_required
def delete_records():
    i, f, c = request.form.get('del_inicio'), request.form.get('del_fin'), request.form.get('del_cliente')
    detalles = f"Período: {i} a {f} | Cliente: {c if c else 'TODOS'}"
    sql = "DELETE FROM ventas WHERE fecha BETWEEN :i AND :f"
    p = {'i': i, 'f': f}
    if c: sql += " AND codigo_cliente = :c"; p['c'] = int(c)
    with engine.begin() as conn:
        res = conn.execute(text(sql), p)
        conn.execute(text("INSERT INTO auditoria_operaciones (username, accion, detalles) VALUES (:u, :a, :d)"), {"u": session['username'], "a": "ELIMINACIÓN", "d": detalles})
    flash(f"Eliminados {res.rowcount} registros. Operación auditada.")
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
