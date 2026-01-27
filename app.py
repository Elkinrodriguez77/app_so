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

# --- SEGURIDAD ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

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

def clean_money(value):
    if pd.isna(value) or value == '': return 0.0
    if isinstance(value, (int, float)): return float(value)
    s = str(value).strip().replace('$', '').replace(' ', '')
    # Lógica inteligente de miles/decimales
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

# --- RUTAS ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        u, p = request.form.get('username'), request.form.get('password')
        with engine.connect() as conn:
            user = conn.execute(text("SELECT * FROM usuarios WHERE username = :u"), {"u": u}).mappings().first()
        if user and check_password_hash(user['password_hash'], p):
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
        df = pd.read_csv(filepath, nrows=0) if sheet == 'csv' else pd.read_excel(filepath, sheet_name=sheet, nrows=0)
        return render_template('mapping.html', headers=df.columns.tolist(), filepath=filepath, sheet=sheet, required_columns=REQUIRED_COLUMNS)
    except Exception as e:
        flash(f"Error: {str(e)}")
        return redirect(url_for('index'))

@app.route('/import_data', methods=['POST'])
@login_required
def import_data():
    filepath, sheet = request.form.get('filepath'), request.form.get('sheet')
    mapping = {k: request.form.get(f"mapping[{k}]") for k in REQUIRED_COLUMNS.keys()}
    try:
        df = pd.read_csv(filepath) if sheet == 'csv' else pd.read_excel(filepath, sheet_name=sheet)
        final_m = {v: k for k, v in mapping.items() if v}
        df = df[list(final_m.keys())].rename(columns=final_m)
        for col in ['cantidad_vendida', 'total_venta_costo', 'total_venta']:
            if col in df.columns: df[col] = df[col].apply(clean_money)
        if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.date
        if 'codigo_cliente' in df.columns: df['codigo_cliente'] = pd.to_numeric(df['codigo_cliente'], errors='coerce').fillna(0).astype(int)
        df.to_sql('ventas', engine, if_exists='append', index=False)
        if os.path.exists(filepath): os.remove(filepath)
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
    
    q_params = {'inicio': fi, 'fin': ff}
    where = "fecha BETWEEN :inicio AND :fin"
    if fc:
        where += " AND codigo_cliente = :cod"
        q_params['cod'] = int(fc)
    
    gc = ap if ap != 'mes_anio' else "TO_CHAR(fecha, 'YYYY-MM')"
    sql = f"SELECT {gc} as etiqueta, SUM(cantidad_vendida) as total_cantidad, SUM(total_venta_costo) as total_monto FROM ventas WHERE {where} GROUP BY etiqueta ORDER BY etiqueta ASC"
    
    with engine.connect() as conn:
        resumen = conn.execute(text(sql), q_params).mappings().all()
        recent = conn.execute(text("SELECT * FROM ventas ORDER BY id DESC LIMIT 10")).mappings().all()
    
    return render_template('dashboard.html', resumen=resumen, recent=recent, fecha_inicio=fi, fecha_fin=ff, 
                           agrupar_por=ap, filtro_cliente=fc, 
                           gran_total_cant=sum(r['total_cantidad'] for r in resumen), 
                           gran_total_monto=sum(r['total_monto'] for r in resumen))

@app.route('/delete_records', methods=['POST'])
@login_required
def delete_records():
    i, f, c = request.form.get('del_inicio'), request.form.get('del_fin'), request.form.get('del_cliente')
    sql = "DELETE FROM ventas WHERE fecha BETWEEN :i AND :f"
    p = {'i': i, 'f': f}
    if c:
        sql += " AND codigo_cliente = :c"
        p['c'] = int(c)
    with engine.begin() as conn:
        res = conn.execute(text(sql), p)
    flash(f"Eliminados {res.rowcount} registros.")
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
