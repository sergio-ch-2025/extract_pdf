# archivo: visor_web_interactivo.py

from flask import Flask, request, render_template_string, send_from_directory, redirect, url_for
import pymysql
import os
from configparser import ConfigParser

app = Flask(__name__)

# Cargar configuración
CONFIG = ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), '../config/config.cf'))

DB_CONFIG = {
    'host': CONFIG.get('database', 'host'),
    'user': CONFIG.get('database', 'user'),
    'password': CONFIG.get('database', 'password'),
    'database': CONFIG.get('database', 'dbname'),
    'cursorclass': pymysql.cursors.DictCursor
}

CARPETA_PROCESADOS = CONFIG.get('paths', 'carpeta_procesados', fallback='../procesados')

ORDEN_CAMPOS = [
    'tipo_doc', 'numero_documento', 'localidad', 'fecha_documento',
    'rut_proveedor','nombre_proveedor', 
    'rut_comprador', 'nombre_comprador','direccion_comprador', 'telefono_comprador', 'comuna_comprador', 'ciudad_comprador',
    'placa_patente', 'tipo_vehiculo', 'marca', 'modelo', 'n_motor', 'n_chasis', 'vin',
    'serie', 'color', 'anio', 'unidad_pbv', 'pbv', 'cit', 'combustible', 'unidad_carga',
    'carga', 'asientos', 'puertas', 'unidad_potencia', 'potencia_motor', 'ejes', 'traccion',
    'tipo_carroceria', 'cilindrada', 'transmision', 'monto_neto', 'monto_iva', 'monto_total','num_contrato'
]

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Visor de Documentos</title>
    <style>
        body {{ font-family: Arial; margin: 0; padding: 0; }}
        .container {{ display: flex; height: 100vh; }}
        .sidebar {{ width: 400px;  /* ancho fijo */
        background: #f0f0f0;
        padding: 20px;
        box-shadow: 2px 0 5px rgba(0,0,0,0.1);
        overflow-y: auto;
        flex-shrink: 0; /* evita que se reduzca en pantallas chicas */ }}
        .content {{ flex-grow: 1;  /* ocupa todo el espacio restante */
        padding: 20px;
        display: flex;
        flex-direction: column;
        min-width: 0;  /* evita overflow horizontal */ }}
        iframe {{ width: 100%; height: 60%; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; }}
        th {{ background-color: #4CAF50; color: white; }}
        .nav-buttons {{ margin-top: 10px; display: flex; justify-content: space-between; }}
        .text-output {{ margin-top: 20px; background: #f9f9f9; padding: 10px; border: 1px solid #ccc; white-space: pre-wrap; font-family: monospace; height: 30%; overflow-y: auto; position: relative; }}
        .copy-button {{ position: absolute; top: 10px; right: 10px; padding: 5px 10px; background-color: #007bff; color: white; border: none; cursor: pointer; }}
        .copy-status {{ position: absolute; top: 10px; left: 10px; font-size: 0.9em; color: green; display: none; }}
        .line-number {{
            user-select: none;
            color: gray;
        }}
    </style>
   <script>
function copiarTexto() {{
    const lineas = document.querySelectorAll("#textoExtraido .line-content");
    const textoSinNumeros = Array.from(lineas).map(el => el.textContent).join('\\n');

    if (navigator.clipboard && navigator.clipboard.writeText) {{
        navigator.clipboard.writeText(textoSinNumeros).then(function() {{
            mostrarStatus();
        }}).catch(function(err) {{
            alert("Error al copiar: " + err);
        }});
    }} else {{
        var textarea = document.createElement("textarea");
        textarea.value = textoSinNumeros;
        document.body.appendChild(textarea);
        textarea.select();
        try {{
            document.execCommand("copy");
            mostrarStatus();
        }} catch (err) {{
            alert("Error al copiar usando fallback: " + err);
        }}
        document.body.removeChild(textarea);
    }}

    function mostrarStatus() {{
        var status = document.getElementById("copyStatus");
        status.style.display = 'inline';
        setTimeout(function() {{ status.style.display = 'none'; }}, 2000);
    }}
}}
</script>
</head>
<body>
<div class="container">
    <div class="sidebar">
        <h3>Buscar Documento</h3>
        <form method="get">
            <label>ID Documento:</label><br>
            <input type="number" name="documento_id" value="{documento_id}"><br><br>
            <label>o Nombre de Archivo:</label><br>
            <input type="text" name="nombre_archivo" value="{nombre_archivo}"><br><br>
            <label>Método:</label><br>
            <select name="metodo">
                <option value="doctr" {selected_doctr}>doctr</option>
                <option value="paddleocr" {selected_paddleocr}>paddleocr</option>
                <option value="easyocr" {selected_easyocr}>easyocr</option>
                <option value="tesseract4" {selected_tesseract4}>tesseract4</option>
                <option value="tesseract6" {selected_tesseract6}>tesseract6</option>
            </select><br><br>
            <button type="submit">Visualizar</button>
        </form>
        <hr>
        <h4>Agregar o Actualizar Campo</h4>
        <form method="post" action="/insertar_campo">
            <input type="hidden" name="documento_id" value="{documento_id}">
            <input type="hidden" name="metodo" value="{metodo}">
            <label for="campo">Campo:</label><br>
            <select id="campo" name="campo" required>
                {campo_options}
            </select><br><br>
            <label for="valor">Valor:</label><br>
            <input type="text" id="valor" name="valor" required><br><br>
            <button type="submit">Guardar</button>
        </form>
        <div class="nav-buttons">
            <form method="get" style="display:inline;">
                <input type="hidden" name="documento_id" value="{prev_id}">
                <input type="hidden" name="metodo" value="{metodo}">
                <button type="submit">&laquo; Anterior</button>
            </form>
            <form method="get" style="display:inline;">
                <input type="hidden" name="documento_id" value="{documento_id}">
                <input type="hidden" name="metodo" value="consolidado">
                <button type="submit">Datos Consolidados</button>
            </form>
            <form method="get" style="display:inline;">
                <input type="hidden" name="documento_id" value="{next_id}">
                <input type="hidden" name="metodo" value="{metodo}">
                <button type="submit">Siguiente &raquo;</button>
            </form>
        </div>
        {error_html}
        {tabla_html}
    </div>
    <div class="content">
        {pdf_html}
        {texto_extraccion}
    </div>
</div>
</body>
</html>
'''

def obtener_texto_extraido(documento_id, metodo):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT texto_extraccion
                FROM extracciones_texto_total
                WHERE documento_id = %s AND metodo = %s
                AND deleted_at IS NULL ORDER BY id DESC LIMIT 1
            """, (documento_id, metodo))
            row = cursor.fetchone()
            return row['texto_extraccion'] if row else ''
    finally:
        connection.close()

def documento_existe(doc_id):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM documentos WHERE id = %s", (doc_id,))
            return cursor.fetchone() is not None
    finally:
        connection.close()

def obtener_documento_id_por_nombre(nombre_archivo):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM documentos WHERE nombre_archivo = %s", (nombre_archivo,))
            row = cursor.fetchone()
            return row['id'] if row else None
    finally:
        connection.close()

def obtener_siguiente_anterior_id(actual_id):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(id) as anterior FROM documentos WHERE id < %s", (actual_id,))
            anterior = cursor.fetchone()['anterior']
            cursor.execute("SELECT MIN(id) as siguiente FROM documentos WHERE id > %s", (actual_id,))
            siguiente = cursor.fetchone()['siguiente']
            return anterior or actual_id, siguiente or actual_id
    finally:
        connection.close()

def obtener_datos(documento_id, metodo):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT nombre_archivo FROM documentos WHERE id = %s", (documento_id,))
            row = cursor.fetchone()
            if not row:
                return None, None, "Documento no encontrado."

            nombre_archivo = row['nombre_archivo']
            ruta_pdf = os.path.join(CARPETA_PROCESADOS, nombre_archivo)
            if not os.path.isfile(ruta_pdf):
                return None, None, f"Archivo PDF no encontrado en {ruta_pdf}"

            if metodo == 'consolidado':
                cursor.execute("""
                    SELECT campo, valor
                    FROM extraccion_campos_consolidada
                    WHERE documento_id = %s AND deleted_at IS NULL 
                """, (documento_id,))
            else:
                cursor.execute("""
                    SELECT campo, valor, score
                    FROM extracciones_campos
                    WHERE documento_id = %s AND metodo = %s AND deleted_at IS NULL 
                """, (documento_id, metodo))
            campos = cursor.fetchall()
            if not isinstance(campos, list):
                campos = list(campos)  # Forzar conversión si es tupla u otro iterable
            campos.sort(key=lambda row: ORDEN_CAMPOS.index(row['campo']) if row['campo'] in ORDEN_CAMPOS else len(ORDEN_CAMPOS))
            return nombre_archivo, campos, None
    finally:
        connection.close()

def enumerate_texto_lineas(texto):
    return '\n'.join(f'<span class="line-number">{i+1:04d}:</span><span class="line-content">{line}</span>' for i, line in enumerate(texto.splitlines()))

@app.route('/', methods=['GET'])
def index():
    documento_id = request.args.get('documento_id', default='', type=str)
    nombre_archivo = request.args.get('nombre_archivo', default='', type=str)
    metodo = request.args.get('metodo', default='doctr', type=str)
    pdf_html = ''
    tabla_html = ''
    error_html = ''
    texto_extraccion_html = ''

    if not documento_id and nombre_archivo:
        doc_id = obtener_documento_id_por_nombre(nombre_archivo)
        if doc_id:
            documento_id = str(doc_id)
        else:
            error_html = f'<p style="color:red;">No se encontró el archivo: {nombre_archivo}</p>'
            documento_id = '0'

    try:
        documento_id_int = int(documento_id)
    except ValueError:
        documento_id_int = 1
        error_html = '<p style="color:red;">El ID debe ser un número entero.</p>'

    if not documento_existe(documento_id_int):
        error_html += f'<p style="color:red;">El documento con ID {documento_id_int} no existe en la base de datos.</p>'
        nombre_pdf = None
        campos = []
    else:
        nombre_pdf, campos, error = obtener_datos(documento_id_int, metodo)
        texto_extraido = obtener_texto_extraido(documento_id_int, metodo)
        if texto_extraido:
            texto_extraccion_html = (
                '<div class="text-output">'
                '<button class="copy-button" onclick="copiarTexto()">Copiar</button>'
                '<span id="copyStatus" class="copy-status">Texto copiado</span>'
                f'<pre id="textoExtraido"><code>{enumerate_texto_lineas(texto_extraido)}</code></pre>'
                '</div>'
            )

        if error:
            error_html = f'<p style="color:red;"><strong>Error:</strong> {error}</p>'
        else:
            pdf_html = f'<iframe src="/pdf/{nombre_pdf}"></iframe>'
            if metodo == 'consolidado':
                filas = ''.join(
                    f"<tr><td>{row['campo']}</td><td>{row['valor']}</td><td>-</td></tr>"
                    for row in campos)
            else:
                filas = ''.join(
                    f"<tr><td>{row['campo']}</td><td>{row['valor']}</td><td>{row['score'] if row['score'] is not None else ''}</td></tr>"
                    for row in campos)
            tabla_html = f'<h4>Resultados {"consolidados" if metodo == "consolidado" else f"para método: {metodo}"}</h4>' \
                         f'<table><tr><th>Campo</th><th>Valor</th><th>Score</th></tr>{filas}</table>'

    selected_doctr = 'selected' if metodo == 'doctr' else ''
    selected_paddleocr = 'selected' if metodo == 'paddleocr' else ''
    selected_easyocr = 'selected' if metodo == 'easyocr' else ''
    selected_tesseract4 = 'selected' if metodo == 'tesseract4' else ''
    selected_tesseract6 = 'selected' if metodo == 'tesseract6' else ''
    prev_id, next_id = obtener_siguiente_anterior_id(documento_id_int)

    # Generar dinámicamente las opciones para el campo select
    campo_options = ''.join(
        f'<option value="{campo}">{campo}</option>' for campo in ORDEN_CAMPOS
    )

    html_render = HTML_TEMPLATE.format(
        documento_id=documento_id_int,
        nombre_archivo=nombre_archivo,
        metodo=metodo,
        pdf_html=pdf_html,
        tabla_html=tabla_html,
        error_html=error_html,
        texto_extraccion=texto_extraccion_html,
        selected_doctr=selected_doctr,
        selected_paddleocr=selected_paddleocr,
        selected_easyocr=selected_easyocr,
        selected_tesseract4=selected_tesseract4,
        selected_tesseract6=selected_tesseract6,
        prev_id=prev_id,
        next_id=next_id,
        campo_options=campo_options
    )
    return render_template_string(html_render)

@app.route('/pdf/<path:filename>')
def serve_pdf(filename):
    return send_from_directory(CARPETA_PROCESADOS, filename)


# Endpoint para insertar o actualizar campo en extracciones_campos
@app.route('/insertar_campo', methods=['POST'])
def insertar_campo():
    documento_id = request.form.get('documento_id', type=int)
    metodo = request.form.get('metodo')
    campo = request.form.get('campo')
    valor = request.form.get('valor')

    if not (documento_id and metodo and campo and valor):
        return redirect(url_for('index', documento_id=documento_id, metodo=metodo))

    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM extracciones_campos 
                WHERE documento_id = %s AND metodo = %s AND campo = %s AND deleted_at IS NULL
                ORDER BY id DESC LIMIT 1
            """, (documento_id, metodo, campo))
            existe = cursor.fetchone()

            if existe:
                cursor.execute("""
                    UPDATE extracciones_campos SET valor = %s 
                    WHERE id = %s
                """, (valor, existe["id"]))

                
            else:
                cursor.execute("""
                    INSERT INTO extracciones_campos (documento_id, metodo, campo, valor, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                """, (documento_id, metodo, campo, valor))
                connection.commit()
                
                # También insertar en extracciones_campos_consolidada

                cursor.execute("""
                    INSERT INTO extraccion_campos_consolidada (documento_id, metodo, campo, valor, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                """, (documento_id, metodo, campo, valor))

        connection.commit()
    finally:
        connection.close()

    return redirect(url_for('index', documento_id=documento_id, metodo=metodo))


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
