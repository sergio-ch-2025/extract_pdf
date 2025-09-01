# python3 generar_vista_html.py --documento_id 123
# 
# 
import pymysql
import os
import argparse
from configparser import ConfigParser
from datetime import datetime
import webbrowser

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
DIRECTORIO_TEMPORAL = CONFIG.get('html_test', 'directorio_temporal', fallback='../html_temp')
ABRIR_HTML = CONFIG.getboolean('html_test', 'abrir_html_automatico', fallback=True)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Documento {documento_id}</title>
    <style>
        body {{ margin: 0; font-family: Arial, sans-serif; }}
        .container {{ display: flex; height: 100vh; }}
        .pdf-viewer {{ flex: 2; height: 100%; }}
        .data-viewer {{ flex: 1; padding: 20px; overflow-y: auto; background-color: #f7f7f7; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #4CAF50; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="pdf-viewer">
            <iframe src="{ruta_pdf}" width="100%" height="100%"></iframe>
        </div>
        <div class="data-viewer">
            <h2>Campos Consolidados (ID {documento_id})</h2>
            <table>
                <tr><th>Campo</th><th>Valor</th></tr>
                {filas_tabla}
            </table>
        </div>
    </div>
</body>
</html>
'''

def generar_html(documento_id):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            # Obtener nombre del archivo PDF desde tabla documentos
            cursor.execute("SELECT nombre_archivo FROM documentos WHERE id = %s", (documento_id,))
            row = cursor.fetchone()
            if not row:
                print("[ERROR] Documento no encontrado en tabla documentos.")
                return

            nombre_archivo = row['nombre_archivo']
            ruta_pdf = os.path.join(CARPETA_PROCESADOS, nombre_archivo)
            if not os.path.isfile(ruta_pdf):
                print(f"[ERROR] PDF no encontrado en: {ruta_pdf}")
                return

            cursor.execute("""
                SELECT campo, valor
                FROM extraccion_campos_consolidada
                WHERE documento_id = %s
                ORDER BY campo ASC
            """, (documento_id,))
            filas = cursor.fetchall()

            if not filas:
                print("[ERROR] No hay datos consolidados para ese documento_id.")
                return

            filas_html = "\n".join([f"<tr><td>{row['campo']}</td><td>{row['valor']}</td></tr>" for row in filas])
            html_rendered = HTML_TEMPLATE.format(
                documento_id=documento_id,
                ruta_pdf=ruta_pdf,
                filas_tabla=filas_html
            )

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(DIRECTORIO_TEMPORAL, f"{documento_id}_{timestamp}.html")
            os.makedirs(DIRECTORIO_TEMPORAL, exist_ok=True)

            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_rendered)

            print(f"✅ HTML generado en {output_path}")

            if ABRIR_HTML:
                webbrowser.open(f"file://{os.path.abspath(output_path)}")

    except Exception as e:
        print(f"[ERROR] Fallo al generar HTML: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generar vista HTML temporal de campos consolidados + PDF")
    parser.add_argument("--documento_id", type=int, required=True, help="ID del documento consolidado")
    args = parser.parse_args()
    generar_html(args.documento_id)
