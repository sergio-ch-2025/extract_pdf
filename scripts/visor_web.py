from flask import Flask, send_from_directory
import os
from configparser import ConfigParser

# Cargar configuracion desde config.cf
CONFIG = ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), '../config/config.cf'))
HTML_DIR = os.path.abspath(CONFIG.get('html_test', 'directorio_temporal', fallback='../html_temp'))
PDF_DIR = os.path.abspath(CONFIG.get('paths', 'carpeta_procesados', fallback='../procesados'))

os.makedirs(HTML_DIR, exist_ok=True)

app = Flask(__name__)

@app.route('/')
def index():
    archivos = [f for f in os.listdir(HTML_DIR) if f.endswith('.html')]
    links = "<br>".join(f"<a href='/{f}' target='_blank'>{f}</a>" for f in archivos)
    return f"<h1>Documentos generados</h1>{links}"

@app.route('/<path:nombre>')
def ver_html(nombre):
    return send_from_directory(HTML_DIR, nombre)

@app.route('/procesados/<path:nombre>')
def ver_pdf(nombre):
    return send_from_directory(PDF_DIR, nombre)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
