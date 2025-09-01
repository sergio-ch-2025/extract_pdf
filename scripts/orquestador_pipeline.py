#!/usr/bin/env python3
import subprocess
import sys
import os
from datetime import datetime
import logging
import configparser
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

SCRIPTS_BASE = [
    "1get_pdf_of_remote.py --allpdf",  # Se omite en modo local
    "2registrar_documentos.py",
    "3extract_text.py",
    "4texts_parse_campos.py",
    "5evaluador_score.py",
    "6consolidar_por_score.py",
    "7put_pdf_to_remote_ok.py --allpdf --modo {modo}"
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR)

LOG_FILE = os.path.join(BASE_DIR, "logs", "pipeline.log")
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

import shlex
import threading

def ejecutar_script(script_comando):
    partes = shlex.split(script_comando)
    script_path = os.path.join(SCRIPTS_DIR, partes[0])
    comando = ["python3", script_path] + partes[1:]
    logging.info(f"🟡 Ejecutando: {' '.join(comando)}")
    try:
        resultado = subprocess.run(comando, capture_output=True, text=True)
        logging.info(resultado.stdout)
        if "No se encontraron PDFs en el servidor remoto." in resultado.stdout:
            logging.info("📭 No hay archivos PDF para procesar. Deteniendo pipeline.")
            return "sin_archivos"
        if resultado.stderr:
            logging.error(f"🔴 STDERR Capturado en {script_comando}:\n{resultado.stderr}")
            logging.error(f"❌ STDOUT en {script_comando}:\n{resultado.stdout}")
        if resultado.returncode != 0:
            logging.error(f"🔴 {script_comando} terminó con código de salida {resultado.returncode}")
            return False
        logging.info("✅ Subproceso ejecutado correctamente.")
        return True
    except Exception as e:
        logging.error(f"❌ Excepción al ejecutar {script_comando}: {e}")
        return False

class PDFWatcherHandler(FileSystemEventHandler):
    def __init__(self, callback, delay=5):
        super().__init__()
        self.callback = callback
        self.delay = delay
        self.timer = None
        self.lock = threading.Lock()
        self.pdf_count = 0

    def on_created(self, event):
        if event.is_directory or not event.src_path.lower().endswith(".pdf"):
            return
        logging.info(f"📥 Archivo PDF detectado: {event.src_path}")
        self.incrementar_y_reiniciar()

    def incrementar_y_reiniciar(self):
        with self.lock:
            self.pdf_count += 1
            if self.timer and self.timer.is_alive():
                self.timer.cancel()
            self.timer = threading.Timer(self.delay, self.ejecutar_callback)
            self.timer.start()

    def ejecutar_callback(self):
        logging.info(f"📊 Total de archivos PDF detectados en ventana de espera: {self.pdf_count}")
        self.pdf_count = 0
        self.callback()

def main():
    config = configparser.ConfigParser()
    config.read(os.path.join(BASE_DIR, '../config/config.cf'))
    directorio_local_entrada = config.get('paths', 'directorio_local_entrada', fallback=None)
    directorio_local_para_procesar = config.get('paths', 'directorio_local_para_procesar', fallback=None)
    delay_pipeline = config.getint('pipeline', 'delay_pipeline', fallback=5)
    max_archivos_por_tanda = config.getint('pipeline', 'max_archivos_por_tanda', fallback=30)

    if not directorio_local_para_procesar or not os.path.exists(directorio_local_para_procesar):
        logging.error(f"❌ Directorio inválido o no configurado: {directorio_local_para_procesar}")
        sys.exit(1)

    if not directorio_local_entrada or not os.path.exists(directorio_local_entrada):
        logging.error(f"❌ Directorio de entrada inválido o no configurado: {directorio_local_entrada}")
        sys.exit(1)

    modo_remoto = "--remote" in sys.argv
    modo = "remoto" if modo_remoto else "local"
    SCRIPTS = [script.format(modo=modo) for script in SCRIPTS_BASE]

    def ejecutar_pipeline(scripts_a_ejecutar):
        inicio = datetime.now()
        logging.info(f"🚀 Iniciando pipeline: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"📄 Ruta del log: {LOG_FILE}")
        logging.info(f"👀 Para seguir el progreso en vivo: tail -f {LOG_FILE}")

        tiempos_por_script = []

        for script in scripts_a_ejecutar:
            tiempo_inicio = datetime.now()
            resultado = ejecutar_script(script)
            if resultado == "sin_archivos" and script.startswith("1get_pdf_of_remote.py"):
                logging.info("🛑 Pipeline detenido: no hay PDFs nuevos para procesar.")
                return
            if resultado is False:
                logging.error(f"❌ Pipeline detenido por error en: {script}")
                return
            duracion = datetime.now() - tiempo_inicio
            tiempos_por_script.append((script, duracion))
            logging.info(f"✅ Finalizado {script} en {duracion}")

        logging.info("\n⏱️ Tiempo por script:")
        for script_name, duracion_script in tiempos_por_script:
            logging.info(f"   • {script_name}: {duracion_script}")

        fin = datetime.now()
        logging.info(f"\n✅ Pipeline completado exitosamente: {fin.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"⏱️ Duración total: {fin - inicio}")
        logging.info(f"\n🟢🟢🟢 DURACIÓN TOTAL DEL PIPELINE: {fin - inicio} 🟢🟢🟢")

    if modo_remoto:
        ejecutar_pipeline(SCRIPTS)
    else:
        scripts_local = SCRIPTS[1:]  # omitir el script 1get_pdf_of_remote.py
        

        def pipeline_watchdog():
            while True:
                if not directorio_local_entrada or not os.path.exists(directorio_local_entrada):
                    logging.warning(f"⚠️ Directorio de entrada no existe: {directorio_local_entrada}")
                    break

                archivos_pdf = [
                    f for f in os.listdir(directorio_local_entrada)
                    if (f.endswith('.pdf') or f.endswith('.PDF')) and not f.startswith('.') and not f.startswith('~') and not f.startswith('._') and not f.startswith('~$')
                ]

                if not archivos_pdf:
                    logging.info("📭 No se encontraron archivos PDF para mover al directorio de procesamiento.")
                    break

                archivos_a_mover = archivos_pdf[:max_archivos_por_tanda]
                moved = 0

                for archivo in archivos_a_mover:
                    origen = os.path.join(directorio_local_entrada, archivo)
                    destino = os.path.join(directorio_local_para_procesar, archivo)
                    try:
                        os.rename(origen, destino)
                        logging.info(f"📁 Movido: {archivo}")
                        moved += 1
                    except Exception as e:
                        logging.error(f"❌ Error moviendo {archivo}: {e}")

                if moved > 0:
                    logging.info(f"✅ Total archivos movidos en esta tanda: {moved}")
                    ejecutar_pipeline(scripts_local)
                else:
                    break

        # Inicia Watchdog en directorio_local_entrada, no en el de procesamiento
        event_handler = PDFWatcherHandler(pipeline_watchdog, delay=delay_pipeline)
        observer = Observer()
        observer.schedule(event_handler, path=directorio_local_entrada, recursive=False)
        observer.start()
        logging.info(f"👂 Observando directorio de entrada: {directorio_local_entrada}...")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

if __name__ == "__main__":
    main()