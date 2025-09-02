#!/usr/bin/env python3
#  python3 registrar_documentos.py --debug
#
#
import argparse
import os
import sys
import logging
import pymysql
import configparser
import fitz  # PyMuPDF
import hashlib
from datetime import datetime
import shutil
from time import time

# ===========================
# Cargar configuraciones
# ===========================
config = configparser.ConfigParser()
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Permitir override por variable de entorno, si se desea.
    DEFAULT_CONFIG_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'config', 'config.cf'))
    CONFIG_PATH = os.environ.get('EXTRACT_PDF_CONFIG', DEFAULT_CONFIG_PATH)

    read_files = config.read(CONFIG_PATH)
    if not read_files:
        raise FileNotFoundError(f"No se pudo leer el archivo de configuración en: {CONFIG_PATH}")

    if not config.has_section('database'):
        raise configparser.NoSectionError('database')
    if not config.has_section('paths'):
        raise configparser.NoSectionError('paths')

    DB_HOST = config.get('database', 'host')
    DB_USER = config.get('database', 'user')
    DB_PASS = config.get('database', 'password')
    DB_NAME = config.get('database', 'dbname')

    DIRECTORIO_PDFS = config.get('paths', 'directorio_local_para_procesar')
    DIRECTORIO_ERRORES = config.get('paths', 'directorio_errores', fallback='../errores')
    CARPETA_ARCHIVOS_PADRES = config.get('paths', 'carpeta_archivos_padres', fallback='../archivos_padres')
    logfile = config.get('logs', 'archivo_log', fallback='../logs/actividad.log')
except Exception as e:
    print(f"Error cargando configuraciones: {e}\nRuta intentada: {locals().get('CONFIG_PATH', 'desconocida')}")
    sys.exit(1)

# Configurar logging
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ===========================
# Funciones auxiliares
# ===========================

def debug_log(message, debug):
    if debug:
        print("[DEBUG]", message)

def calcular_hash(path_pdf):
    sha256_hash = hashlib.sha256()
    try:
        with open(path_pdf, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
    except Exception as e:
        logging.error(f"Error calculando hash: {e}")
        return None
    return sha256_hash.hexdigest()

def obtener_metadata_pdf(path_pdf, doc=None):
    metadata = {}
    try:
        metadata["tamaño_bytes"] = os.path.getsize(path_pdf)
        metadata["hash_archivo"] = calcular_hash(path_pdf)
        if doc is None:
            doc = fitz.open(path_pdf)
            close_doc = True
        else:
            close_doc = False
        metadata["numero_paginas"] = len(doc)

        texto_extraido = doc[0].get_text()

        metadata["tipo_documento"] = "escaneado" if len(texto_extraido.strip()) < 500 else "nativo"

        resoluciones = []
        for img in doc.get_page_images(0):
            xres, yres = img[5], img[6]
            if xres and yres:
                resoluciones.append((xres + yres) / 2)
        metadata["resolucion_ppi"] = sum(resoluciones) / len(resoluciones) if resoluciones else 0

        if metadata["resolucion_ppi"] >= 300:
            metadata["calidad_estimativa"] = 90
        elif metadata["resolucion_ppi"] >= 200:
            metadata["calidad_estimativa"] = 70
        elif metadata["resolucion_ppi"] > 0:
            metadata["calidad_estimativa"] = 50
        else:
            metadata["calidad_estimativa"] = 40

        if close_doc:
            doc.close()
    except Exception as e:
        logging.error(f"Error leyendo metadata de {path_pdf}: {e}")
        metadata = None
    return metadata

def insertar_documento(nombre_archivo, metadata, connection, archivo_padre=None, debug=False):
    documento_id = None
    try:
        with connection.cursor() as cursor:
            # Validar si ya existe un documento con el mismo hash y nombre_archivo
            cursor.execute("""
                SELECT id FROM documentos
                WHERE hash_archivo = %s AND nombre_archivo = %s
            """, (metadata.get("hash_archivo"), nombre_archivo))
            if cursor.fetchone():
                logging.warning(f"Documento duplicado detectado (hash + nombre): {nombre_archivo}")
                return None

            sql_doc = ("""
                INSERT INTO documentos
                (nombre_archivo, archivo_padre, hash_archivo, tamaño_bytes, numero_paginas, tipo_documento, resolucion_ppi, calidad_estimativa, estado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(sql_doc, (
                nombre_archivo,
                archivo_padre or nombre_archivo,
                metadata.get("hash_archivo"),
                metadata.get("tamaño_bytes"),
                metadata.get("numero_paginas"),
                metadata.get("tipo_documento"),
                metadata.get("resolucion_ppi"),
                metadata.get("calidad_estimativa"),
                1
            ))
            documento_id = cursor.lastrowid
            connection.commit()
            logging.info(f"Documento insertado: ID={documento_id} | Archivo={nombre_archivo}")

    except Exception as e:
        logging.error(f"Error insertando documento '{nombre_archivo}' en DB: {e}")
        connection.rollback()
        raise e
    return documento_id

def mover_a_errores(path_pdf, mensaje_error=""):
    os.makedirs(DIRECTORIO_ERRORES, exist_ok=True)
    destino_pdf = os.path.join(DIRECTORIO_ERRORES, os.path.basename(path_pdf))
    try:
        shutil.move(path_pdf, destino_pdf)
        logging.warning(f"Archivo movido a carpeta de errores: {path_pdf}")

        if mensaje_error:
            log_path = os.path.splitext(destino_pdf)[0] + ".log"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Error al procesar: {os.path.basename(path_pdf)}\n")
                f.write(f"{mensaje_error.strip()}\n")

    except Exception as e:
        logging.error(f"Error moviendo archivo con error: {e}")

def procesar_directorio(debug=False):
    if not os.path.exists(DIRECTORIO_PDFS):
        logging.error(f"Directorio no existe: {DIRECTORIO_PDFS}")
        sys.exit(1)

    # Conexión a la base de datos con manejo de errores explícito
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    except pymysql.err.OperationalError as e:
        mensaje = (
            f"Error de conexión a MySQL: {e}.\n"
            f"Verifique credenciales/permisos y que exista la BD.\n"
            f"host={DB_HOST}, user={DB_USER}, db={DB_NAME}"
        )
        logging.error(mensaje)
        print(mensaje)
        sys.exit(2)

    archivos = [f for f in os.listdir(DIRECTORIO_PDFS) if f.lower().endswith('.pdf')]
    if not archivos:
        debug_log("No hay archivos PDF para procesar.", debug)
        logging.info("No se encontraron archivos PDF para registrar.")
        if connection:
            connection.close()
        return

    inicio = datetime.now()
    insertados = 0
    errores = 0

    for archivo in archivos:
        path_pdf = os.path.join(DIRECTORIO_PDFS, archivo)
        debug_log(f"Procesando: {path_pdf}", debug)
        try:
            doc = fitz.open(path_pdf)
            archivo_padre = archivo
            if len(doc) == 1:
                t0 = time()
                metadata = obtener_metadata_pdf(path_pdf, doc)
                t1 = time()
                logging.info(f"[TIEMPO] obtener_metadata_pdf: {t1 - t0:.2f} seg")

                t2 = time()
                insertar_documento(archivo, metadata, connection, archivo_padre, debug)
                t3 = time()
                logging.info(f"[TIEMPO] insertar_documento: {t3 - t2:.2f} seg")

                insertados += 1
            else:
                for i in range(len(doc)):
                    nueva_ruta = os.path.join(DIRECTORIO_PDFS, f"{os.path.splitext(archivo)[0]}_{i+1}.pdf")
                    nueva_doc = fitz.open()
                    nueva_doc.insert_pdf(doc, from_page=i, to_page=i)

                    t_save0 = time()
                    nueva_doc.save(nueva_ruta)
                    t_save1 = time()
                    logging.info(f"[TIEMPO] save_pagina_{i+1}: {t_save1 - t_save0:.2f} seg")
                    nueva_doc.close()

                    t4 = time()
                    metadata_pagina = obtener_metadata_pdf(nueva_ruta)
                    t5 = time()
                    logging.info(f"[TIEMPO] obtener_metadata_pdf_p{str(i+1)}: {t5 - t4:.2f} seg")

                    if metadata_pagina:
                        t6 = time()
                        insertar_documento(os.path.basename(nueva_ruta), metadata_pagina, connection, archivo_padre, debug)
                        t7 = time()
                        logging.info(f"[TIEMPO] insertar_documento_p{str(i+1)}: {t7 - t6:.2f} seg")
                        insertados += 1
                    else:
                        errores += 1
                        mover_a_errores(nueva_ruta, "No se pudo extraer metadatos del archivo generado por página.")
                
                os.makedirs(CARPETA_ARCHIVOS_PADRES, exist_ok=True)
                destino_padre = os.path.join(CARPETA_ARCHIVOS_PADRES, archivo)
                shutil.move(path_pdf, destino_padre)
                logging.info(f"Archivo padre movido a: {destino_padre}")
                doc.close()
                continue
            doc.close()
        except Exception as err:
            errores += 1
            logging.error(f"❌ Error procesando metadatos para: {archivo}")
            mover_a_errores(path_pdf, str(err))

    fin = datetime.now()
    logging.info(
        f"📄 Resultado: Archivos procesados: {len(archivos)} | Insertados OK: {insertados} | Con error: {errores} | Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')} | Fin: {fin.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    if connection:
        connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Registrar documentos PDF en base de datos.")
    parser.add_argument("--debug", action="store_true", help="Habilitar salida debug")
    args = parser.parse_args()

    procesar_directorio(args.debug)
