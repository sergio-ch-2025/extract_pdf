#!/usr/bin/env python3
import os
import sys
import logging
import fitz  # PyMuPDF
import pytesseract
import easyocr
import pymysql
import configparser
import io
import cv2
import shutil
import numpy as np
from paddleocr import PaddleOCR
from doctr.models import ocr_predictor
from PIL import Image, ImageOps, ImageEnhance
from pathlib import Path
from pdf2image import convert_from_path



# ===========================
#  gar configuraciones
# ===========================
config = configparser.ConfigParser()
try:
    config.read('../config/config.cf')
    USE_DOCTR = config.getboolean('ocr', 'use_doctr', fallback=True)
    USE_PADDLEOCR = config.getboolean('ocr', 'use_paddleocr', fallback=True)
    USE_TESSERACT4 = config.getboolean('ocr', 'use_tesseract4', fallback=True)
    USE_TESSERACT6 = config.getboolean('ocr', 'use_tesseract6', fallback=True)
    USE_EASYOCR = config.getboolean('ocr', 'use_easyocr', fallback=True)
    DB_HOST = config.get('database', 'host')
    DB_USER = config.get('database', 'user')
    DB_PASS = config.get('database', 'password')
    DB_NAME = config.get('database', 'dbname')
    DIRECTORIO_PDFS = config.get('paths', 'directorio_local_para_procesar')
    DIRECTORIO_ERRORES = config.get('paths', 'directorio_errores', fallback='../errores')
    CARPETA_PROCESADOS = config.get('paths', 'carpeta_procesados')
    TEMP_DIR = config.get('paths', 'directorio_temporal', fallback='./tmp')
    logfile = config.get('logs', 'archivo_log', fallback='../logs/actividad.log')
except Exception as e:
    print(f"Error cargando configuraciones: {e}")
    sys.exit(1)

# Asegurar que las carpetas necesarias existen
for path in [logfile, TEMP_DIR]:
    directory = os.path.dirname(os.path.abspath(path))
    if not os.path.exists(directory):
        os.makedirs(directory)

# Configurar logging
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("=== Inicio de ejecución de extract_text.py ===")
for handler in logging.getLogger().handlers:
    handler.flush()

# ===========================
# Preprocesamiento de imagen
# ===========================

def preprocesar_imagen(pix):
    img_data = pix.tobytes('png')
    img = Image.open(io.BytesIO(img_data))
    img = ImageOps.grayscale(img)
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.0)
    img_np = np.array(img)
    _, img_bin = cv2.threshold(img_np, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return Image.fromarray(img_bin)


'''def preprocesar_imagen(pix):
    """
    Función avanzada de preprocesamiento de imágenes para OCR,
    basada en heurísticas visuales en lugar de OCR real.
    
    Procesos:
    - Escala de grises
    - Mejora de contraste
    - Suavizado para eliminar ruido
    - Binarización automática (Otsu)
    - Corrección de inclinación (deskew)
    - Evaluación de inversión basada en densidad de píxeles
    """
    # 1. Convertir pix a imagen PIL
    img_data = pix.tobytes('png')
    img = Image.open(io.BytesIO(img_data))

    # 2. Escala de grises
    img = ImageOps.grayscale(img)

    # 3. Mejorar contraste
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.0)

    # 4. Convertir a array numpy
    img_np = np.array(img)

    # 5. Suavizado Gaussiano
    img_np = cv2.GaussianBlur(img_np, (5, 5), 0)

    # 6. Binarización Otsu
    _, img_bin = cv2.threshold(img_np, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # 7. Corrección de inclinación (deskew)
    coords = np.column_stack(np.where(img_bin < 255))
    if coords.shape[0] > 0:
        rect = cv2.minAreaRect(coords)
        angle = rect[-1]
        if angle < -45:
            angle = -(90 + angle)
        else:
            angle = -angle

        if abs(angle) > 1:
            (h, w) = img_bin.shape
            M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
            img_bin = cv2.warpAffine(img_bin, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)

    # 8. Crear imagen PIL
    img_final = Image.fromarray(img_bin)

    # 9. Evaluar inversión por cantidad de "contenido" (píxeles negros)
    img_invertida = ImageOps.invert(img_final)

    np_normal = np.array(img_final)
    np_invertida = np.array(img_invertida)

    contenido_normal = np.count_nonzero(np_normal < 128)  # píxeles "oscuros" (texto probable)
    contenido_invertido = np.count_nonzero(np_invertida < 128)

    # 10. Seleccionar versión con más contenido útil
    if contenido_invertido > contenido_normal:
        return img_invertida
    else:
        return img_final    
'''

# ===========================
# Funciones de extraccion OCR
# ===========================
def extraer_texto_paddleocr_v2(path_pdf, debug=False):
    dpi = 300
    try:
        ocr = PaddleOCR(use_angle_cls=True, lang='es', use_gpu=False)
        imagenes = convert_from_path(path_pdf, dpi=dpi)
        texto = ""

        for imagen in imagenes:
            imagen_np = np.array(imagen)

            # OCR sobre la imagen completa
            resultados = ocr.ocr(imagen_np, cls=True)
            for bloque in resultados:
                for linea in bloque:
                    texto += linea[1][0] + "\n"


            # OCR sobre el bloque derecho inferior
            bloque_derecho = dividir_y_extraer_inferior_derecha(imagen, debug=debug)
            if bloque_derecho is None:
                logging.warning("Segmento derecho inferior no generado correctamente.")
                continue  # pasa a la siguiente página
            else:
                bloque_np = np.array(bloque_derecho)
                resultado_bloque = ocr.ocr(bloque_np, cls=True)

                texto += "\n[SEGMENTO_TOTALES]\n"

                if resultado_bloque:
                    for bloque in resultado_bloque:
                        for linea in bloque:
                            texto += linea[1][0] + "\n"
                else:
                    texto += "[Sin texto detectado en segmento totales]\n"

        logging.info("PaddleOCR v2 extracción exitosa.")
        return texto.strip()

    except Exception as e:
        logging.error(f"Error en PaddleOCR v2: {e}")
        return ""

def dividir_y_extraer_inferior_derecha(img: Image.Image, debug=False):
    if img is None or not hasattr(img, "size"):
        logging.error("Imagen inválida al intentar dividir.")
        return None

    ancho, alto = img.size

    if ancho < 10 or alto < 10:
        logging.error("Tamaño de imagen muy pequeño para segmentar.")
        return None

    # Calcular mitades
    mitad_ancho = ancho // 2
    mitad_alto = alto // 2

    # Extraer el bloque inferior derecho
    bloque_inferior_derecho = img.crop((mitad_ancho, mitad_alto, ancho, alto))

    if debug:
        bloque_inferior_derecho.save("bloque_inferior_derecho.png")
        logging.info("Bloque inferior derecho guardado como imagen de depuración.")

    return bloque_inferior_derecho

def extraer_texto_paddleocr_v2_(path_pdf):
    dpi = 300
    try:
        ocr = PaddleOCR(use_angle_cls=True, lang='es', use_gpu=False)
        imagenes = convert_from_path(path_pdf, dpi=dpi)
        texto = ""

        for imagen in imagenes:
            imagen_np = np.array(imagen)
            
            # OCR principal sobre imagen completa
            resultados = ocr.ocr(imagen_np, cls=True)
            if resultados:
                for bloque in resultados:
                    for linea in bloque:
                        texto += linea[1][0] + "\n"

            
        logging.info("PaddleOCR extracción exitosa.")
        return texto.strip()

    except Exception as e:
        logging.error(f"Error en PaddleOCR v2: {e}")
        return ""

def extraer_texto_paddleocr(path_pdf):
    try:
        ocr = PaddleOCR(use_angle_cls=True, lang='es', use_gpu=False)
        doc = fitz.open(path_pdf)
        texto = ""
        for page in doc:
            pix = page.get_pixmap()
            img = preprocesar_imagen(pix)
            temp_path = os.path.join(TEMP_DIR, "temp_paddle.png")
            img.save(temp_path)
            result = ocr.ocr(temp_path, cls=True)
            texto += " ".join([line[1][0] for block in result for line in block]) + "\n"

            imagen_totales = dividir_y_extraer_inferior_derecha(temp_path, debug=True)
            os.remove(temp_path)
        doc.close()
        logging.info("PaddleOCR extracción exitosa.")
    
        texto += " Segmento_totales: \n"
        result_seg_total = ocr.ocr(imagen_totales, cls=True)
        texto += " ".join([line[1][0] for block in result for line in block]) + "\n"
        
        return texto
    except Exception as e:
        logging.error(f"Error en PaddleOCR: {e}")
        return ""


def extraer_texto_easyocr(path_pdf, use_gpu=False, debug=False):
    try:
        reader = easyocr.Reader(['es'], gpu=use_gpu)
        doc = fitz.open(path_pdf)
        texto = ""
        
        for i, page in enumerate(doc):
            # Convertir la página a imagen con alta resolución
            pix = page.get_pixmap(dpi=300)
            img = preprocesar_imagen(pix)  # Usa tu función de preprocesamiento personalizada

            # Guardar imagen temporal para OCR
            temp_path = os.path.join(TEMP_DIR, f"temp_easyocr_{i}.png")
            img.save(temp_path)

            # Realizar OCR
            results = reader.readtext(temp_path, detail=0)
            texto_pagina = " ".join(results)
            texto += texto_pagina + "\n"

            if debug:
                print(f"[DEBUG] Página {i+1}: {texto_pagina}")

            os.remove(temp_path)

        doc.close()
        logging.info("EasyOCR extracción exitosa.")
        return texto.strip()
    
    except Exception as e:
        logging.error(f"Error en EasyOCR: {e}")
        return ""

def extraer_texto_tesseract_old(path_pdf):
    try:
        doc = fitz.open(path_pdf)
        texto = ""
        for page in doc:
            pix = page.get_pixmap(dpi=300) #pix = page.get_pixmap()
            img = preprocesar_imagen(pix)
            texto += pytesseract.image_to_string(img, lang='spa')
        doc.close()
        logging.info("Tesseract extracción exitosa.")
        return texto
    except Exception as e:
        logging.error(f"Error en Tesseract: {e}")
        return ""

def extraer_texto_tesseract_psm6(path_pdf, debug=False):
    try:
        doc = fitz.open(path_pdf)
        texto = ""
        whitelist = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:,;/()$%&+°ÑñáéíóúÁÉÍÓÚ"
        custom_config = f'--psm 6 -l spa -c tessedit_char_whitelist="{whitelist}"'

        for num, page in enumerate(doc, 1):
            pix = page.get_pixmap(dpi=300)
            img = preprocesar_imagen(pix)
            texto_extraido = pytesseract.image_to_string(img, config=custom_config)
            if debug:
                print(f"Texto extraído página {num}:\n{texto_extraido}\n{'-'*40}")
            texto += texto_extraido + "\n"
        doc.close()
        logging.info("Tesseract extracción exitosa.")
        return texto.strip()
    except Exception as e:
        logging.error(f"Error en Tesseract: {e}")
        return ""
        
def extraer_texto_tesseract_psm4(path_pdf, debug=False):
    try:
        doc = fitz.open(path_pdf)
        texto = ""
        whitelist = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:,;/()$%&+°ÑñáéíóúÁÉÍÓÚ"
        custom_config = f'--psm 4 -l spa -c tessedit_char_whitelist="{whitelist}"'

        for num, page in enumerate(doc, 1):
            pix = page.get_pixmap(dpi=300)
            img = preprocesar_imagen(pix)
            texto_extraido = pytesseract.image_to_string(img, config=custom_config)
            if debug:
                print(f"Texto extraído página {num}:\n{texto_extraido}\n{'-'*40}")
            texto += texto_extraido + "\n"
        doc.close()
        logging.info("Tesseract extracción exitosa.")
        return texto.strip()
    except Exception as e:
        logging.error(f"Error en Tesseract: {e}")
        return ""

def extraer_texto_doctr(path_pdf):
    try:
        model = ocr_predictor(pretrained=True)
        doc = fitz.open(path_pdf)
        texto = ""
        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img = Image.open(io.BytesIO(pix.tobytes('png'))).convert('RGB')
            result = model([np.array(img)])
            for block in result.pages[0].blocks:
                for line in block.lines:
                    texto += " ".join([word.value for word in line.words]) + "\n"

            # OCR sobre el bloque inferior derecho
            bloque_inferior_derecho = dividir_y_extraer_inferior_derecha(img, debug=False)
            if bloque_inferior_derecho is None:
                logging.warning(f"Página : No se pudo extraer el bloque inferior derecho.")
                continue

            texto += "\n[SEGMENTO_TOTALES]\n"
            resultado_segmento = model([np.array(bloque_inferior_derecho)])

            if resultado_segmento.pages[0].blocks:
                for block in resultado_segmento.pages[0].blocks:
                    for line in block.lines:
                        texto += " ".join([word.value for word in line.words]) + "\n"
            else:
                texto += "[Sin texto detectado en segmento totales]\n"

        doc.close()
        logging.info("DocTR extracción exitosa.")
        return texto
    except Exception as e:
        logging.error(f"Error en DocTR: {e}")
        return ""

# ===========================
# Funciones de Base de Datos
# ===========================

def guardar_texto_total(nombre_archivo, metodo, texto):
    connection = None
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        with connection.cursor() as cursor:
            sql_doc_id = "SELECT id FROM documentos WHERE nombre_archivo = %s"
            cursor.execute(sql_doc_id, (nombre_archivo,))
            result = cursor.fetchone()
            if result:
                documento_id = result[0]
                sql_insert = """
                    INSERT INTO extracciones_texto_total (documento_id, metodo, texto_extraccion)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(sql_insert, (documento_id, metodo, texto))
                connection.commit()
                logging.info(f"Texto guardado exitosamente - Documento: {nombre_archivo}, Metodo: {metodo}")
            else:
                logging.warning(f"Documento no encontrado en base de datos: {nombre_archivo}")
    except Exception as e:
        logging.error(f"Error guardando texto total: {e}")
    finally:
        if connection:
            connection.close()

def actualizar_estado_documento(nombre_archivo, estado):
    connection = None
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        with connection.cursor() as cursor:
            sql_update = """
                UPDATE documentos
                SET estado = %s
                WHERE nombre_archivo = %s
            """
            cursor.execute(sql_update, (estado, nombre_archivo))
            connection.commit()
            logging.info(f"Estado actualizado: {nombre_archivo} -> {estado}")
    except Exception as e:
        logging.error(f"Error actualizando estado documento: {e}")
    finally:
        if connection:
            connection.close()

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
# ===========================
# Procesamiento principal
# ===========================

def procesar_directorio(debug=False):
    if not os.path.exists(DIRECTORIO_PDFS):
        logging.error(f"Directorio no existe: {DIRECTORIO_PDFS}")
        sys.exit(1)

    archivos = [f for f in os.listdir(DIRECTORIO_PDFS) if f.lower().endswith('.pdf')]
    if not archivos:
        if debug:
            print("No hay archivos PDF para procesar.")
        return

    for archivo in archivos:
        path_pdf = os.path.join(DIRECTORIO_PDFS, archivo)
        logging.info(f"Iniciando validación de estado para: {archivo}")

        # Validar si existe en la BD y está en estado 'pendiente'
        connection = None
        try:
            connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, cursorclass=pymysql.cursors.DictCursor)
            with connection.cursor() as cursor:
                cursor.execute("SELECT id, estado FROM documentos WHERE nombre_archivo = %s", (archivo,))
                doc = cursor.fetchone()

                if not doc:
                    logging.warning(f"Documento '{archivo}' no está registrado en la base de datos. Falta procesamiento previo.")
                    continue

                if doc['estado'] != 'pendiente':
                    logging.warning(f"Documento '{archivo}' ya fue procesado (estado actual: {doc['estado']}). Se moverá a excepciones.")
                    mover_a_errores(path_pdf, mensaje_error="Documento ya procesado o con estado diferente a 'pendiente'.")
                    continue

                documento_id = doc['id']
        except Exception as e:
            logging.error(f"Error consultando estado en la base de datos para '{archivo}': {e}")
            continue
        finally:
            if connection:
                connection.close()

        logging.info(f"Iniciando extracción de texto para: {path_pdf}")

        textos = {}

        if USE_PADDLEOCR:
            textos['paddleocr'] = extraer_texto_paddleocr_v2(path_pdf)
        if USE_EASYOCR:
            textos['easyocr'] = extraer_texto_easyocr(path_pdf)
        if USE_TESSERACT4:
            textos['tesseract4'] = extraer_texto_tesseract_psm4(path_pdf)
        if USE_TESSERACT6:
            textos['tesseract6'] = extraer_texto_tesseract_psm6(path_pdf) 
        if USE_DOCTR:
            textos['doctr'] = extraer_texto_doctr(path_pdf)

        for metodo, texto in textos.items():
            if texto.strip():
                logging.info(f"Intentando guardar texto extraído por {metodo}...")
                guardar_texto_total(archivo, metodo, texto)
            else:
                logging.warning(f"No se extrajo texto con {metodo}, no se guarda.")

        actualizar_estado_documento(archivo, 'procesado')

        # Mover archivo a carpeta procesados
        try:
            if not os.path.exists(CARPETA_PROCESADOS):
                os.makedirs(CARPETA_PROCESADOS)
            destino = os.path.join(CARPETA_PROCESADOS, archivo)
            os.rename(path_pdf, destino)
            logging.info(f"Archivo movido a: {destino}")
        except Exception as e:
            logging.error(f"Error moviendo archivo: {e}")



if __name__ == "__main__":
    procesar_directorio(debug=True)
