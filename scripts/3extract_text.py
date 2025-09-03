# ===========================
# Utilidad: Cálculo de entropía de texto
# ===========================
import math
from collections import Counter

def calcular_entropia(texto):
    if not texto:
        return 0.0
    frecuencia = Counter(texto)
    total = len(texto)
    entropia = -sum((f / total) * math.log2(f / total) for f in frecuencia.values())
    return entropia
# ===========================
# LD_LIBRARY_PATH workaround para libcupti.so.12
# ===========================
import os
os.environ['LD_LIBRARY_PATH'] = (
    '/usr/local/lib/python3.9/site-packages/nvidia/cuda_cupti/lib:' +
    os.environ.get('LD_LIBRARY_PATH', '')
)

import ctypes
try:
    ctypes.CDLL("libcupti.so.12")
except OSError as e:
    print(f"⚠️ Advertencia: No se pudo cargar libcupti.so.12 - {e}")

# ===========================
# Multiprocessing: LD_LIBRARY_PATH workaround for child processes
# ===========================

def init_worker():
    global ocr_paddle
    global predictor_doctr

    try:
        if config.getboolean('ocr', 'usar_paddleocr', fallback=True):
            logger.info("Inicializando PaddleOCR...")
            # Evitar 'use_gpu' para compatibilidad entre versiones
            ocr_paddle = PaddleOCR(use_angle_cls=True, lang='es', show_log=False)
            logger.info("PaddleOCR cargado correctamente.")
        else:
            ocr_paddle = None
    except Exception as e:
        logger.error(f"Error inicializando PaddleOCR: {e}")
        ocr_paddle = None

    try:
        if config.getboolean('ocr', 'usar_doctr', fallback=True):
            predictor_doctr = ocr_predictor(det_arch='db_resnet50', reco_arch='crnn_vgg16_bn', pretrained=True)
            logger.info("DocTR cargado correctamente.")
        else:
            predictor_doctr = None
    except Exception as e:
        logger.error(f"Error inicializando DocTR: {e}")
        predictor_doctr = None
_models = {}
#!/usr/bin/env python3
import os
import sys

# ===========================
# Configuración de entorno CUDA antes de importar cualquier librería OCR/CUDA
# ===========================
def configurar_entorno_cuda():
    librerias_cuda = [
        "/usr/local/cuda-11.8/extras/CUPTI/lib64",
        "/usr/locallib64/python3.9/site-packages/nvidia/cublas/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/cudnn/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/cusparse/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/cusparselt/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/cufile/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/cuda_cupti/lib",
        "/usr/local/lib64/python3.9/site-packages/triton/backends/nvidialib64/cupti",
        "/usr/locallib64/python3.9/site-packages/nvidia/curand/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/nccl/lib",
        "/usr/locallib64/python3.9/site-packages/nvidia/cufft/lib",

    ]


    for path in librerias_cuda:
        if path not in os.environ.get("LD_LIBRARY_PATH", ""):
            os.environ["LD_LIBRARY_PATH"] = path + ":" + os.environ.get("LD_LIBRARY_PATH", "")

configurar_entorno_cuda()

import logging
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)
# Manejo robusto de errores al importar y crear el objeto EasyOCR (con reintento CPU si GPU falla)
try:
    import easyocr
    try:
        reader_easyocr = easyocr.Reader(['es'], gpu=False)
        logger.info("EasyOCR cargado correctamente con GPU.")
    except Exception as gpu_e:
        logger.warning(f"Fallo carga EasyOCR con GPU: {gpu_e}. Reintentando con CPU...")
        try:
            reader_easyocr = easyocr.Reader(['es'], gpu=False)
            logger.info("EasyOCR cargado correctamente con CPU.")
        except Exception as cpu_e:
            logger.error(f"Fallo carga EasyOCR con CPU también: {cpu_e}")
            reader_easyocr = None
except Exception as e:
    logger.error(f"Error al importar EasyOCR: {e}")
    reader_easyocr = None
import pymysql
import configparser
import io
import cv2
import shutil
import numpy as np
import multiprocessing
from paddleocr import PaddleOCR
from doctr.models import ocr_predictor
from tqdm import tqdm
from PIL import Image, ImageOps, ImageEnhance
from pathlib import Path
from pdf2image import convert_from_path
import pytesseract
import signal
import warnings
from functools import partial
import argparse
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

# ===========================
#  gar configuraciones
# ===========================
config = configparser.ConfigParser()
try:
    config.read('../config/config.cf')
    USAR_PREPROCESAMIENTO_CV2 = config.getboolean('ocr', 'usar_preprocesamiento_cv2', fallback=True)
    USE_DOCTR = config.getboolean('ocr', 'use_doctr', fallback=True)
    USE_PADDLEOCR = config.getboolean('ocr', 'use_paddleocr', fallback=True)
    USE_TESSERACT4 = config.getboolean('ocr', 'use_tesseract4', fallback=True)
    USE_TESSERACT6 = config.getboolean('ocr', 'use_tesseract6', fallback=True)
    USE_EASYOCR = config.getboolean('ocr', 'use_easyocr', fallback=True)
    USE_NATIVO = config.getboolean('ocr', 'use_nativo', fallback=True)
    DB_HOST = config.get('database', 'host')
    DB_USER = config.get('database', 'user')
    DB_PASS = config.get('database', 'password')
    DB_NAME = config.get('database', 'dbname')
    DIRECTORIO_PDFS = config.get('paths', 'directorio_local_para_procesar')
    DIRECTORIO_ERRORES = config.get('paths', 'directorio_errores', fallback='../errores')
    CARPETA_PROCESADOS = config.get('paths', 'carpeta_procesados')
    TEMP_DIR = config.get('paths', 'directorio_temporal', fallback='./tmp')
    logfile = config.get('logs', 'archivo_log', fallback='../logs/actividad.log')
    PROCESAMIENTO_SIMULTANEO = config.getint('ocr', 'procesamiento_simultaneo', fallback=os.cpu_count())
except Exception as e:
    print(f"Error cargando configuraciones: {e}")
    sys.exit(1)



# Asegurar que las carpetas necesarias existen
for path in [logfile, TEMP_DIR]:
    directory = os.path.dirname(os.path.abspath(path))
    if not os.path.exists(directory):
        os.makedirs(directory)

# Configurar logging
logger = logging.getLogger()
fh = logging.FileHandler(logfile, mode='a', encoding='utf-8')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


# Asegurarse que la variable debug esté definida antes de su uso
try:
    debug
except NameError:
    debug = False

# El nivel del logger se ajusta más adelante según el valor de debug en main()
logger.setLevel(logging.DEBUG if debug else logging.INFO)

if debug:
    logger.info("Logger inicializado correctamente")

# ===========================
# Preprocesamiento de imagen
# ===========================

def preprocesar_imagen(pix):
    img_data = pix.tobytes("png")
    img = Image.open(io.BytesIO(img_data))
    img = ImageOps.grayscale(img)
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.0)
    img_np = np.array(img)
    _, img_bin = cv2.threshold(img_np, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return Image.fromarray(img_bin)



# ===========================
# Preprocesamiento específico por motor OCR
# ===========================

def preprocesar_para_tesseract(imagen_pil):
    imagen = cv2.cvtColor(np.array(imagen_pil.convert("RGB")), cv2.COLOR_RGB2BGR)
    gris = cv2.cvtColor(imagen, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    gris = clahe.apply(gris)
    binarizada = cv2.adaptiveThreshold(gris, 255,
                                       cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY_INV, 11, 2)
    desenfocada = cv2.medianBlur(binarizada, 3)
    return desenfocada

def preprocesar_para_paddleocr(imagen_pil):
    # PaddleOCR requiere imagen en RGB y clara
    return cv2.cvtColor(np.array(imagen_pil.convert("RGB")), cv2.COLOR_RGB2BGR)

def preprocesar_para_easyocr(imagen_pil):
    imagen = cv2.cvtColor(np.array(imagen_pil.convert("RGB")), cv2.COLOR_RGB2BGR)
    gris = cv2.cvtColor(imagen, cv2.COLOR_BGR2GRAY)
    return cv2.equalizeHist(gris)

def preprocesar_imagen_cv2(entrada, blockSize=11, C=2, kernel_size=3):
    """
    Preprocesa una imagen (ruta, PIL.Image o numpy array) con OpenCV.
    Aplica CLAHE, binarización adaptativa, limpieza de ruido y deskew.
    Retorna un array listo para OCR.
    """
    if isinstance(entrada, str):
        imagen = cv2.imread(entrada)
        if imagen is None:
            raise ValueError(f"No se pudo cargar la imagen desde ruta: {entrada}")
    elif isinstance(entrada, Image.Image):
        imagen = cv2.cvtColor(np.array(entrada.convert("RGB")), cv2.COLOR_RGB2BGR)
    elif isinstance(entrada, np.ndarray):
        imagen = entrada
    else:
        raise TypeError("Tipo de entrada no válido para preprocesamiento")

    gris = cv2.cvtColor(imagen, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    gris = clahe.apply(gris)
    binarizada = cv2.adaptiveThreshold(gris, 255,
                                       cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY_INV, blockSize, C)
    desenfocada = cv2.medianBlur(binarizada, kernel_size)
    kernel = np.ones((2, 2), np.uint8)
    procesada = cv2.morphologyEx(desenfocada, cv2.MORPH_CLOSE, kernel)

    coords = np.column_stack(np.where(procesada > 0))
    if len(coords) > 0:
        angle = cv2.minAreaRect(coords)[-1]
        if angle < -45:
            angle = -(90 + angle)
        elif angle > 45:
            angle = -(90 - angle)
        else:
            angle = -angle

        (h, w) = procesada.shape[:2]
        M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
        enderezada = cv2.warpAffine(procesada, M, (w, h),
                                    flags=cv2.INTER_CUBIC,
                                    borderMode=cv2.BORDER_REPLICATE)
    else:
        enderezada = procesada

    return enderezada


# ===========================
# Funciones de extraccion OCR
# ===========================
def extraer_texto_paddleocr_v2(path_pdf, ocr, debug=False):
    dpi = 300
    try:
        # Usar la variable global ocr_paddle si ocr es None
        global ocr_paddle
        if ocr is None:
            ocr = ocr_paddle
        imagenes = convert_from_path(path_pdf, dpi=dpi)
        texto = ""

        for imagen in imagenes:
            imagen_np = np.array(imagen)

            # OCR sobre la imagen completa
            resultados = ocr.ocr(imagen_np, cls=True)
            if resultados is None:
                logging.warning("PaddleOCR retornó None en resultados de imagen principal.")
                continue
            if resultados:
                for bloque in resultados:
                    if bloque:
                        for linea in bloque:
                            texto += linea[1][0] + "\n"
            else:
                logging.warning("PaddleOCR retornó resultados vacíos en imagen principal.")

            # OCR sobre el bloque derecho inferior
            bloque_derecho = dividir_y_extraer_inferior_derecha(imagen, debug=debug)
            if bloque_derecho is None:
                logging.warning("Segmento derecho inferior no generado correctamente.")
                continue  # pasa a la siguiente página
            else:
                bloque_np = np.array(bloque_derecho)
                resultado_bloque = ocr.ocr(bloque_np, cls=True)
                if resultado_bloque is None:
                    logging.warning("PaddleOCR retornó None en resultados del bloque inferior derecho.")
                    continue

                texto += "\n[SEGMENTO_TOTALES]\n"

                if resultado_bloque:
                    for bloque in resultado_bloque:
                        if bloque:
                            for linea in bloque:
                                texto += linea[1][0] + "\n"
                else:
                    texto += "[Sin texto detectado en segmento totales]\n"

        if debug:
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

""" def extraer_texto_paddleocr_v2_(path_pdf):
    dpi = 300
    try:
 #       ocr = PaddleOCR(use_angle_cls=True, lang='es', use_gpu=False)
        ocr = PaddleOCR(
            use_angle_cls=True,
            lang='es',
            use_gpu=True,
            det_model_dir='/AI/ia_sch/models/paddle/det_model_es',
            rec_model_dir='/AI/ia_sch/models/paddle/rec_model_es',
            cls_model_dir='/AI/ia_sch/models/paddle/cls_model_es'
        )
        imagenes = convert_from_path(path_pdf, dpi=dpi)
        texto = ""

        for imagen in imagenes:
            if USAR_PREPROCESAMIENTO_CV2:
                imagen_np = preprocesar_para_paddleocr(imagen)  # PIL.Image -> OpenCV np.ndarray
            else:
                imagen_np = np.array(imagen)  # sin procesar

            resultados = ocr.ocr(imagen_np, cls=True)
            if resultados:
                for bloque in resultados:
                    for linea in bloque:
                        texto += linea[1][0] + "\n"

        if debug:
            logging.info("PaddleOCR extracción exitosa.")
        return texto.strip()

    except Exception as e:
        logging.error(f"Error en PaddleOCR v2: {e}")
        return ""

def extraer_texto_paddleocr(path_pdf):
    try:
        #ocr = PaddleOCR(use_angle_cls=True, lang='es')
        ocr = PaddleOCR(
                use_angle_cls=True,
                lang='es',
                det_model_dir='/AI/ia_sch/models/paddle/det_model_es',
                rec_model_dir='/AI/ia_sch/models/paddle/rec_model_es',
                cls_model_dir='/AI/ia_sch/models/paddle/cls_model_es'
            )
        doc = fitz.open(path_pdf)
        texto = ""

        for i, page in enumerate(doc):
            pix = page.get_pixmap(dpi=300)
            img_pil = Image.open(io.BytesIO(pix.tobytes("png")))

            # === Preprocesar o no según configuración ===
            if USAR_PREPROCESAMIENTO_CV2:
                img_cv2 = preprocesar_para_paddleocr(img_pil)
            else:
                img_cv2 = np.array(img_pil)

            # Ejecutar OCR sobre imagen principal
            result = ocr.ocr(img_cv2, cls=True)
            texto += " ".join([line[1][0] for block in result for line in block]) + "\n"

            # === Segmento inferior derecho ===
            bloque = dividir_y_extraer_inferior_derecha(img_pil, debug=False)
            texto += "\n[SEGMENTO_TOTALES]\n"

            if bloque:
                if USAR_PREPROCESAMIENTO_CV2:
                    bloque_cv2 = preprocesar_para_paddleocr(bloque)
                else:
                    bloque_cv2 = np.array(bloque)

                result_bloque = ocr.ocr(bloque_cv2, cls=True)
                texto += " ".join([line[1][0] for block in result_bloque for line in block]) + "\n"
            else:
                texto += "[Sin texto detectado en segmento totales]\n"

        doc.close()
        if debug:
            logging.info("PaddleOCR extracción exitosa.")
        return texto.strip()

    except Exception as e:
        logging.error(f"Error en PaddleOCR: {e}")
        return ""
"""

def extraer_texto_easyocr(path_pdf, reader, debug=False):
    try:
        doc = fitz.open(path_pdf)
        texto = "" 
        
        for i, page in enumerate(doc):
            pix = page.get_pixmap(dpi=300)
            img_pil = Image.open(io.BytesIO(pix.tobytes("png")))

            # === Preprocesar si está habilitado ===
            if USAR_PREPROCESAMIENTO_CV2:
                img_np = preprocesar_para_easyocr(img_pil)
                img_pil = Image.fromarray(img_np)

            temp_path = os.path.join(TEMP_DIR, f"temp_easyocr_{i}.png")
            img_pil.save(temp_path)

            # Validar si reader_easyocr está disponible antes de llamar readtext
            if reader is not None:
                results = reader.readtext(temp_path, detail=0)
            else:
                raise ValueError("EasyOCR no está disponible.")
            texto_pagina = " ".join(results)
            texto += texto_pagina + "\n"

            if debug:
                print(f"[DEBUG] Página {i+1}: {texto_pagina}")

            os.remove(temp_path)

        doc.close()
        if debug:
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
            pix = page.get_pixmap(dpi=300)
            img_pil = Image.open(io.BytesIO(pix.tobytes("png")))

            # === Preprocesamiento condicional ===
            if USAR_PREPROCESAMIENTO_CV2:
                img_np = preprocesar_para_tesseract(img_pil)      # procesado con OpenCV
                img = Image.fromarray(img_np)                 # convertir a PIL.Image
            else:
                img = img_pil

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
            img_pil = Image.open(io.BytesIO(pix.tobytes("png")))

            if USAR_PREPROCESAMIENTO_CV2:
                img_np = preprocesar_para_tesseract(img_pil)
                img = Image.fromarray(img_np)
            else:
                img = img_pil

            texto_extraido = pytesseract.image_to_string(img, config=custom_config)
            if debug:
                print(f"Texto extraído página {num}:\n{texto_extraido}\n{'-'*40}")
            texto += texto_extraido + "\n"

        doc.close()
        logging.info("Tesseract PSM6 extracción exitosa.")
        return texto.strip()
    except Exception as e:
        logging.error(f"Error en Tesseract PSM6: {e}")
        return ""
        
def extraer_texto_tesseract_psm4(path_pdf, debug=False):
    try:
        doc = fitz.open(path_pdf)
        texto = ""
        whitelist = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.:,;/()$%&+°ÑñáéíóúÁÉÍÓÚ"
        custom_config = f'--psm 4 -l spa -c tessedit_char_whitelist="{whitelist}"'

        for num, page in enumerate(doc, 1):
            pix = page.get_pixmap(dpi=300)
            img_pil = Image.open(io.BytesIO(pix.tobytes("png")))

            if USAR_PREPROCESAMIENTO_CV2:
                img_np = preprocesar_para_tesseract(img_pil)
                img = Image.fromarray(img_np)
            else:
                img = img_pil

            texto_extraido = pytesseract.image_to_string(img, config=custom_config)
            if debug:
                print(f"Texto extraído página {num}:\n{texto_extraido}\n{'-'*40}")
            texto += texto_extraido + "\n"

        doc.close()
        if debug:
            logging.info("Tesseract PSM4 extracción exitosa.")
        return texto.strip()
    except Exception as e:
        logging.error(f"Error en Tesseract PSM4: {e}")
        return ""

def extraer_texto_doctr(path_pdf, model, debug=False):
    try:
        global predictor_doctr
        if model is None:
            model = predictor_doctr
        doc = fitz.open(path_pdf)
        texto = ""

        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img_pil = Image.open(io.BytesIO(pix.tobytes('png'))).convert('RGB')

            if USAR_PREPROCESAMIENTO_CV2:
                img_np = preprocesar_imagen_cv2(img_pil)
                # Asegurar que img_np tenga 3 canales (RGB) para DocTR
                if len(img_np.shape) == 2:
                    img_np = cv2.cvtColor(img_np, cv2.COLOR_GRAY2RGB)
            else:
                img_np = np.array(img_pil)

            result = model([img_np])
            for block in result.pages[0].blocks:
                for line in block.lines:
                    texto += " ".join([word.value for word in line.words]) + "\n"

            bloque_inferior_derecho = dividir_y_extraer_inferior_derecha(img_pil, debug=False)
            texto += "\n[SEGMENTO_TOTALES]\n"

            if bloque_inferior_derecho:
                if USAR_PREPROCESAMIENTO_CV2:
                    bloque_np = preprocesar_imagen_cv2(bloque_inferior_derecho)
                    # Asegurar que bloque_np tenga 3 canales (RGB) para DocTR
                    if len(bloque_np.shape) == 2:
                        bloque_np = cv2.cvtColor(bloque_np, cv2.COLOR_GRAY2RGB)
                else:
                    bloque_np = np.array(bloque_inferior_derecho)

                resultado_segmento = model([bloque_np])
                if resultado_segmento.pages[0].blocks:
                    for block in resultado_segmento.pages[0].blocks:
                        for line in block.lines:
                            texto += " ".join([word.value for word in line.words]) + "\n"
                else:
                    texto += "[Sin texto detectado en segmento totales]\n"
            else:
                texto += "[Sin texto detectado en segmento totales]\n"

        doc.close()
        logging.info("DocTR extracción exitosa.")
        return texto.strip()
    except Exception as e:
        logging.error(f"Error en DocTR: {e}")
        return ""


def extraer_texto_nativo(pdf_path, modo="blocks", debug=False):
    """
    Extrae texto nativo desde un PDF con PyMuPDF (fitz).
    
    Parámetros:
        pdf_path (str): Ruta al archivo PDF.
        modo (str): Modo de extracción: 'plain', 'blocks', 'layout'.
        debug (bool): Si es True, imprime info de cada página.
    
    Retorna:
        str: Texto extraído concatenado de todas las páginas.
    """
    try:
        doc = fitz.open(pdf_path)
        texto_total = ""

        for i, page in enumerate(doc):
            if modo == "plain":
                texto = page.get_text("text")
            elif modo == "blocks":
                bloques = page.get_text("blocks")
                bloques = sorted(bloques, key=lambda b: (b[1], b[0]))  # ordenar por posición y altura
                texto = "\n".join(b[4].strip() for b in bloques if b[4].strip())
            elif modo == "layout":
                texto = page.get_text("dict")
                texto = "\n".join(
                    word['text']
                    for block in texto["blocks"]
                    if "lines" in block
                    for line in block["lines"]
                    for span in line["spans"]
                    for word in [span]
                    if word['text'].strip()
                )
            else:
                raise ValueError(f"Modo de extracción inválido: {modo}")

            if debug:
                print(f"[Página {i+1}] - {len(texto)} caracteres")

            texto_total += f"\n--- Página {i+1} ---\n"
            texto_total += texto.strip() + "\n"

        doc.close()

        # Normalización UTF-8 limpia
        texto_utf8 = texto_total.encode("utf-8", errors="ignore").decode("utf-8")
        return texto_utf8.strip()

    except Exception as e:
        logging.error(f"Error extrayendo texto nativo desde PDF: {e}")
        return ""
    
    
# ===========================
# Funciones de Base de Datos
# ===========================

def guardar_texto_total(nombre_archivo, metodo, texto):
    """
    Guarda el texto extraído en la base de datos. Usa logger para registrar eventos.
    Compatible con multiprocessing.
    Incluye cálculo y guardado de entropía del texto.
    """
    connection = None
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        try:
            with connection.cursor() as cursor:
                sql_doc_id = "SELECT id FROM documentos WHERE nombre_archivo = %s and estado=1 LIMIT 10"
                cursor.execute(sql_doc_id, (nombre_archivo,))
                result = cursor.fetchone()
                if result:
                    documento_id = result[0]
                    # Calcular entropía antes de insertar
                    entropia_valor = calcular_entropia(texto)
                    sql_insert = """
                        INSERT INTO extracciones_texto_total (documento_id, metodo, texto_extraccion, entropia)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE texto_extraccion = VALUES(texto_extraccion), entropia = VALUES(entropia)
                    """
                    cursor.execute(sql_insert, (documento_id, metodo, texto, entropia_valor))
                       # Actualizar estado en la base de datos a 2 'texto_ok'
                    actualizar_estado_documento(nombre_archivo, 2)
                    
                    logger.info(f"Texto guardado exitosamente - Documento: {nombre_archivo}, Metodo: {metodo}")
                    logger.info(f"Texto con entropía {entropia_valor:.2f} para el método {metodo}")
                else:
                    logger.warning(f"Documento no encontrado en base de datos: {nombre_archivo}")
            connection.commit()
        except Exception as inner_e:
            logger.error(f"Error al insertar texto total: {inner_e}")
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
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
    nombre_archivo = os.path.basename(path_pdf)
    os.makedirs(DIRECTORIO_ERRORES, exist_ok=True)
    destino_pdf = os.path.join(DIRECTORIO_ERRORES, nombre_archivo)
    try:
        shutil.move(path_pdf, destino_pdf)
        logging.warning(f"Archivo movido a carpeta de errores: {path_pdf}")

        # Actualizar estado en la base de datos a 'excepcion'
        actualizar_estado_documento(nombre_archivo, 500)

        if mensaje_error:
            log_path = os.path.splitext(destino_pdf)[0] + ".log"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"Error al procesar: {nombre_archivo}\n")
                f.write(f"{mensaje_error.strip()}\n")

    except Exception as e:
        logging.error(f"Error moviendo archivo a errores o actualizando BD: {e}")
# ===========================
# Procesamiento principal
# ===========================


# ===========================
# Multiprocessing: Procesamiento de archivos PDF en paralelo
# ===========================


def procesar_archivo_con_modelos(path_pdf):
    global _models
    archivo = os.path.basename(path_pdf)
    try:
        logger.info(f"[{multiprocessing.current_process().name}] Procesando archivo: {archivo}")
        connection = None
        try:
            connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, cursorclass=pymysql.cursors.DictCursor)
            with connection.cursor() as cursor:
                cursor.execute("SELECT id, estado FROM documentos WHERE nombre_archivo = %s and estado=1 order by id desc ", (archivo,))
                doc = cursor.fetchone()

                if not doc:
                    logger.warning(f"Documento '{archivo}' no está registrado en la base de datos. Falta procesamiento previo.")
                    return

                if doc['estado'] != 1:
                    logger.warning(f"Documento '{archivo}' ya fue procesado (estado actual: {doc['estado']}). Se moverá a excepciones.")
                    mover_a_errores(path_pdf, mensaje_error="Documento ya procesado o con estado diferente a 1 o 'pendiente'.")
                    return

                if doc['estado'] == 1:
                    if debug:
                        logger.info(f"Iniciando extracción de texto para: {path_pdf}")
                    textos = {}

                
                    try:
                        if USE_PADDLEOCR:
                            ocr_paddle = _models.get('paddleocr', None)
                            textos['paddleocr'] = extraer_texto_paddleocr_v2(path_pdf, ocr_paddle, debug=False)
                        if USE_EASYOCR:
                            ocr_easy = _models.get('easyocr', None)
                            textos['easyocr'] = extraer_texto_easyocr(path_pdf, ocr_easy, debug=False)
                        if USE_TESSERACT4:
                            textos['tesseract4'] = extraer_texto_tesseract_psm4(path_pdf, debug=False)
                        if USE_TESSERACT6:
                            textos['tesseract6'] = extraer_texto_tesseract_psm6(path_pdf, debug=False)
                        if USE_DOCTR:
                            ocr_doctr = _models.get('doctr', None)
                            textos['doctr'] = extraer_texto_doctr(path_pdf, ocr_doctr, debug=False)
                        if USE_NATIVO:
                            textos['nativo'] = extraer_texto_nativo(path_pdf, modo="blocks", debug=False)
                    except Exception as ocr_e:
                        logger.error(f"Error durante extracción OCR de '{archivo}': {ocr_e}")
                        mover_a_errores(path_pdf, mensaje_error=f"Error OCR: {ocr_e}")
                        return

                    for metodo, texto in textos.items():
                        if texto.strip():
                            guardar_texto_total(archivo, metodo, texto)
                        else:
                            logger.warning(f"No se extrajo texto con {metodo}, no se guarda.")

                    # Mover archivo PDF a carpeta de procesados
                    if not os.path.exists(CARPETA_PROCESADOS):
                        os.makedirs(CARPETA_PROCESADOS)
                    destino = os.path.join(CARPETA_PROCESADOS, archivo)
                    os.rename(path_pdf, destino)
                    if debug:
                        logger.info(f"Archivo movido a: {destino}")
                        logger.info(f"[{multiprocessing.current_process().name}] Finalizado: {archivo}")
                else:   
                    logger.info(f"archivo no en estado 1  no se proceso. : {path_pdf}")
                          

        except Exception as e:
            logger.error(f"Error procesando archivo '{archivo}': {e}")
        finally:
            if connection:
                connection.close()
    except Exception as e:
        logger.error(f"Error general al procesar '{archivo}': {e}")


# Inicializador para modelos OCR con validación de rutas y logs
DETECT_MODEL_DIR = '/AI/ia_sch/models/paddle/det_model_es'
REC_MODEL_DIR = '/AI/ia_sch/models/paddle/rec_model_es'
CLS_MODEL_DIR = '/AI/ia_sch/models/paddle/cls_model_es'

def initializer():
    global _models
    try:
        if USE_PADDLEOCR:
            # Validación explícita de rutas
            if not (os.path.isdir(DETECT_MODEL_DIR) and os.path.isdir(REC_MODEL_DIR) and os.path.isdir(CLS_MODEL_DIR)):
                logger.error(f"Una o más rutas de modelos PaddleOCR no existen: det={DETECT_MODEL_DIR}, rec={REC_MODEL_DIR}, cls={CLS_MODEL_DIR}")
            else:
                try:
                    logger.info("Inicializando PaddleOCR...")
                    _models['paddleocr'] = PaddleOCR(
                        use_angle_cls=True,
                        lang='es',
                        det_model_dir=DETECT_MODEL_DIR,
                        rec_model_dir=REC_MODEL_DIR,
                        cls_model_dir=CLS_MODEL_DIR
                    )
                    logger.info("PaddleOCR cargado correctamente.")
                except Exception as e:
                    logger.error("❌ Error al cargar PaddleOCR: %s", e)
                    raise
        if USE_EASYOCR:
            # Usar el reader_easyocr si está disponible, si no, None
            _models['easyocr'] = reader_easyocr
            if reader_easyocr is not None:
                logger.info("EasyOCR cargado correctamente.")
            else:
                logger.error("EasyOCR no está disponible.")
        if USE_DOCTR:
            _models['doctr'] = ocr_predictor(pretrained=True)
            logger.info("DocTR cargado correctamente.")
    except Exception as e:
        logger.error(f"Error inicializando modelos OCR: {e}")




def procesar_directorio(debug=False):
    if not os.path.exists(DIRECTORIO_PDFS):
        logger.error(f"Directorio no existe: {DIRECTORIO_PDFS}")
        sys.exit(1)

    archivos = [f for f in os.listdir(DIRECTORIO_PDFS) if f.lower().endswith('.pdf')]
    if not archivos:
        logger.warning("No hay archivos PDF para procesar.")
        return

    paths_pdf = [os.path.join(DIRECTORIO_PDFS, f) for f in archivos if f.lower().endswith('.pdf')]
    total = len(paths_pdf)
    print(f"Total archivos PDF detectados: {total}")
    if debug:
        logger.info(f"Total de archivos PDF detectados para procesamiento: {total}")

    if total == 0:
        logger.warning("No hay archivos PDF para procesar. Terminando ejecución.")
        return

    cantidad_archivos = len(paths_pdf)
    procesos_a_utilizar = min(PROCESAMIENTO_SIMULTANEO, cantidad_archivos)

    if procesos_a_utilizar == 0:
        logging.info("📭 No hay archivos PDF para procesar.")
        return

    logging.info(f"🧵 Se utilizarán {procesos_a_utilizar} procesos para {cantidad_archivos} archivos.")

    print(f"Iniciando pool con {procesos_a_utilizar} procesos simultáneos")
    print("Antes de iniciar pool de procesos")
    if debug:
        logger.info("Antes de iniciar pool de procesos")

    def handler_sigterm(signum, frame):
        logger.info(f"Señal de finalización recibida: {signum}")

    signal.signal(signal.SIGTERM, handler_sigterm)

    warnings.filterwarnings("ignore", message=".*SIGTERM.*")

    with multiprocessing.Pool(
        processes=procesos_a_utilizar,
        initializer=init_worker
    ) as pool:
        for resultado in tqdm(pool.imap_unordered(procesar_archivo_con_modelos, paths_pdf), total=total, desc="Procesando PDF"):
            print("Subproceso ejecutado")
            if debug:
                logger.info("Subproceso ejecutado")
        pool.close()
        pool.join()
        if debug:
            logger.info("Procesamiento completo. Todos los subprocesos finalizados correctamente.")
    try:
        pass
    except KeyboardInterrupt:
        logger.warning("Interrupción manual detectada. Finalizando ejecución.")
    except Exception as e:
        logger.error(f"Error finalizando el pool de procesos: {e}")



def main():

    multiprocessing.set_start_method("spawn", force=True)

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Activa modo debug')
    args, _ = parser.parse_known_args()
    global debug
    debug = args.debug

   # Suprimir logs de PaddleOCR y paddle si no está activado debug
    if not debug:
        logging.getLogger('ppocr').setLevel(logging.ERROR)
        logging.getLogger('ppocr').propagate = False
    else:
        logging.getLogger('ppocr').setLevel(logging.INFO)
        logging.getLogger('ppocr').propagate = True

    print("Main iniciado correctamente")
    procesar_directorio(debug=args.debug)

if __name__ == "__main__":
    try:
        main()  # o lo que sea que se ejecute
    except Exception as e:
        import traceback
        print("❌ Error no controlado en 3extract_text.py:", e)
        traceback.print_exc()
        sys.exit(1)
  
