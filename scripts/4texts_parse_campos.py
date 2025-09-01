#!/usr/bin/env python3
# ###   python3 texts_parse_campos.py --forzar_id=90  
# Listado de campos
# documento_id, metodo, archivo_origen, tipo_doc, numero_documento, localidad, fecha_documento, nombre_proveedor, rut_proveedor, nombre_comprador, rut_comprador, direccion_comprador, telefono_comprador, comuna_comprador, ciudad_comprador, placa_patente, tipo_vehiculo, marca, modelo, n_motor, n_chasis, vin, serie, color, anio, unidad_pbv, pbv, cit, combustible, unidad_carga, carga, asientos, puertas, unidad_potencia, potencia_motor, ejes, traccion, tipo_carroceria, cilindrada, transmision, monto_neto, monto_iva, monto_total
import os
import json
import re
import pandas as pd
import sys
import configparser
import pymysql
import logging
import argparse
import csv
import difflib
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from num2words import num2words
from rapidfuzz import fuzz, process


config = configparser.ConfigParser()
config.read('../config/config.cf')

# Verificar existencia de diccionarios
def verificar_diccionario(ruta, nombre):
    if not os.path.isfile(ruta):
        print(f"[ERROR] No se encontró el diccionario de {nombre}: {ruta}")
        sys.exit(1)

ruta_colores = config.get('extraccion', 'ruta_diccionario_colores')
ruta_carrocerias = config.get('extraccion', 'ruta_diccionario_carrocerias')
ruta_comunas = config.get('extraccion', 'ruta_diccionario_comunas')
ruta_ciudades = config.get('extraccion', 'ruta_diccionario_ciudades')
ruta_marcas = config.get('extraccion', 'ruta_diccionario_marcas', fallback='../diccionarios/marcas.csv')

verificar_diccionario(ruta_colores, "colores")
verificar_diccionario(ruta_carrocerias, "carrocerias")
verificar_diccionario(ruta_comunas, "comunas")
verificar_diccionario(ruta_ciudades, "ciudades")
verificar_diccionario(ruta_marcas, "marcas")

directorio_salida_csv = config.get('extraccion', 'directorio_salida_csv', fallback='../resultados')
directorio_salida_json = config.get('extraccion', 'directorio_salida_json', fallback='../resultados')
# Crear los directorios de salida si no existen
if not os.path.exists(directorio_salida_csv):
    os.makedirs(directorio_salida_csv)

if not os.path.exists(directorio_salida_json):
    os.makedirs(directorio_salida_json)

GUARDAR_CSV = config.getboolean('extraccion', 'guardar_csv', fallback=True)
GUARDAR_JSON = config.getboolean('extraccion', 'guardar_json', fallback=True)
GUARDAR_BD = config.getboolean('extraccion', 'guardar_bd', fallback=False)

archivo_log = config.get('logs', 'archivo_log', fallback='../logs/actividad.log')
# Configurar logging usando la ruta del config
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s"
logging.basicConfig(filename=archivo_log, level=logging.INFO, format=LOG_FORMAT)

# ETIQUETAS DE TERMINO DE CAMPOS 
ETIQUETAS_FIN_CAMPOS = [
        "COLOR", "CHASIS", "PESO", "TRANSMISION", "TRACCION", "CILINDRADA", "PATENTE",
        "TIPO VEHICULO", "ANO", "MODELO", "LLAVES", "PUERTAS", "ASIENTOS", "CODIGO", "CIT", "PBV","N/A"
    ]

def normalizar_texto(texto):
    texto = texto.upper()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    texto = texto.replace("0", "O").replace("1", "I").replace("5", "S").replace("4", "A").replace("3", "E").replace("8", "B")
    texto = re.sub(r"[^A-Z\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


# Lista de sinónimos o errores comunes para cada tipo (con exclusiones)
# Todos los tipos válidos deben estar presentes como tuplas (sinonimo, None)
SINONIMOS_TIPOS_DOCUMENTOS = {
    "SOLICITUD PRIMERA INSCRIPCION": [
        ("SOLICITUD PRIMERA INSCRIPCION", None),
        ("SOLICITUD DE PRIMERA INSCRIPCION", None),
        ("INSCRIPCION PRIMERA", None),
        ("SOLICITUDELECTRONICA", None),
        ("SOLICITUD ELECTRONICA DE PRIMERAINSCRIPCION", None),
        ("SOLICITUD ELECTRONICA DE PRIMERAINSCRIPCION R.V.M.", None),
        ("SOLICITUD ELECTRONICA DE PRIMERAINSCRIPCION RVM", None)
    ],
    "NOTA DE CREDITO ELECTRONICA": [
        ("NOTA DE CREDITO ELECTRONICA", None)
    ],
    "FACTURA ELECTRONICA": [
        ("FACTURA ELECTRONICA", None),
        ("FCTRA ELEC", None),
        ("FCT ELEC", None),
        ("FACT ELEC", None),
        ("ACTURA ELECTRONICA", None),
        ("FACTURAELECTRONICA", None)
    ],
    "NOTA DE CREDITO": [
        ("NOTA DE CREDITO", None),
        ("NDC", None),
        ("NOT CRED", None),
        ("NOTA CREDITO", None)
    ],
    "ORDEN DE COMPRA": [
        ("ORDEN DE COMPRA", None),
        ("ORDEN COMPRA", None),
        ("ORDN DCOMPR", None),
        ("ORD DE COMPR", None)
    ],
    "HOMOLOGADO": [
        ("HOMOLOGADO", None),
        ("CERTIFICADODECUMPLIMIENTO", None),
        ("D.S.55/94", None),
        ("55/94", None),
        ("55/94", "ONIH"),
        ("MINISTERIO DE TRANSPORTES Y", None)
    ],
    "CEDULA DE IDENTIDAD": [
        ("CEDULA DE IDENTIDAD", None),
        ("CED IDENTIDAD", None),
        ("CEDULA ID", None),
        ("CED ID", None),
        ("CEDULA DE", None),
        ("INCHL5", None),
        ("INCHLS", None)
    ],
    "ACTA DE RECEPCION": [
        ("ACTA DE RECEPCION", None),
        ("ACTA RECEPCION", None),
        ("RECEPCION BIENES", None),
        ("ACTA...DE RECEPCION CONFORME", None),
        ("ACTA DE RECEPCION CONFORME", None),
        ("ACTA DE RECEPCION CONFORME DE BIENES", None)
    ],
    "HOJA CONTROL DE PAGOS": [
        ("HOJA CONTROL DE PAGOS", None)
    ],
    "CONTRATO": [
        ("CONTRATO", None)
    ],
    "ROL UNICO TRIBUTARIO": [
        ("ROL UNICO TRIBUTARIO", None)
    ],
    "REVISION TECNICA": [
        ("REVISION TECNICA", None),
        ("CERTIFICADO EMISIONES CONTAMINANTES", None),
        ("REV TECNICA", None),
        ("CERT REV", None),
        ("REVISION TECNICA", "55/94"),
        ("REVISION TECNICA", "CERTIFICADO DE CUMPLIMIENTO"),
        ("EMISIONESCONTAMINANTES", None)
    ],
    "REGISTRO DE COMPRA": [
        ("REGISTRO DE COMPRA", None)
    ]
}

# Construir mapa de variantes para comparación inversa, con exclusión
VARIANTES_TIPOS_DOCUMENTOS = {
    normalizar_texto(f): (tipo, normalizar_texto(exclusor) if exclusor else None)
    for tipo, frases in SINONIMOS_TIPOS_DOCUMENTOS.items()
    for f, exclusor in frases
}



def generar_ngrams(palabras, n=3):
    return [' '.join(palabras[i:i+n]) for i in range(len(palabras)-n+1)]

def extraer_tipo_documento(texto, debug=False):
    texto_normalizado = normalizar_texto(texto)

    # Mostrar texto normalizado antes de buscar sinónimos, si debug=True
    if debug:
        print("Texto normalizado para análisis:")
        print(texto_normalizado)
        print("="*40)


    # === Eliminar frases irrelevantes antes de evaluar similitud
    FRASES_BASURA = {"SS A", "S A", "S.", "S.A.", "S"}
    palabras = texto_normalizado.split()
    palabras = [p for p in palabras if p.upper() not in FRASES_BASURA]

    # === 4. Corrección por similitud (fallback)
    frases = []
    for n in range(2, 6):
        frases.extend(generar_ngrams(palabras, n))

    # Buscar similitud con frases conocidas
    frases_unidas = frases + palabras
    candidatos = list(VARIANTES_TIPOS_DOCUMENTOS.keys())

    for frase in frases_unidas:
        # Excluir falsos positivos comunes antes de buscar similitudes
        if frase.strip().upper() in ["SS", "S", "SS A"]:
            continue  # evitar falsos positivos comunes
        # Validación para filtrar frases OCR basura que provocan errores
        frase_limpia = re.sub(r'[^A-Z]', '', frase.upper())
        if frase_limpia in ["SS", "SSA", "S"]:
            continue  # Evita falsos positivos como 'HOMOLOGADO' por basura OCR
        match = difflib.get_close_matches(frase, candidatos, n=1, cutoff=0.75)
        if match:
            tipo_detectado, exclusor = VARIANTES_TIPOS_DOCUMENTOS[match[0]]
            if exclusor and exclusor in texto_normalizado:
                if debug:
                    print(f"⛔ Exclusor '{exclusor}' detectado para tipo '{tipo_detectado}', se descarta coincidencia con '{match[0]}'")
                continue
            if debug:
                print(f"🤖 Tipo de documento corregido por similitud: {tipo_detectado} (desde: {match[0]})")
            return tipo_detectado

    if debug:
        print("⚠️ Tipo de documento no identificado")
    return "DESCONOCIDO"

def extraer_numero_documento(texto):
    # Incluye "Nº", "N°", "N9", "N o", "FOLIO", "NO", "N*", etc.
    # Si se modifica este patrón, también se debe modificar el de "extraer_nombre_comprador"
    patron = r"(?<!\w)(?:N[º°9*]?\s*[:\-]?\s*|N\s*[Oo]\s*[:\-]?\s*|FOLIO\s*[:\-]?\s*|NO\s*[:\-]?\s*)(\d{5,})(?!\w)"    
    match = re.search(patron, texto.upper())
    return str(match.group(1).strip()) if match else ""

def extraer_localidad(texto, comunas_permitidas=None):
    """
    Extrae la localidad posterior a variantes comunes (incluso mal escritas) de 'SII'.
    Maneja errores comunes de OCR como S11, SLL, 5II, etc.

    Retorna la comuna si está en el listado permitido.
    """
    if comunas_permitidas is None:
        comunas_permitidas = cargar_diccionario_comunas(ruta_comunas)

    texto = texto.upper()

    # Variantes aceptadas por errores de OCR (usamos regex para mayor tolerancia)
    variantes_sii = [
        r"\bS[\.\s\-]*I[\.\s\-]*I[\.\s\-]*",    # SII, S.I.I, S I I
        r"\bS[\.\s\-]*1[\.\s\-]*1[\.\s\-]*",    # S11
        r"\b5[\.\s\-]*I[\.\s\-]*I[\.\s\-]*",    # 5II
        r"\bS[\.\s\-]*L[\.\s\-]*L[\.\s\-]*",    # SLL
        r"\bS[\.\s\-]*I[\.\s\-]*1[\.\s\-]*",    # SI1
        r"\bS[\.\s\-]*1[\.\s\-]*I[\.\s\-]*",    # S1I
        r"\bS[\.\s\-]*L[\.\s\-]*I[\.\s\-]*",    # SLI
        r"\bS[\.\s\-]*I[\.\s\-]*1[\.\s\-]*",    # S.I1
    ]

    for variante in variantes_sii:
        # Buscar comuna justo después de variante "SII"
        patron = rf"{variante}[-:\s]*([A-Z\s]{{3,30}})"
        match = re.search(patron, texto)
        if match:
            candidato = match.group(1).strip()
            candidato = re.split(r'[\n\r:.,]', candidato)[0].strip()
            for comuna in comunas_permitidas:
                if comuna in candidato:
                    return comuna

    return ""

def extraer_fecha_documento_old(texto, debug=False):
    """
    Extrae la fecha de emisión del documento a partir de varias etiquetas posibles
    y de múltiples formatos de fecha (numérico, texto, mixto).
    Si debug=True, imprime información de seguimiento y detiene al encontrar la fecha.
    """

    # Definimos las etiquetas que pueden preceder a la fecha
    etiquetas = [
        r"FECHA\s*EMISIÓN?\.?",    # Fecha Emisión
        r"EMITIDO",               # Emitido
        r"SANTIAGO,",             # Santiago,
        r"SANTLAGO,",             # Santlago,
        r"FECHA\b",               # Fecha
        r"FECHA\s*EMISI[ÓO]N",        # Fecha Emisión o Fecha Emision
    ]
    # Regex para fechas:
    meses_full = r"(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)"
    meses_abbr = r"(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)"
    # Patrones de fecha
    ''' patrones_fecha = [
        #  DD/MM/YYYY ó DD-MM-YYYY ó DD.MM.YYYY
        r"(\d{1,2}[\/\-\.\s]\d{1,2}[\/\-\.\s]\d{2,4})",
        # dd de MES de|del yyyy
        rf"(\d{{1,2}}\s+DE\s+{meses_full}\s+DE(L)?\s+\d{{4}})",
        # DD-MES_FULL-YYYY
        rf"(\d{{1,2}}[-]\s*{meses_full}\s*[-]\s*\d{{2,4}})",
        #  DD MES YYYY
        rf"(\d{{1,2}}\s+{meses_full}\s+\d{{4}})",
        #  DD-MESABR-YYYY  (ej. 29-ABR-2022)
        rf"(\d{{1,2}}[-]\s*{meses_abbr}\s*[-]\s*\d{{2,4}})",
        #  DD MESABR YYYY (ej. 29 ABR 2022)
        rf"(\d{{1,2}}\s+{meses_abbr}\s+\d{{2,4}})"
    ]'''
    patrones_fecha = [
    r"(\d{1,2}[\/\-\.\s]\d{1,2}[\/\-\.\s]\d{4})",  # exigiendo 4 dígitos de año
    rf"(\d{{1,2}}\s+DE\s+{meses_full}\s+DE(L)?\s+\d{{4}})",
    rf"(\d{{1,2}}[-]\s*{meses_full}\s*[-]\s*\d{{2,4}})",
    rf"(\d{{1,2}}\s+{meses_full}\s+\d{{4}})",
    rf"(\d{{1,2}}[-]\s*{meses_abbr}\s*[-]\s*\d{{2,4}})",
    rf"(\d{{1,2}}\s+{meses_abbr}\s+\d{{2,4}})"
    ]   

    # Preparamos líneas
    lineas = [l.strip() for l in texto.split("\n")]

    if debug:
        print("=== DEBUG extraer_fecha_documento ===")
        print("Líneas del documento:")
        for i, l in enumerate(lineas, 1):
            print(f"{i:02d}: {l}")
        print("-" * 40)

    # Recorremos cada línea buscando una etiqueta
    for idx, linea in enumerate(lineas):
        linea_up = linea.upper()
        etiqueta_encontrada = False
        for et in etiquetas:
            if re.search(et, linea_up):
                etiqueta_encontrada = True
                if debug:
                    print(f"Etiqueta '{et}' encontrada en línea {idx+1}: {linea}")
                break
        if not etiqueta_encontrada:
            continue

        # Si la misma línea contiene fecha, extraemos
        for pf in patrones_fecha:
            m = re.search(pf, linea_up)
            if m:
                if debug:
                    print(f"Fecha encontrada en misma línea ({idx+1}): {m.group(1)}")
                    #sys.exit("Debug mode: fecha extraída.")
                return m.group(1).strip()

        # Si no, buscamos hasta 3 líneas siguientes
        for offset in range(1, 4):
            if idx + offset >= len(lineas):
                break
            siguiente = lineas[idx + offset].upper()
            if debug:
                print(f"Buscando fecha en línea {idx+1+offset}: {siguiente}")
            for pf in patrones_fecha:
                m = re.search(pf, siguiente)
                if m:
                    if debug:
                        print(f"Fecha encontrada en línea {idx+1+offset}: {m.group(1)}")
                        #sys.exit("Debug mode: fecha extraída.")
                    return m.group(1).strip()
        # Si encontramos etiqueta pero no fecha, continuar buscando otras etiquetas

    if debug:
        print("No se encontró fecha de emisión.")
        #sys.exit("Debug mode: terminando sin fecha.")
    return ""

def extraer_fecha_documento(texto, debug=False):
    etiquetas_permitidas = [
        r"FECHA\s*EMISIÓN?\.?", r"EMITIDO", r"SANTIAGO,", r"SANTLAGO,", r"FECHA\b", r"FECHA\s*EMISI[ÓO]N"
    ]
    etiquetas_excluidas = ["FECHA VENC", "VENCIMIENTO", "FECHA VCTO", "VCTO"]

    meses_full = r"(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)"
    meses_abbr = r"(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)"
    patrones_fecha = [
        r"(\d{1,2}[\/\-\.\s]\d{1,2}[\/\-\.\s]\d{4})",
        rf"(\d{{1,2}}\s+DE\s+{meses_full}\s+DE(L)?\s+\d{{4}})",
        rf"(\d{{1,2}}[-]\s*{meses_full}\s*[-]\s*\d{{2,4}})",
        rf"(\d{{1,2}}\s+{meses_full}\s+\d{{4}})",
        rf"(\d{{1,2}}[-]\s*{meses_abbr}\s*[-]\s*\d{{2,4}})",
        rf"(\d{{1,2}}\s+{meses_abbr}\s+\d{{2,4}})"
    ]

    lineas = [l.strip() for l in texto.split("\n")]

    # Paso 1: Buscar con etiquetas
    for idx, linea in enumerate(lineas):
        linea_up = linea.upper()

        if any(excl in linea_up for excl in etiquetas_excluidas):
            continue
        if not any(re.search(et, linea_up) for et in etiquetas_permitidas):
            continue

        for pf in patrones_fecha:
            m = re.search(pf, linea_up)
            if m:
                return m.group(1).strip()

        for offset in range(1, 4):
            if idx + offset >= len(lineas):
                break
            siguiente = lineas[idx + offset].upper()
            if any(excl in siguiente for excl in etiquetas_excluidas):
                continue
            for pf in patrones_fecha:
                m = re.search(pf, siguiente)
                if m:
                    return m.group(1).strip()

    # Paso 2: Buscar sin etiquetas si no se encontró nada
    for idx, linea in enumerate(lineas):
        linea_up = linea.upper()
        if any(excl in linea_up for excl in etiquetas_excluidas):
            continue
        for pf in patrones_fecha:
            m = re.search(pf, linea_up)
            if m:
                return m.group(1).strip()

    return ""

def extraer_nombre_proveedor(texto, debug=False):
    
    # Lista de formas jurídicas (normalizadas: sin puntos, en mayúsculas)
    formas_juridicas = ['SA', 'SPA', 'LTDA', 'EIRL', 'SOCIEDAD', 'LIMITADA', 'EMPRESA', 'INVERSIONES', 'BANCO']
    
    # Lista de exclusiones: si en la línea se encuentra alguno de estos términos, se descarta.
    exclusiones = ['SANTIAGO', 'ESPAÑA']

    # Función auxiliar: normaliza un token eliminando caracteres no alfanuméricos
    def normalizar_token(token):
        return re.sub(r'\W', '', token).upper()

    # Función auxiliar: determina si una línea contiene alguna forma jurídica válida.
    # Se revisan cada token individualmente y también se combinan tokens adyacentes para detectar casos como "S A".
    def linea_valida(linea):
        tokens = linea.split()
        tokens_norm = [normalizar_token(token) for token in tokens]
        # Si alguno de los tokens coincide con una palabra de exclusión, se descarta la línea.
        for token in tokens_norm:
            if token in exclusiones:
                return False
        # Revisar cada token individualmente para ver si es una forma jurídica.
        for token in tokens_norm:
            if token in formas_juridicas:
                return True
        # Revisar combinaciones de dos tokens consecutivos (por ejemplo, "S" + "A" = "SA").
        for i in range(len(tokens_norm) - 1):
            if tokens_norm[i] + tokens_norm[i+1] in formas_juridicas:
                return True
        return False

    # Convertir el texto a mayúsculas para la búsqueda (se conserva el original para el candidato)
    texto_mayus = texto.upper()
    if debug:
        print("Texto en mayúsculas:")
        print(texto_mayus)
        print("-" * 40)

    # Patrón para detectar el RUT (acepta separadores de miles opcionales) y opcionalmente "R.U.T." o "RUT:".
    patron_rut = r"(?<!\d)((?:\d{1,3}(?:\.\d{3}){1,2}|\d{7,8})-[\dkK])(?!\d)"
    match = re.search(patron_rut, texto_mayus)
    if not match:
        if debug:
            print("No se encontró ningún RUT en el texto.")
            #sys.exit("Debug mode: terminando script por falta de RUT.")
        return ""
    
    index_rut = match.start()
    if debug:
        print("Posición del primer RUT:", index_rut)
        print("-" * 40)

    # Dividir el texto en líneas (manteniendo el orden original)
    lineas = [linea.strip() for linea in texto.split('\n') if linea.strip()]
    if debug:
        print("Líneas totales encontradas:")
        for i, linea in enumerate(lineas, 1):
            print(f" Línea {i}: {linea}")
        print("-" * 40)

    # Determinar en qué línea se encuentra el primer RUT (usando el texto original)
    linea_rut = None
    acum = 0
    for idx, linea in enumerate(texto.split('\n')):
        acum += len(linea) + 1  # +1 para el salto de línea
        if acum > index_rut:
            linea_rut = idx
            break
    if debug:
        print("Índice de línea donde aparece el primer RUT:", linea_rut)
        print("-" * 40)

    candidato = ""
    # Estrategia 1: Buscar entre las líneas anteriores al RUT (en orden inverso)
    if linea_rut is not None and linea_rut > 0:
        for linea in reversed(lineas[:linea_rut]):
            if debug:
                print("Evaluando candidato previo:", linea)
            if linea_valida(linea):
                candidato = linea.strip()
                if debug:
                    print("Candidato válido encontrado en líneas previas:", candidato)
                break

    # Si no se encontró candidato en las líneas previas, buscar en las líneas posteriores al RUT.
    if not candidato and linea_rut is not None:
        for linea in lineas[linea_rut+1:]:
            if debug:
                print("Evaluando candidato posterior:", linea)
            if linea_valida(linea):
                candidato = linea.strip()
                if debug:
                    print("Candidato válido encontrado en líneas posteriores:", candidato)
                break

    # Estrategia 2: Si aún no se encontró candidato, recorrer todas las líneas y combinar sus tokens (sin espacios)
    # para detectar las formas jurídicas, y además descartar aquellas que contengan términos de exclusión.
    if not candidato:
        if debug:
            print("No se encontró candidato válido con el método anterior.")
            print("Aplicando segunda estrategia: combinación de tokens en cada línea.")
        for linea in lineas:
            # Combinar tokens sin espacios y normalizar para la comparación.
            combinado = re.sub(r'\W', '', "".join(linea.split())).upper()
            if any(excl in combinado for excl in exclusiones):
                if debug:
                    print(f"Se descarta la línea por contener término de exclusión: {linea}")
                continue
            for forma in formas_juridicas:
                forma_clean = forma.replace(".", "").upper()
                if forma_clean in combinado:
                    candidato = linea.strip()
                    if debug:
                        print("Candidato obtenido por combinación de tokens:", candidato)
                        #sys.exit("Debug mode: terminando script después de extraer el nombre del proveedor (segunda estrategia).")
                    break
            if candidato:
                break

    if debug:
        if candidato:
            print("Candidato final:", candidato)
        else:
            print("No se encontró candidato válido con forma jurídica.")
            # sys.exit("Debug mode: terminando script sin encontrar nombre del proveedor.")
    
    return candidato

def extraer_rut_proveedor(texto):
    
    # Este patrón permite dos variantes:
    # 1. 7 u 8 dígitos sin separadores: \d{7,8}
    # 2. Dígitos con puntos como separadores: Ejemplo "78.034.470" o "97.018.000"
    # En ambos casos, se espera un guion y el dígito verificador (número o K).
    patron = r"(?<!\d)((?:\d{1,3}(?:\.\d{3}){1,2}|\d{7,8})-[\dkK])(?!\d)"
    match = re.search(patron, texto.upper())
    if match:
        rut = match.group(1).strip()
        # Normalizar: eliminar puntos que puedan estar en el RUT.
        rut_normalizado = rut.replace(".", "")
        return rut_normalizado
    return ""

def extraer_nombre_comprador(texto, debug=False):

    # Patrón para detectar el número de documento.
    patron_num_doc = r"(?<!\w)(?:N[º°9]?\s*[:\-]?\s*|N\s*[Oo]\s*[:\-]?\s*|FOLIO\s*[:\-]?\s*|NO\s*[:\-]?\s*)(\d{5,})(?!\w)"
    #patron_num_doc = r"(?:N[º°]?\s*[:\-]?\s*|FOLIO\s*[:\-]?\s*|NO\s*[:\-]?\s*)(\d{5,})"
    
    # Dividir el texto en líneas (manteniendo el orden original)
    lineas = [linea.strip() for linea in texto.split('\n') if linea.strip()]
    if debug:
        print("Líneas totales:")
        for i, linea in enumerate(lineas, 1):
            print(f" Línea {i}: {linea}")
        print("-" * 40)
    
    # Localizar la posición (índice de línea) donde se encuentra el número de documento.
    num_doc_linea = None
    for idx, linea in enumerate(lineas):
        if re.search(patron_num_doc, linea.upper()):
            num_doc_linea = idx
            if debug:
                print(f"Número de documento encontrado en la línea {idx+1}: {linea}")
            break
    if num_doc_linea is None:
        if debug:
            print("No se encontró el número de documento en el texto.")
            #sys.exit("Debug mode: terminando script por falta de número de documento.")
        return ""
    
    # Definir la lista de etiquetas alternativas para el nombre del comprador.
    # Se usan expresiones regulares con grupos opcionales para abarcar variantes (por ejemplo, con o sin paréntesis).
    #etiquetas = [r'\bNOMBRE\b', r'\bSE[ÑN]OR(?:\s*\(ES\))?\b', r'\bCLIENTE\b', r'\bRAZ[ÓO]N\b']
    '''etiquetas = [
        r'\bNOMBRE\b',
        r'\bSE[ÑN]OR(?:\(ES\))?\b\s*[:\-–]?',   # Maneja SENOR y SENOR(ES) seguidos de : o -
        r'\bSE[ÑN]ORES\b',
        r'\bCLIENTE\b',
        r'\bRAZ[ÓO]N\b',
        r'\bSR\.?\b'  # Permite "Sr" o "Sr."
    ]
    '''
    etiquetas = [
    r'\bNOMBRE\b',
    r'\bSE[ÑN]OR(?:\(ES\))?\b\s*[:\-–]?',   # Maneja SENOR y SENOR(ES) seguidos de : o -
    r'\bCLIENTE\b',
    r'\bRAZ[ÓO]N\b',
    r'\bSR\.?\b'  # Permite "Sr" o "Sr."
]

    exclusiones = [
        "VENDEDOR", "VENDEDORA", "DIRECCION", "RUT", "FIRMA", "REPRESENTANTE", 
        "SUCURSAL", "GIRO", "TELEFONO", "CONTACTO", "CODIGO", "ORDEN DE COMPRA", "COMUNA", "CIUDAD", "NOTAI DE VENTA", "NOTA DE VENTA", "CONDICIONES","R.U.T"
    ]
    # Lista de patrones a eliminar antes de evaluar
    frases_excluir_inline = [
        r"[\(\[\{]\s*ES\s*[\)\]\}]",
        r"[\(\[\{]\s*AS\s*[\)\]\}]",
        r"[\(\[\{]\s*es\s*[\)\]\}]",
        r"[\(\[\{]\s*as\s*[\)\]\}]"
    ]

    # A partir de la línea siguiente al número de documento, buscar alguna de las etiquetas (máximo 20 líneas)
    for idx in range(num_doc_linea + 1, min(len(lineas), num_doc_linea + 21)):
        linea_actual = lineas[idx]
        match_etiqueta = next(
            (re.search(pat, linea_actual.upper()) for pat in etiquetas if re.search(pat, linea_actual.upper())),
            None
        )
        
        if match_etiqueta:
            if debug:
                print(f"Etiqueta encontrada en la línea {idx+1}: {linea_actual}")

            # 1. Buscar valor en la misma línea
            inicio_valor = match_etiqueta.end()
            fragmento = linea_actual[inicio_valor:]

            # Aplicar limpieza de frases excluidas antes de procesar
            for patron_excluir in frases_excluir_inline:
                fragmento = re.sub(patron_excluir, "", fragmento, flags=re.IGNORECASE)

            resto_linea = fragmento.lstrip(" :.-\t")
            posible_valor = resto_linea.strip()

            if posible_valor and \
                not any(pal in posible_valor.upper() for pal in exclusiones) and \
                len(re.findall(r"[A-Z0-9]", posible_valor.upper())) >= 3 and \
                re.search(r"[A-Z]", posible_valor.upper()):
                
                if debug:
                    print(f"Nombre del comprador detectado en la misma línea {idx+1}: {posible_valor}")
                return posible_valor
            
            # 2. Buscar en las siguientes 3 líneas
            for j in range(idx + 1, min(len(lineas), idx + 4)):
                posible_valor = lineas[j].strip()
                if posible_valor and \
                not any(pal in posible_valor.upper() for pal in exclusiones) and \
                len(re.findall(r"[A-Z0-9]", posible_valor.upper())) >= 3 and \
                re.search(r"[A-Z]", posible_valor.upper()):
                    if debug:
                        print(f"Nombre del comprador detectado en la línea {j+1}: {posible_valor}")
                    return posible_valor
    
    ''' # A partir de la línea siguiente al número de documento, buscar alguna de las etiquetas.
    for idx in range(num_doc_linea + 1, len(lineas)):
        linea_actual = lineas[idx]
        # Verificar si la línea contiene alguna de las etiquetas
        if any(re.search(pattern, linea_actual.upper()) for pattern in etiquetas):
            if debug:
                print(f"Etiqueta encontrada en la línea {idx+1}: {linea_actual}")
            # Buscar la siguiente línea no vacía que se asuma que contiene el nombre del comprador.
            for j in range(idx + 1, len(lineas)):
                if lineas[j]:
                    if debug:
                        print(f"Nombre del comprador detectado en la línea {j+1}: {lineas[j]}")
                    return lineas[j]
            break
    '''
    if debug:
        print("No se encontró ninguna de las etiquetas ('NOMBRE', 'SEÑOR(es)', 'CLIENTE') después del número de documento.")
        #sys.exit("Debug mode: terminando script por falta de etiqueta para nombre del comprador.")
    return ""

def extraer_rut_comprador(texto):
    
    # Este patrón permite dos variantes:
    # 1. 7 u 8 dígitos sin separadores: \d{7,8}
    # 2. Dígitos con puntos como separadores, por ejemplo "78.034.470" o "97.018.000"
    # En ambos casos, se espera un guion y un dígito o la letra K como dígito verificador.
    # Los lookbehind (?<!\d) y lookahead (?!\d) se usan para evitar capturar parte de números más grandes.
    patron = r"(?<!\d)((?:\d{1,3}(?:\.\d{3}){1,2}|\d{7,8})-[\dkK])(?!\d)"
    
    # Se obtienen todas las coincidencias del patrón en el texto (se convierte a mayúsculas para insensibilidad).
    matches = re.findall(patron, texto.upper())
    
    # Si se encuentran al menos dos coincidencias, se toma la segunda.
    if len(matches) >= 2:
        # Normalizar: eliminar puntos que puedan estar en el RUT.
        rut = matches[1].strip().replace(".", "")
        return rut
    
    # Si no se encuentran al menos dos coincidencias, se retorna cadena vacía.
    return ""

def cargar_diccionario_comunas(archivo):
    """
    Carga el diccionario de comunas desde el archivo CSV y devuelve un conjunto con los nombres de comunas en mayúsculas.
    Se asume que el CSV tiene una columna llamada "comuna".
    """
    try:
        df = pd.read_csv(archivo, encoding="utf-8-sig")
        # Convertir a mayúsculas y eliminar espacios adicionales
        comunas = set(df["comuna"].str.upper().str.strip())
        return comunas
    except Exception as e:
        print("Error cargando el diccionario de comunas:", e)
        # Retorna un conjunto vacío en caso de error
        return set()

def extraer_direccion_comprador(texto, debug=False):
    comunas = cargar_diccionario_comunas(ruta_comunas)

    etiquetas = [
        r"DIRECCI[ÓO]N",
        r"\bDIR\b",
        r"\bDIR\.",
        r"\bAV(?:ENIDA)?\b"
    ]

    patron_rut = r"(?<!\d)((?:\d{1,3}(?:\.\d{3}){1,2}|\d{7,8})-[\dkK])(?!\d)"
    match = re.search(patron_rut, texto.upper())
    if not match:
        if debug:
            print("No se encontró el RUT del proveedor.")
            #sys.exit("Debug mode: terminando por falta de RUT.")
        return ""

    index_rut = match.end()
    texto_posterior = texto[index_rut:]
    lineas = [linea.strip() for linea in texto_posterior.split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_direccion_comprador ===")
        print(f"Texto posterior al RUT (desde posición {index_rut}):")
        for i, l in enumerate(lineas, 1):
            print(f"{i:02d}: {l}")
        print("-" * 40)

    for idx, linea in enumerate(lineas):
        linea_upper = linea.upper()
        for etiqueta in etiquetas:
            if re.search(etiqueta, linea_upper):
                if debug:
                    print(f"Etiqueta '{etiqueta}' encontrada en línea {idx+1}: {linea}")

                match = re.search(rf"{etiqueta}\s*[:\-]?\s*(.+)", linea_upper)
                direccion_raw = ""
                if match:
                    direccion_raw = match.group(1).strip()
                elif idx + 1 < len(lineas):
                    direccion_raw = lineas[idx + 1].strip()

                # Limpiar caracteres especiales al inicio
                direccion = re.sub(r'^[^A-ZÁÉÍÓÚÑ0-9 ]+', '', direccion_raw, flags=re.IGNORECASE)

                # Cortar si contiene comuna y está precedida por número o símbolo
                if comunas:
                    for comuna in comunas:
                        comuna_upper = comuna.upper()
                        match_comuna = re.search(rf"([^\w\s]|[0-9])\s*{comuna_upper}", direccion.upper())
                        if match_comuna:
                            index_corte = direccion.upper().find(comuna_upper)
                            direccion = direccion[:index_corte].strip()
                            if debug:
                                print(f"Comuna '{comuna}' detectada y precedida por número o símbolo. Dirección recortada.")
                            break

                if debug:
                    print(f"Dirección final antes de truncar: {direccion}")

                direccion = direccion[:50].strip()

                if debug:
                    print(f"Dirección final truncada a 50 caracteres: {direccion}")
                    #sys.exit("Debug mode: dirección extraída.")
                return direccion

    if debug:
        print("No se encontró ninguna dirección posterior al RUT.")
        #sys.exit("Debug mode: sin dirección encontrada.")
    return ""

def extraer_telefono_comprador(texto, debug=False, solo_uno=True):
    # Buscar todos los RUTs (el segundo es el del comprador)
    patron_rut = r"(?<!\d)((?:\d{1,3}(?:\.\d{3}){1,2}|\d{7,8})-[\dkK])(?!\d)"
    matches = list(re.finditer(patron_rut, texto.upper()))
    if len(matches) < 2:
        if debug:
            print("No se encontró el segundo RUT (comprador).")
            sys.exit("Debug mode: terminando por falta de RUT del comprador.")
        return "" if solo_uno else []

    index_rut = matches[1].end()
    texto_posterior = texto[index_rut:]
    lineas = [linea.strip() for linea in texto_posterior.split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_telefono_comprador ===")
        print(f"Texto posterior al segundo RUT (posición {index_rut}):")
        for i, l in enumerate(lineas, 1):
            print(f"{i:02d}: {l}")
        print("-" * 40)

    telefonos = []
    patron_numero = re.compile(r"(\+?56)?[\s\-\.]?(?:\(?\d{1,3}\)?[\s\-\.]?)?\d{3}[\s\-\.]?\d{4}")

    for idx, linea in enumerate(lineas):
        if "TELEFONO" in linea.upper() or "TELÉFONO" in linea.upper():
            if debug:
                print(f"Etiqueta TELÉFONO encontrada en línea {idx+1}: {linea}")

            # Buscar número en la misma línea
            matches_en_linea = patron_numero.findall(linea)
            if not matches_en_linea and idx + 1 < len(lineas):
                matches_en_linea = patron_numero.findall(lineas[idx + 1])

            for match in re.finditer(patron_numero, linea + "\n" + (lineas[idx+1] if idx + 1 < len(lineas) else "")):
                numero = match.group(0)
                # Limpiar y normalizar
                numero_limpio = re.sub(r"[^\d+]", "", numero)
                if numero_limpio.startswith("56") and len(numero_limpio) >= 11:
                    normalizado = "+56" + numero_limpio[-9:]
                elif numero_limpio.startswith("+56") and len(numero_limpio) >= 12:
                    normalizado = "+56" + numero_limpio[-9:]
                else:
                    normalizado = numero_limpio[-9:] if len(numero_limpio) >= 8 else ""

                if normalizado:
                    telefonos.append(normalizado)
                    if debug:
                        print(f"Teléfono detectado y normalizado: {normalizado}")

            break  # Solo tomamos el primero que esté relacionado con etiqueta

    if debug:
        print("Teléfonos final extraídos:", telefonos)
        #sys.exit("Debug mode: finalizando extracción de teléfono.")

    return telefonos[0] if solo_uno and telefonos else "" if solo_uno else telefonos

def extraer_comuna_comprador(texto, comunas_permitidas=None):
    """
    Extrae la comuna del comprador a partir del texto, buscando la etiqueta "COMUNA"
    y retornando únicamente el valor si se encuentra en el listado de comunas permitidas.
    Si no se encuentra o no es válido, se retorna una cadena vacía.
    """
    if comunas_permitidas is None:
        comunas_permitidas = cargar_diccionario_comunas(ruta_comunas)

    # Patrón que busca "COMUNA" seguido de espacios, dos puntos o guiones y luego una secuencia de letras y espacios.
    patron = r"(?:COMUNA\s*[:\-]?\s*)([A-Z\s]+)"
    match = re.search(patron, texto.upper())
    if match:
        candidato = match.group(1).strip()
        # Se recorre el conjunto de comunas permitidas y se verifica si alguna está contenida en el candidato.
        for comuna in comunas_permitidas:
            if comuna in candidato:
                return comuna
        # En caso de no encontrar coincidencia exacta, se retorna cadena vacía.
        return ""
    return ""

def cargar_diccionario_ciudades(archivo):
    """
    Carga el diccionario de ciudades desde el archivo CSV y devuelve un conjunto de ciudades en mayúsculas.
    Se asume que el CSV tiene una columna llamada "ciudad".
    """
    try:
        df = pd.read_csv(archivo, encoding="utf-8-sig")
        # Convertir los valores a mayúsculas y eliminar espacios extra
        ciudades = set(df["ciudad"].str.upper().str.strip())
        return ciudades
    except Exception as e:
        print("Error cargando el diccionario de ciudades:", e)
        return set()

def extraer_ciudad_comprador(texto, ciudades_permitidas=None):
    """
    Extrae la ciudad del comprador a partir del texto, buscando la etiqueta "CIUDAD" y
    retornando únicamente el valor si se encuentra en el listado de ciudades permitidas.
    Si no se encuentra o no es válido, se retorna una cadena vacía.
    """
    # Si no se ha pasado el conjunto de ciudades permitidas, se carga el diccionario desde el archivo.
    if ciudades_permitidas is None:
        ciudades_permitidas = cargar_diccionario_ciudades()
    
    # Patrón que busca "CIUDAD" seguido de espacios, dos puntos o guiones y luego una secuencia de letras y espacios.
    patron = r"(?:CIUDAD\s*[:\-]?\s*)([A-Z\s]+)"
    match = re.search(patron, texto.upper())
    if match:
        candidato = match.group(1).strip()
        # Se recorre el conjunto de ciudades permitidas y se verifica si alguna está contenida en el candidato.
        for ciudad in ciudades_permitidas:
            if ciudad in candidato:
                return ciudad
        return ""
    return ""

def extraer_placa_patente_old(texto):
    
    # Capturar la etiqueta y la secuencia de letras y números, permitiendo separadores
    patron = r"(?:P(?:\.?P\.?U\.?)|PATENTE|PLACA)[\s:]*([A-Z]+[\W]*\d+)"
    match = re.search(patron, texto.upper())
    if match:
        # Extraer candidato y eliminar separadores (caracteres no alfanuméricos)
        candidato = match.group(1)
        candidato_alnum = re.sub(r'\W', '', candidato)
        # Patrones válidos para la placa
        patrones_validos = [
            r'^[A-Z]{2}\d{4}$',       # Ej: AA1234
            r'^[A-Z]{4}\d{2}$',       # Ej: ABCD12
            r'^[A-Z]{3}\d{2}$',       # Ej: ABC23
            r'^[A-Z]{3}0\d{2}$'       # Ej: ABC023
        ]
        for p in patrones_validos:
            if re.fullmatch(p, candidato_alnum):
                return candidato_alnum
    return ""

def extraer_tipo_vehiculo(texto, debug=False):
    tipos_vehiculo = [
        "GRUA HORQUILLA", "CAMIONETA", "AUTOMOVIL", "CAMIÓN", "TRACTOCAMIÓN", "SUV", "REMOLQUE", "BUS", "GRUA",
        "MOTOCICLETA", "MOTO", "VEHÍCULO ELÉCTRICO", "MAQUINA INDUSTRIAL", "CARRO DE ARRASTRE", "MAQUINA AGRICOLA","MAQUINARIAINDUSTRIAL",
        "BICICLETA MOTOR", "CHASIS CABINADO", "COCHE MORTUORIO", "MINIBUS PESADO", "TRICICLO MOTOR", "STATION WAGON",
        "CASA RODANTE", "SEMIREMOLQUE", "SEMIRREMOLQUE", "TRACTOCAMION", "TRACTO CAMION", "AMBULANCIA", "CARROBOMBA", "CUADRIMOTO", "CUATRIMOTO",
        "BICIMOTO", "LIMUSINA", "MICROBUS", "TROLEBUS", "MINIBUS", "OMNIBUS", "TRACTOR", "TRIMOTO", "CAMION", 
        "FURGON", "BUGGI", "JEEP"
    ]

    tipos_vehiculo = sorted(tipos_vehiculo, key=len, reverse=True)
    texto_upper = texto.upper()

    if debug:
        print("=== DEBUG extraer_tipo_vehiculo ===")
        print("Texto en mayúsculas:")
        print(texto_upper)
        print("-" * 40)

    # 1. Buscar con contexto explícito usando varios patrones
    for tipo in tipos_vehiculo:
        patrones = [
            rf"(TIPO(?:\s+DE)?\s+VEHICULO[\s:\-]*)\b{re.escape(tipo)}\b",                # TIPO DE VEHICULO : CAMIONETA
            rf"(TIPO(?:\s+DE)?\s+VEHICULO[\s:\-]*){re.escape(tipo)}(?=\b|[^A-Z])",      # TIPO DE VEHICULO: CAMIONETA2020
            rf"(TIPO[\s\-]*DE[\s\-]*)?VEHICULO[\s:\-]*{re.escape(tipo)}",               # TIPOVEHICULO:CAMIONETA
            rf"TIPOVEHICULO{re.escape(tipo)}"                                           # TIPOVEHICULOCAMIONETA
        ]
        for patron_etiquetado in patrones:
            match = re.search(patron_etiquetado, texto_upper)
            if match:
                if debug:
                    print(f"✅ Tipo preferido detectado con etiqueta: {match.group(0)}")
                return tipo

    # 2. Buscar como palabra completa (sin prefijo)
    for tipo in tipos_vehiculo:
        patron_simple = rf"\b{re.escape(tipo)}\b"
        match = re.search(patron_simple, texto_upper)
        if match:
            if debug:
                print(f"⚠️ Tipo detectado como palabra aislada: {match.group(0)}")
            return tipo

    if debug:
        print("❌ No se encontró ningún tipo de vehículo.")
    return ""

def cargar_diccionario_marcas(ruta_csv):
    marcas = []
    marcas_comunes = [
        "TOYOTA", "HYUNDAI", "FORD", "CHEVROLET", "NISSAN", "MITSUBISHI", "JEEP",
        "KIA", "PEUGEOT", "RENAULT", "FIAT", "VOLKSWAGEN", "BMW", "MERCEDES", 
        "HONDA", "MAZDA", "SSANGYONG", "CITROEN", "JAC", "DFSK", "SUBARU", "CHERY",
        "SUZUKI", "BYD", "VOLVO", "FOTON", "MAXUS", "GEELY", "CHANGAN", "JETOUR",
        "FAW", "IVECO", "SCANIA", "DAEWOO", "MAN", "ISUZU", "RAM"
    ]
    try:
        with open(ruta_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                nombre = row.get('marca', '').strip().upper()
                if nombre:
                    marcas.append(nombre)
        if not marcas:
            raise ValueError("Diccionario cargado pero sin datos válidos.")
    except Exception as e:
        print(f"[WARN] Error cargando CSV de marcas. {ruta_csv} Usando fallback: {e}")
        marcas = marcas_comunes

    return sorted(marcas, key=len, reverse=True)

def extraer_marca(texto, marcas_permitidas=None, debug=False):
    """
    Extrae la marca desde el texto OCR:
    - Etiqueta explícita como 'Marca: HINO'
    - Coincidencia directa en todo el texto
    - Similitud con marcas permitidas
    """
    if marcas_permitidas is None:
        marcas_permitidas = cargar_diccionario_marcas("../diccionarios/Diccionario_marcas.csv")

    """
    Recorta el texto hasta la palabra 'COLOR' o sus variantes (como 'COLOR:', 'Color:', etc.)
    y devuelve solo la parte anterior a esa palabra. Si no se encuentra, devuelve el texto completo.
    """
    texto_upper = texto.upper()
    match = re.search(r"\bCOLOR\b[:\s]*", texto_upper)
    if match:
        posicion = match.start()
        texto= texto[:posicion].strip()
    texto= texto.strip()

    texto_normalizado = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper()
    lineas = texto_normalizado.splitlines()

    # 1. Buscar por etiqueta: admite errores OCR y separadores variados
    for linea in lineas:
        match = re.search(r"M[\s\-:.]*A[\s\-:.]*R[\s\-:.]*C[\s\-:.]*A[\s\-:.]*[:\-.\s]*([A-Z0-9 ]{2,40}?)(?=\s*(?:MODELO|COLOR|AÑO|FABRICACIÓN|PBV|$))", linea.upper())
        if match:
            bruto = match.group(1).strip(" ,.:")
            candidato = " ".join(bruto.split()[:3])  # máximo 3 palabras
            if debug:
                print(f"🔍 Marca detectada con etiqueta: {candidato}")
            if candidato in marcas_permitidas:
                return candidato
            simil = difflib.get_close_matches(candidato, marcas_permitidas, n=1, cutoff=0.8)
            if simil:
                if debug:
                    print(f"🔁 Marca corregida por similitud: {simil[0]}")
                return simil[0]
            if debug:
                print("⚠️ Marca no válida ni corregible por similitud.")

    # 2. Coincidencia directa en el texto completo
    for marca in marcas_permitidas:
        if re.search(rf"\b{re.escape(marca)}\b", texto_normalizado):
            if debug:
                print(f"✅ Marca encontrada por coincidencia directa: {marca}")
            return marca

    # 3. Inferencia por similitud general (último recurso)
    palabras = set(re.findall(r"[A-Z]{3,}", texto_normalizado))
    bigramas = [" ".join(p) for p in zip(palabras, list(palabras)[1:] + [""])]
    candidatos = list(palabras) + bigramas
    similitudes = difflib.get_close_matches(" ".join(candidatos), marcas_permitidas, n=1, cutoff=0.8)
    if similitudes:
        if debug:
            print(f"🔁 Marca inferida por similitud general: {similitudes[0]}")
        return similitudes[0]

    if debug:
        print("❌ No se encontró ninguna marca válida.")
    return ""

def extraer_marca_old(texto, debug=False):
    # Lista básica de marcas comunes, puedes ampliarla
    marcas_comunes = [
        "TOYOTA", "HYUNDAI", "FORD", "CHEVROLET", "NISSAN", "MITSUBISHI", "JEEP",
        "KIA", "PEUGEOT", "RENAULT", "FIAT", "VOLKSWAGEN", "BMW", "MERCEDES", 
        "HONDA", "MAZDA", "SSANGYONG", "CITROEN", "JAC", "DFSK", "SUBARU", "CHERY",
        "SUZUKI", "BYD", "VOLVO", "FOTON", "MAXUS", "GEELY", "CHANGAN", "JETOUR",
        "FAW", "IVECO", "SCANIA", "DAEWOO", "MAN", "ISUZU", "RAM", "HINO"
    ]

    texto_upper = texto.upper()

    # 1. Buscar por etiqueta explícita
    patron = r"M[\s\-:.]*A[\s\-:.]*R[\s\-:.]*C[\s\-:.]*A[\s\-:.]*[:\-.\s]*([A-Z0-9 ]{2,40})"
    match = re.search(patron, texto_upper)
    if match:
        marca = match.group(1).strip()
        if debug:
            print(f"🔍 Marca detectada con etiqueta: {marca}")
        return marca

    # 2. Buscar marcas conocidas dentro del texto si no se encontró con etiqueta
    for marca in marcas_comunes:
        if re.search(rf"\b{re.escape(marca)}\b", texto_upper):
            if debug:
                print(f"⚠️ Marca detectada sin etiqueta: {marca}")
            return marca

    if debug:
        print("❌ No se detectó marca.")
    return ""

def extraer_placa_patente(texto):
    """
    Extrae la placa patente desde texto OCR, permitiendo variantes con puntos entre letras y números.
    """
    import re
    texto_upper = texto.upper()
    patron = r"(?:P(?:\.?P\.?U\.?)|PATENTE|PLACA)[\s:]*([A-Z]{2,4}\.?\d{2,4}(?:-\d{1,2})?)"
    match = re.search(patron, texto_upper)
    if match:
        candidato = match.group(1)
        candidato_alnum = re.sub(r'\W', '', candidato)  # Limpiar para validar
        # Patrones válidos
        patrones_validos = [
            r'^[A-Z]{2}\d{4}$',       # AA1234
            r'^[A-Z]{4}\d{2}$',       # ABCD12
            r'^[A-Z]{3}\d{2}$',       # ABC23
            r'^[A-Z]{3}0\d{2}$'       # ABC023
        ]
        for p in patrones_validos:
            if re.fullmatch(p, candidato_alnum):
                return candidato_alnum
        return candidato  # Si no calza limpio, devolver original (ej. RLSS.19-5)
    return ""

def extraer_modelo(texto, debug=False):
    """
    Extrae el modelo desde el texto OCR.
    - Busca etiqueta 'MODELO' seguida de caracteres.
    - Corta el resultado si encuentra etiquetas de término como CILINDRADA, COLOR, etc.
    """
    etiquetas_fin = [
        "CILINDRADA", "COLOR", "PESO", "TRACCION", "MOTOR", "NRO.",
        "CODIGO", "CIT", "VIN", "CARROCERIA", "COMBUSTIBLE", "AFECTO", "AÑO", "ANO", "FABRICACION", "PBV", "FABRICACION"
    ]

    texto_upper = texto.upper()

    # Buscar el patrón: 'MODELO' seguido de valor
    patron = r"(?:MODELO[\s:\-]*)([A-Z0-9\-./ ]{3,40})"
    match = re.search(patron, texto_upper)

    if match:
        valor_crudo = match.group(1).strip()

        # Buscar si alguna etiqueta de término aparece dentro del valor extraído
        for etiqueta in etiquetas_fin:
            pos = valor_crudo.find(etiqueta)
            if pos != -1:
                valor_crudo = valor_crudo[:pos].strip()
                break  # Cortar en la primera coincidencia

        if debug:
            print(f"[DEBUG] Modelo detectado: {valor_crudo}")
        return valor_crudo

    if debug:
        print("[DEBUG] No se encontró modelo.")
    return ""

def extraer_n_motor_old(texto, debug=False):
    """
    Extrae el número de motor desde el texto OCR.
    - Busca etiquetas como 'MOTOR', 'NRO MOTOR', 'MOTCR', etc.
    - Extrae valores de 7 a 20 caracteres, alfanuméricos.
    - Corta si detecta el inicio de otra etiqueta como CHASIS, COLOR, etc.
    """
    texto = texto.upper()
    lineas = texto.splitlines()
    etiquetas_motor = ["MOTOR", "NRO MOTOR", "NO MOTOR", "MOTCR", "MOT0R"]
    etiquetas_fin = [
        "COLOR", "CHASIS", "PESO", "TRANSMISION", "TRACCION", "CILINDRADA", "PATENTE", 
        "TIPO VEHICULO", "ANO", "MODELO", "LLAVES", "PUERTAS", "ASIENTOS"
    ]

    for i, linea in enumerate(lineas):
        if any(etiqueta in linea for etiqueta in etiquetas_motor):
            if debug:
                print(f"[DEBUG] Línea con etiqueta MOTOR detectada: {linea}")

            # Línea siguiente opcional
            siguiente = lineas[i + 1] if i + 1 < len(lineas) else ""
            contenido = (linea + " " + siguiente).strip()

            # Cortar si aparece otra etiqueta
            for etiqueta_fin in etiquetas_fin:
                idx = contenido.find(etiqueta_fin)
                if idx != -1:
                    contenido = contenido[:idx]
                    if debug:
                        print(f"[DEBUG] Cadena cortada por aparición de etiqueta '{etiqueta_fin}': {contenido}")
                    break

            # Buscar patrón alfanumérico
            posibles = re.findall(r"[A-Z0-9\-]{7,30}", contenido)
            for candidato in posibles:
                limpio = candidato.replace(" ", "").replace("-", "")
                if re.search(r"\d", limpio) and re.search(r"[A-Z]", limpio) and 7 <= len(limpio) <= 20:
                    if debug:
                        print(f"[DEBUG] Motor válido encontrado: {limpio}")
                    return limpio
                else:
                    if debug:
                        print(f"[DEBUG] Candidato descartado por validación: '{limpio}'")

    if debug:
        print("[DEBUG] No se encontró número de motor válido.")
    return ""

def extraer_n_motor(texto, debug=False):
    """
    Extrae un único número de motor desde texto OCR.
    Prioriza: etiqueta > coincidencia > heurística.
    Retorna solo el valor como string.
    """
    texto_upper = texto.upper()
    lineas = texto_upper.splitlines()
    etiquetas_motor = ["MOTOR", "NRO MOTOR", "NO MOTOR", "MOTCR", "MOT0R", "Nro. Motor"]

     # Excluir "MOTOR" de etiquetas de corte para evitar truncamiento prematuro
    etiquetas_fin_filtradas = [e for e in ETIQUETAS_FIN_CAMPOS if e != "MOTOR"]

    # --- 1. Búsqueda por etiqueta ---
    for i, linea in enumerate(lineas):
        if any(etiqueta in linea for etiqueta in etiquetas_motor):
            siguiente = lineas[i + 1] if i + 1 < len(lineas) else ""
            contenido = (linea + " " + siguiente).strip()
            for etiquetas_fin_filtradas in etiquetas_fin_filtradas:
                idx = contenido.find(etiquetas_fin_filtradas)
                if idx != -1:
                    contenido = contenido[:idx]
                    break
            posibles = re.findall(r"[A-Z0-9\-]{7,30}", contenido)
            for candidato in posibles:
                limpio = candidato.replace(" ", "").replace("-", "")
                if 7 <= len(limpio) <= 20 and re.search(r"\d", limpio):
                    if debug:
                        print(f"[Etiqueta] Detectado: {limpio}")
                    return limpio

    # --- 2. Coincidencia directa en todo el texto ---
    posibles = re.findall(r"\b[A-Z0-9]{7,20}\b", texto_upper)
    for p in posibles:
        if re.search(r"\d", p):
            if debug:
                print(f"[Coincidencia] Detectado: {p}")
            return p

    # --- 3. Heurística general ---
    candidatos = re.findall(r"[A-Z0-9\-]{7,30}", texto_upper)
    for c in candidatos:
        limpio = c.replace("-", "")
        if 7 <= len(limpio) <= 20 and re.search(r"\d", limpio):
            if debug:
                print(f"[Heurística] Detectado: {limpio}")
            return limpio

    if debug:
        print("[❌] No se encontró número de motor válido.")
    return ""

def es_chasis_valido(candidato):
    """
    Valida si el string es un número de chasis probable.
    Requisitos:
    - Largo entre 12 y 20 caracteres
    - Alfanumérico
    - Contiene al menos 3 letras y 3 números
    - No contiene palabras comunes no técnicas (lista negra)
    """
    candidato = candidato.replace("-", "").replace(" ", "").strip().upper()

    if not (12 <= len(candidato) <= 20):
        return False
    if not re.fullmatch(r"[A-Z0-9]+", candidato):
        return False

    letras = sum(1 for c in candidato if c.isalpha())
    numeros = sum(1 for c in candidato if c.isdigit())

    if letras < 3 or numeros < 3:
        return False

    # Palabras comunes no técnicas que pueden confundirse con chasis
    palabras_excluidas = {"SANTIAGO", "CARGA", "DIESEL", "GASOLINA", "COLOR", "CHILE", "MOTOR", "SERIE", "VALOR", "PLATAFORMA"}

    for palabra in palabras_excluidas:
        if palabra in candidato:
            return False

    return True

def extraer_n_chasis(texto, debug=False):
    texto_upper = texto.upper()

    # Preprocesar: insertar saltos de línea artificiales para separar bloques "pegados"
    texto_upper = re.sub(r"([A-Z]{3,15})\s*:", r"\n\1:", texto_upper)  # Ej: "CHASIS:" → "\nCHASIS:"
    texto_upper = texto_upper.replace("II", "\n").replace("//", "\n").replace("  ", "\n")

    lineas = texto_upper.splitlines()

    etiquetas_chasis = [
        "CHASIS", "CHASSIS", "CHASI", "CHASSI",
        "N CHASSIS", "NO. CHASIS", "N° CHASIS", "NUMERO CH", "NRO. CHASIS"
    ]
    etiquetas_fin = [
        "CILINDRADA", "COLOR", "PESO", "TRACCION", "MOTOR",
        "CODIGO", "CIT", "VIN", "CARROCERIA", "COMBUSTIBLE", "AFECTO", "AÑO"
    ]

    etiquetas_chasis_pat = "|".join(re.escape(e) for e in etiquetas_chasis)
    etiquetas_fin_pat = "|".join(re.escape(e) for e in etiquetas_fin)

    # Regex que corta el valor luego de una etiqueta de término si aparece
    patron = re.compile(
        rf"(?:{etiquetas_chasis_pat})[\s:\-]*([A-Z0-9\- ]{{7,30}}?)(?:\s+(?:{etiquetas_fin_pat})|\b)",
        re.IGNORECASE
    )

    match = patron.search(texto_upper)
    if match:
        valor = match.group(1).strip().replace(" ", "").replace("-", "")
        if es_chasis_valido(valor):
            if debug:
                print(f"[Etiqueta] Chasis detectado: {valor}")
            return valor

    # Coincidencia directa
    posibles = re.findall(r"\b[A-Z0-9]{12,20}\b", texto_upper)
    for p in posibles:
        if es_chasis_valido(p):
            if debug:
                print(f"[Coincidencia] Detectado: {p}")
            return p

    # Heurística general
    candidatos = re.findall(r"[A-Z0-9\-]{12,30}", texto_upper)
    for c in candidatos:
        if es_chasis_valido(c):
            if debug:
                print(f"[Heurística] Detectado: {c.strip()}")
            return c.strip()

    if debug:
        print("[❌] No se encontró número de chasis válido.")
    return ""

def extraer_n_chasis_old(texto, debug=False):
    """
    Extrae el número de chasis desde el texto OCR.
    - Soporta múltiples etiquetas como CHASIS, CHASSIS, NO. CHASIS, etc.
    - Valida valores alfanuméricos de 12 a 18 caracteres.
    - Si no encuentra candidatos válidos, usa la función de respaldo.
    """
    texto = texto.upper()
    lineas = texto.splitlines()
    etiquetas_posibles = ["CHASIS", "CHASSIS", "CHASI", "CHASSI", "N CHASSIS", "NO. CHASIS", "N° CHASIS"]
    candidatos = []

    for i, linea in enumerate(lineas):
        if any(etiqueta in linea for etiqueta in etiquetas_posibles):
            if debug:
                print(f"[DEBUG] Línea {i+1} con etiqueta encontrada: {linea}")
            # Revisa esta línea y la siguiente
            targets = [linea]
            if i + 1 < len(lineas):
                targets.append(lineas[i + 1])
            for target_line in targets:
                posibles = re.findall(r"[A-Z0-9\-]{8,25}", target_line)
                for p in posibles:
                    limpio = p.strip().replace(" ", "").replace(":", "")
                    if 8 <= len(limpio) <= 25:
                        candidatos.append(limpio)

    candidatos_filtrados = [c for c in candidatos if 12 <= len(c) <= 18]

    if debug:
        print(f"[DEBUG] Todos los candidatos encontrados: {candidatos}")
        print(f"[DEBUG] Candidatos filtrados (12–18 caracteres): {candidatos_filtrados}")

    if candidatos_filtrados:
        if debug:
            print(f"[DEBUG] ✅ Retornando mejor candidato: {candidatos_filtrados[0]}")
        return candidatos_filtrados[0]
    elif candidatos:
        if debug:
            print(f"[DEBUG] ⚠️ No hay candidatos en rango ideal, se retorna: {candidatos[0]}")
        return candidatos[0]

    if debug:
        print("[DEBUG] ❌ No se encontró chasis por etiquetas, usando función de respaldo...")
    return extraer_n_chasis_remate_old(texto, debug)

def extraer_n_chasis_remate_old(texto, debug=False):
    """
    Intenta extraer un chasis desde todo el texto con expresiones regulares generales.
    """
    patron = r"(?:CHASIS|CHASSIS|N[ÚU]?M(?:ERO)?\.?\s*CHASIS?)\s*[:\-]*\s*([A-Z0-9\- ]{8,25})"
    match = re.search(patron, texto.upper())
    if match:
        valor = match.group(1).strip().replace(" ", "").replace("-", "")
        if 12 <= len(valor) <= 18:
            if debug:
                print(f"[DEBUG] ✅ Remate detectado válido: {valor}")
            return valor
    if debug:
        print("[DEBUG] ❌ Remate no detectó valor válido.")
    return ""

def extraer_vin(texto):
    """
    Extrae el VIN (Vehicle Identification Number) desde el texto OCR.
    - Acepta etiquetas como VIN, V.I.N., VIN.
    - Soporta separador :, -, espacio, o ninguno.
    - Retorna el primer match válido (12-17 caracteres alfanuméricos).
    """
    texto = texto.upper()
    patron = r"(?:V\.?\s*I\.?\s*N\.?)\s*[:\-]?\s*([A-Z0-9]{12,17})"
    matches = re.findall(patron, texto)

    for match in matches:
        match = match.strip()
        if 12 <= len(match) <= 17:
            return match
    return ""

def extraer_serie(texto):
    patron = r"(?:SERIE[\s:\-]*)([A-Z0-9\-]{8,20})"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def cargar_diccionario_colores(archivo):
    """
    Carga el diccionario de colores desde el CSV y devuelve un conjunto de colores en mayúsculas.
    """
    try:
        df = pd.read_csv(archivo, encoding="utf-8-sig")
        # Se asume que la columna se llama "color"
        allowed = set(df["color"].str.upper().str.strip())
        return allowed
    except Exception as e:
        print("Error cargando el diccionario de colores:", e)
        # Fallback a una lista por defecto
        return {"ROJO", "AZUL", "VERDE", "NEGRO", "BLANCO", "AMARILLO", "MORADO", "GRIS", "NARANJA"}

def extraer_color(texto, allowed_colors=None):
    """
    Extrae el color o combinación de colores indicado en el texto a partir de la etiqueta "COLOR" o "COLOR EXTERIOR".
    Primero se extrae la cadena candidata, luego se separa en palabras y se intenta formar combinaciones
    (por ejemplo, de dos palabras) que estén en el diccionario. Se devuelve la(s) coincidencia(s)
    encontradas, en el orden en que aparecen.
    """
    if allowed_colors is None:
        allowed_colors = cargar_diccionario_colores()

    patron = r"(?:COLOR|COIOR|COLOR EXTERIOR)[\s:\-]*([A-ZÁÉÍÓÚÑ\s\/\.\,\-]+)"
    match = re.search(patron, texto.upper())
    if match:
        candidato = match.group(1).strip()
        # Limpiar caracteres como / , . y reemplazarlos por espacio
        limpio = re.sub(r"[^\wÁÉÍÓÚÑ\s]", " ", candidato)
        palabras = limpio.split()

        resultados = []
        i = 0
        while i < len(palabras):
            # Combinación de tres palabras
            if i < len(palabras) - 2:
                compuesto3 = f"{palabras[i]} {palabras[i+1]} {palabras[i+2]}"
                if compuesto3 in allowed_colors:
                    resultados.append(compuesto3)
                    i += 3
                    continue
            # Combinación de dos palabras
            if i < len(palabras) - 1:
                compuesto2 = f"{palabras[i]} {palabras[i+1]}"
                if compuesto2 in allowed_colors:
                    resultados.append(compuesto2)
                    i += 2
                    continue
            # Palabra individual
            if palabras[i] in allowed_colors:
                resultados.append(palabras[i])
            i += 1

        if resultados:
            return " ".join(resultados)
    return ""

def extraer_anio_old(texto):
    #patron = r"(?:AÑO COMERCIAL|ANO COMERCIAL|AÑO|ANO)[\s:\-]*([1-2][0-9]{3})"
    patron = r"(?:A[ÑN\?]O COMERCIAL|A[ÑN\?]O)[\s:\-]*([1-2][0-9]{3})"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_anio(texto, debug=False):
    """
    Extrae el año del vehículo desde el texto, evitando confundirlo con fechas completas.
    Si el año aparece inmediatamente después de 'AÑO' o 'AÑO COMERCIAL', lo acepta aunque
    también exista en fechas.
    """
    texto_upper = texto.upper()

    # 1. Buscar año con etiqueta
    patron = r"(?:A[ÑN\?]O COMERCIAL|A[ÑN\?]O)[\s:\-]*([1-2][0-9]{3})"
    candidatos = re.findall(patron, texto_upper)

    if debug:
        print(f"🎯 Candidatos detectados: {candidatos}")

    if not candidatos:
        return ""

    for anio in candidatos:
        anio = anio.strip()

        # Validar que es un año razonable
        if not (1980 <= int(anio) <= 2100):
            continue

        # Si viene directamente después de etiqueta, lo aceptamos
        # → no filtramos por fechas si vino por 'AÑO'
        if debug:
            print(f"✅ Año aceptado por etiqueta directa: {anio}")
        return anio

    if debug:
        print("❌ Ningún año válido encontrado.")
    return ""

def extraer_unidad_pbv_old(texto):
    # Se asume que se indica la unidad en el mismo campo que PBV
    patron = r"(KG|KGS|TON|KILOS)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_unidad_pbv(texto):
    """
    Extrae la unidad asociada al Peso Bruto Vehicular (PBV) evaluando líneas relevantes como
    'PBV', 'PESO BRUTO VEHICULAR', etc. Retorna KG, TON, etc. si se encuentra en el contexto adecuado.
    """
    texto = texto.upper()
    unidades_validas = {"KG", "TON", "KGS", "KILOS"}
    etiquetas_relevantes = [
        "PBV","P.B.V.", "PESO BRUTO", "PESO BRUTO VEHICULAR", "PESO VEHICULAR", "Peso Bruto Vehicular (Kgs.)","PBV (Kgs.)","P.B.V. (Kgs.)", "PESO BRUTO (Kgs.)"
    ]

    lineas = texto.splitlines()
    for i, linea in enumerate(lineas):
        linea_limpia = linea.strip()
        # Buscar etiquetas relacionadas al peso
        if any(etq in linea_limpia for etq in etiquetas_relevantes):
            # Revisar unidad en la misma línea
            match = re.search(r"\b(KG|TON|KGS|KILOS)\b", linea_limpia)
            if match:
                return match.group(1)
            # Revisar unidad en la línea siguiente
            if i + 1 < len(lineas):
                siguiente = lineas[i + 1]
                match = re.search(r"\b(KG|TON|KGS|KILOS)\b", siguiente)
                if match:
                    return match.group(1)

    # Si no encuentra unidad en contexto, buscar primera aparición genérica
    match = re.search(r"\b(KG|TON|KGS|KILOS)\b", texto)
    if match:
        return match.group(1)

    return ""

def extraer_pbv(texto, debug=False):
    """
    Extrae el valor más alto de Peso Bruto Vehicular (PBV) desde el texto OCR.
    Busca variantes como PBV, PESO BRUTO VEHICULAR, etc., y evalúa misma o siguiente línea.
    Retorna el valor mayor a 10 encontrado.
    """
    etiquetas = [
        r"P(?:\.?\s*)?B(?:\.?\s*)?V(?:\s*\(KGS\.?\))?",         # PBV, P.B.V., PBV (Kgs.)
        r"PESO\s+BRUTO\s+VEHICULAR(?:\s*\(KGS\.?\))?",          # PESO BRUTO VEHICULAR
        r"PESO\s+BRUTO(?:\s*\(KGS\.?\))?",                      # PESO BRUTO
        r"PESO\s+VEHICULAR",                                    # PESO VEHICULAR
    ]
    patron_valor = re.compile(r"([\d\.,]+)")
    texto_upper = texto.upper()
    lineas = texto_upper.splitlines()

    valores_encontrados = []

    for i, linea in enumerate(lineas):
        for patron_etq in etiquetas:
            if re.search(patron_etq, linea):
                if debug:
                    print(f"[Etiqueta] Detectada en línea {i+1}: {linea.strip()}")

                # 1. Buscar valor en la misma línea
                match_misma = patron_valor.search(linea)
                if match_misma:
                    try:
                        valor = float(match_misma.group(1).replace(".", "").replace(",", "."))
                        if valor > 10:
                            valores_encontrados.append(valor)
                            if debug:
                                print(f"  ➜ Valor en misma línea: {valor}")
                    except ValueError:
                        pass

                # 2. Buscar en la línea siguiente
                if i + 1 < len(lineas):
                    match_siguiente = patron_valor.search(lineas[i + 1])
                    if match_siguiente:
                        try:
                            valor = float(match_siguiente.group(1).replace(".", "").replace(",", "."))
                            if valor > 10:
                                valores_encontrados.append(valor)
                                if debug:
                                    print(f"  ➜ Valor en línea siguiente: {valor}")
                        except ValueError:
                            pass

    if valores_encontrados:
        max_valor = int(max(valores_encontrados))
        if debug:
            print(f"✅ Mayor valor detectado: {max_valor}")
        return str(max_valor)

    if debug:
        print("❌ No se encontró PBV válido mayor a 10.")
    return ""

def extraer_cit_old(texto):
    #patron = r"(?:CIT|INFORME TÉCNICO|CODIGO INFORME TÉCNICO)[\s:\-]*([A-Z0-9\-]{6,})"
    patron = (
    r"(?:CIT|"  # CIT
    r"I[NÑ]FORME\s+[TÉE]CNICO|"  # INFORME TÉCNICO o INFORME TECNICO
    r"C[ÓO]DIGO\s+I[NÑ]FORME\s+[TÉE]CNICO"  # CÓDIGO INFORME TÉCNICO o variantes
    r")[\s:\-]*([A-Z0-9\-]{6,})"
)
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def buscar_codigo_cit_por_marca(texto, marca_detectada, debug=False):
    """
    Busca un código CIT en el texto según el prefijo asociado a la marca especificada.
    El código esperado tiene un formato alfanumérico largo, empezando con el prefijo de marca.

    Parámetros:
    - texto: str - el contenido OCR del documento.
    - marca: str - el nombre de la marca del vehículo.
    - debug: bool - muestra información adicional si es True.

    Retorna:
    - str: el primer código CIT que coincida con el prefijo de la marca, o "" si no se encuentra.
    """

    # Lista simplificada de prefijos por marca (debes cargar la completa desde archivo o configuración externa)
    cit_prefixes = [
        {"marca": "ALFA ROMEO", "prefix": "AR"},
        {"marca": "ASTON MARTIN", "prefix": "AM"},
        {"marca": "AUDI", "prefix": "AD"},
        {"marca": "AUTORRAD", "prefix": "AT"},
        {"marca": "BAIC", "prefix": "BC"},
        {"marca": "BAW", "prefix": "BW"},
        {"marca": "BENTLEY", "prefix": "BT"},
        {"marca": "BMW", "prefix": "BM"},
        {"marca": "BORGWARD", "prefix": "BG"},
        {"marca": "BRILLIANCE", "prefix": "BL"},
        {"marca": "BRILLIANCE", "prefix": "BR"},
        {"marca": "BYD", "prefix": "BY"},
        {"marca": "CADILLAC", "prefix": "CD"},
        {"marca": "CITECAR", "prefix": "CC"},
        {"marca": "CITROEN", "prefix": "PG"},
        {"marca": "CITROEN", "prefix": "CT"},
        {"marca": "CUPRA", "prefix": "CP"},
        {"marca": "CHANGAN", "prefix": "CN"},
        {"marca": "CHANGHE", "prefix": "CN"},
        {"marca": "CHERY", "prefix": "CY"},
        {"marca": "CHEVROLET", "prefix": "CH"},
        {"marca": "CHRYSLER", "prefix": "CR"},
        {"marca": "DAIHATSU", "prefix": "DH"},
        {"marca": "DFLM", "prefix": "DF"},
        {"marca": "DFLZ", "prefix": "DL"},
        {"marca": "DFLZ", "prefix": "DF"},
        {"marca": "DFM", "prefix": "DF"},
        {"marca": "DFSK", "prefix": "DS"},
        {"marca": "DFSK", "prefix": "DF"},
        {"marca": "DODGE", "prefix": "DD"},
        {"marca": "DONGFENG", "prefix": "DF"},
        {"marca": "DS", "prefix": "DC"},
        {"marca": "EAGLE", "prefix": "EG"},
        {"marca": "EVEASY", "prefix": "EV"},
        {"marca": "EXEED BORN FOR MORE", "prefix": "EX"},
        {"marca": "FARIZON", "prefix": "FA"},
        {"marca": "FAW", "prefix": "BS"},
        {"marca": "FAW", "prefix": "FW"},
        {"marca": "FERRARI", "prefix": "FE"},
        {"marca": "FEST", "prefix": "FS"},
        {"marca": "FIAT", "prefix": "FT"},
        {"marca": "FORD", "prefix": "FR"},
        {"marca": "FOTON", "prefix": "FT"},
        {"marca": "FOTON", "prefix": "FN"},
        {"marca": "FUSO", "prefix": "FU"},
        {"marca": "GAC", "prefix": "GC"},
        {"marca": "GAC GONOW", "prefix": "GN"},
        {"marca": "GECKO", "prefix": "GK"},
        {"marca": "GEELY", "prefix": "GY"},
        {"marca": "GENESIS", "prefix": "GS"},
        {"marca": "GONOW", "prefix": "GN"},
        {"marca": "GREAT WALL", "prefix": "GW"},
        {"marca": "HAFEI", "prefix": "HF"},
        {"marca": "HAIMA", "prefix": "HI"},
        {"marca": "HAVAL", "prefix": "HV"},
        {"marca": "HINO", "prefix": "HO"},
        {"marca": "HONDA", "prefix": "HN"},
        {"marca": "HUANGHAI", "prefix": "HG"},
        {"marca": "HUMMER", "prefix": "HM"},
        {"marca": "HYUNDAI", "prefix": "HY"},
        {"marca": "INFINITI", "prefix": "NF"},
        {"marca": "IVECO", "prefix": "IV"},
        {"marca": "JAC", "prefix": "JC"},
        {"marca": "JAECOO", "prefix": "JA"},
        {"marca": "JAGUAR", "prefix": "JG"},
        {"marca": "JEEP", "prefix": "JP"},
        {"marca": "JETOUR", "prefix": "JT"},
        {"marca": "JIM", "prefix": "JI"},
        {"marca": "JINBEI", "prefix": "JB"},
        {"marca": "JMC", "prefix": "JC"},
        {"marca": "JMC", "prefix": "JM"},
        {"marca": "KAIYI", "prefix": "KA"},
        {"marca": "KARRY", "prefix": "KR"},
        {"marca": "KENBO", "prefix": "KB"},
        {"marca": "KIA", "prefix": "K"},
        {"marca": "KIA", "prefix": "KI"},
        {"marca": "KING LONG", "prefix": "KN"},
        {"marca": "KYC", "prefix": "KY"},
        {"marca": "LADA", "prefix": "LD"},
        {"marca": "LAMBORGHINI", "prefix": "LM"},
        {"marca": "LAND ROVER", "prefix": "LR"},
        {"marca": "LANDKING", "prefix": "LK"},
        {"marca": "LANDWIND", "prefix": "LW"},
        {"marca": "LEAPMOTOR", "prefix": "LP"},
        {"marca": "LEXUS", "prefix": "LX"},
        {"marca": "LIFAN", "prefix": "LF"},
        {"marca": "LIVAN", "prefix": "LV"},
        {"marca": "LOTUS", "prefix": "LT"},
        {"marca": "LYNK CO", "prefix": "LY"},
        {"marca": "M. BENZ", "prefix": "MB"},
        {"marca": "MAHINDRA", "prefix": "MH"},
        {"marca": "MAPLE", "prefix": "MP"},
        {"marca": "MASERATI", "prefix": "MS"},
        {"marca": "MAXUS", "prefix": "MX"},
        {"marca": "MAZDA", "prefix": "MZ"},
        {"marca": "MCLAREN", "prefix": "MC"},
        {"marca": "MG", "prefix": "MG"},
        {"marca": "MINI", "prefix": "MN"},
        {"marca": "MINI", "prefix": "BM"},
        {"marca": "MITSUBISHI", "prefix": "MT"},
        {"marca": "MITSUBISHI FUSO", "prefix": "FU"},
        {"marca": "NAMMI", "prefix": "NM"},
        {"marca": "NAMMI", "prefix": "MN"},
        {"marca": "NETA", "prefix": "NT"},
        {"marca": "NISSAN", "prefix": "NS"},
        {"marca": "OMODA", "prefix": "OM"},
        {"marca": "OPEL", "prefix": "OP"},
        {"marca": "PEUGEOT", "prefix": "PG"},
        {"marca": "PIAGGIO", "prefix": "GG"},
        {"marca": "PORSCHE", "prefix": "PO"},
        {"marca": "PROTON", "prefix": "PR"},
        {"marca": "R. SAMSUNG", "prefix": "RS"},
        {"marca": "RAM", "prefix": "RM"},
        {"marca": "RENAULT", "prefix": "RN"},
        {"marca": "RIDDARA", "prefix": "RD"},
        {"marca": "ROLLS-ROYCE", "prefix": "RR"},
        {"marca": "SAAB", "prefix": "SA"},
        {"marca": "SANGYONG", "prefix": "SS"},
        {"marca": "SEAT", "prefix": "ST"},
        {"marca": "SERES", "prefix": "SE"},
        {"marca": "SHINERAY", "prefix": "SH"},
        {"marca": "SKODA", "prefix": "SK"},
        {"marca": "SKYWELL", "prefix": "SY"},
        {"marca": "SMA MAPLE", "prefix": "SP"},
        {"marca": "SMART", "prefix": "SR"},
        {"marca": "SSANGYONG", "prefix": "SS"},
        {"marca": "SUBARU", "prefix": "SB"},
        {"marca": "SUZUKI", "prefix": "SZ"},
        {"marca": "SWM", "prefix": "SW"},
        {"marca": "TATA", "prefix": "TT"},
        {"marca": "TESLA", "prefix": "TS"},
        {"marca": "TOYOTA", "prefix": "TY"},
        {"marca": "UAZ", "prefix": "UZ"},
        {"marca": "VOLKSWAGEN", "prefix": "VW"},
        {"marca": "VOLVO", "prefix": "VL"},
        {"marca": "WELTMEISTER", "prefix": "WT"},
        {"marca": "WULING", "prefix": "WL"},
        {"marca": "ZNA", "prefix": "ZN"},
        {"marca": "ZNA DONGFENG", "prefix": "DF"},
        {"marca": "ZNA DONGFENG", "prefix": "ZN"},
        {"marca": "ZOTYE", "prefix": "ZT"},
        {"marca": "ZX", "prefix": "ZX"},
        {"marca": "ZX AUTO", "prefix": "ZX"},
    ]                           

    texto_upper = texto.upper()

    # Buscar los prefijos válidos para la marca dada
    prefijos_marca = [p["prefix"] for p in cit_prefixes if p["marca"] == marca_detectada.upper()]
    if not prefijos_marca:
        if debug:
            print(f"[DEBUG] No se encontró prefijo para la marca '{marca_detectada}'")
        return ""

    # Buscar todos los candidatos que parecen códigos CIT
    candidatos = re.findall(r"\b[A-Z0-9]{6,}\-[A-Z0-9]{1,3}\b", texto_upper)

    if debug:
        print(f"[DEBUG] Candidatos encontrados: {candidatos}")

    for c in candidatos:
        for prefijo in prefijos_marca:
            if c.startswith(prefijo):
                if debug:
                    print(f"[DEBUG] Coincidencia válida con prefijo '{prefijo}': {c}")
                return c

    if debug:
        print("[DEBUG] No se encontró un CIT con el prefijo esperado.")
    return ""

def extraer_cit(texto, marca_encontrada, debug=False):

    """
    Extrae el código CIT desde el texto OCR, tolerando variantes como:
    - 'INFORME TECNICO : XXX', 'CODIGO INFORME TECNICO', 'CIT : XXX', etc.
    - Retorna el primer valor alfanumérico largo (mínimo 6 caracteres con letras y números).
    """

    texto_upper = texto.upper()
    lineas = texto_upper.splitlines()

    # Patrones que pueden preceder el código CIT
    patrones_contexto = [
        r"C\.?\s*I\.?\s*T\.?", r"\bCIT\b", r"\bSIT\b", r"\bGIT\b", r"\bINFORME[\s\W]*TECNICO\b", r"\bHOMOLOGACION\b",
        r"\bCODIGO\s+INFORME\s+TECNICO\b", r"CODIGO\s+HOMOLOGACION\b"
    ]

    # Patrón para valores válidos (alfanuméricos, mínimo 6 caracteres, letras y números)
    patron_valor = re.compile(r"\b([A-Z0-9\-]{6,})\b")

    if debug:
        print("=== DEBUG extraer_cit ===")

    for i, linea in enumerate(lineas):
        for patron_etq in patrones_contexto:
            if re.search(patron_etq, linea):
                posibles = patron_valor.findall(linea)
                for candidato in posibles:
                    if re.search(r'[A-Z]', candidato) and re.search(r'\d', candidato):
                        if debug:
                            print(f"[DEBUG] Coincidencia en línea {i+1}: {linea}")
                            print(f"[DEBUG] CIT válido detectado: {candidato}")
                        return candidato

    # Si no se encontró ningún candidato, usa el método alternativo por marca
    if debug:
        print("[DEBUG] No se encontró un CIT alfanumérico válido. Buscando por prefijo de marca...")
    return buscar_codigo_cit_por_marca(texto_upper, marca_encontrada, debug=debug)

def extraer_combustible(texto, debug=False):
    # Usamos mayúsculas en opciones para que todo esté en el mismo caso
    opciones = (
        r"DUAL\s*\(\s*ELECTRICO\s*,\s*GASOLINA\s*\)|"
        r"DUAL\s*\(\s*ELECTRICO\s*,\s*DIESEL\s*\)|"
        r"GASOLINA|DIESEL|ELECTRICO|HIBRIDO|GAS LICUADO|GLP|"
        r"ELECTRICA|PETROLEO|PETROLERO|BENCINA"
    )

    texto_upper = texto.upper()
    lineas = texto_upper.splitlines()

    # Buscar etiqueta + valor en línea actual + línea siguiente
    for i in range(len(lineas)):
        combinada = lineas[i].strip()
        if i + 1 < len(lineas):
            combinada += " " + lineas[i + 1].strip()

        patron_etiqueta = rf"(TIPO\s+DE\s+COMBUSTIBLE|TIPO\s+COMBUSTIBLE|COMBUSTIBLE)[\s:\-]*({opciones})"
        match = re.search(patron_etiqueta, combinada)
        if match:
            valor = match.group(2).strip()
            if debug:
                print(f"[Etiqueta] Combustible detectado en líneas {i+1}-{i+2 if i+1 < len(lineas) else ''}: {valor}")
            return valor

    # Fallback: detectar solo por palabra clave
    for linea in lineas:
        for palabra in opciones.split("|"):
            if palabra in linea:
                if debug:
                    print("[Línea suelta] Combustible detectado en línea:", linea)
                return palabra

    if debug:
        print("No se encontró combustible.")
    return ""

def extraer_unidad_carga(texto):
    patron = r"(TONELADAS|KILOS|TON|KG)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_carga(texto):
    
    # Se busca cualquiera de las etiquetas: "CARGA" (con opción "UTIL") o "CAPACIDAD DE CARGA",
    # seguida de espacios, dos puntos o guiones, y luego una secuencia numérica que puede incluir puntos.
 #   patron = r"(?:CARGA\s*UTIL(?:\s*ADMISIBLE)?|CARGAUTILADMISIBLE|CARGAUTILVEHICULAR|C/CARGA|CAPACIDAD DE CARGA)[\s:\-]*([\d\.,]+)"
    patron = (
        r"(?:"
        r"CARGA[\s\-]*UTIL(?:[\s\-]*ADMISIBLE)?"
        r"|CARGA[\s\-]*UTIL[\s\-]*VEHICULAR"
        r"|CARGAUTILADMISIBLE"
        r"|CARGAUTILVEHICULAR"
        r"|C/CARGA"
        r"|CAPACIDAD[\s\-]*DE[\s\-]*CARGA"
        r")[\s:\-]*([\d\.,]+)"
        )

    match = re.search(patron, texto.upper())
    if match:
        valor = match.group(1).strip()
        # Elimina los separadores de miles (puntos)
        valor = valor.replace(".", "")
        valor = valor.replace(",", "")
        return str(valor)
    return ""

def extraer_asientos(texto):
    """
    Extrae el número de asientos desde el texto.
    Acepta formatos como 'ASIENTOS: 7' o '7 ASIENTOS'.
    El valor máximo aceptado es 60; si excede o no es válido, retorna vacío.
    """
    patrones = [
        r"ASIENTOS[\s:\-]*([0-9]+)",      # ASIENTOS: 7
        r"([0-9]+)[\s\-]*ASIENTOS",       # 7 ASIENTOS o 7Asientos
    ]
    texto_upper = texto.upper()
    for patron in patrones:
        match = re.search(patron, texto_upper)
        if match:
            valor = match.group(1).strip()
            if valor.isdigit() and int(valor) <= 60:
                return valor
    return ""

def extraer_puertas(texto, debug=False):
    """
    Extrae el número de puertas desde el texto si está precedido o seguido de la palabra PUERTAS.
    El valor máximo aceptado es 9. Si es mayor, retorna vacío.
    """
    texto_upper = texto.upper()
    patron = r"(?:PUERTAS[\s:\-\.]*(\d+)|(\d+)[\s:\-\.]*PUERTAS)"
    match = re.search(patron, texto_upper)
    if match:
        resultado = match.group(1) or match.group(2)
        if resultado.isdigit() and int(resultado) <= 9:
            if debug:
                print("✅ Coincidencia válida:", resultado)
            return resultado
        else:
            if debug:
                print(f"⚠️ Valor fuera de rango permitido (<= 9): {resultado}")
    else:
        if debug:
            print("❌ No se encontró coincidencia.")
    return ""

def extraer_potencia_motor(texto, debug=False):
    lineas = [linea.strip().upper() for linea in texto.splitlines() if linea.strip()]

    # Patrones posibles
    patrones = [
        r"POTENCIA(?:\s+MOTOR)?[\s:\-.\t]*([\d]{2,4})\s*(?:HP|CV)?",
        r"POT\.?\s*MÁXIMA[\s:\-.\t]*([\d]{2,4})\s*(?:HP|CV)?",
        r"([\d]{2,4})\s*(?:HP|CV)\b"  # búsqueda genérica
    ]

    for i, linea in enumerate(lineas):
        # Verificar línea actual y la siguiente
        secciones = [linea]
        if i + 1 < len(lineas):
            secciones.append(f"{linea} {lineas[i + 1]}")  # unir ambas

        for seccion in secciones:
            for patron in patrones:
                match = re.search(patron, seccion)
                if match:
                    valor = match.group(1).strip()
                    if debug:
                        print(f"[POTENCIA] Detectado: {valor} en línea {i+1}")
                    return valor

    if debug:
        print("[❌] No se encontró valor de potencia.")
    return ""

def extraer_unidad_potencia(texto):
    potencia = extraer_potencia_motor(texto)
    if not potencia:  # Si está vacío
        return ""
    
    patron = r"(KCAL/S|BTU/S|CV|HP|KP|KW)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_ejes(texto):
        # Conjunto de valores permitidos para la disposición de ejes.
    allowed = {
        "S4-S4", "S4-D8", "S4", "S2-D8", "D8", "T12", "S2-S4", "C16", 
        "S2-S2", "D4-D8", "S2-T12", "D4-T12", "T10", "S2-D6", "E48", "D4", 
        "S2", "C32", "D4-D6", "S2-D4", "D8-D8", "T6", "S4-T12", "D8-T12", 
        "0-D16", "S4-C16", "E24", "S4-D10","T-12"
    }

    # Patrón que busca "DISPOSICION DE LOS EJES" o "EJES", permitiendo que la información esté
    # entre paréntesis, llaves o corchetes, y capture uno o dos grupos de caracteres alfanuméricos.
    #patron = r"(?:DISPOSICION DE LOS EJES|EJES)[\s:\-]*[({\[]?\s*([A-Z0-9]+)(?:\s*[\W_]+\s*([A-Z0-9]+))?\s*[)}\]]?"
    patron = r"(?:(?:DISP(?:OSICION)?(?:\s+DE)?\s+EJES)|EJES)[\s:\-]*[({\[]?\s*([A-Z0-9]+)(?:\s*\W+\s*([A-Z0-9]+))?\s*[)}\]]?"
    matches = re.findall(patron, texto.upper())
    candidates = []
    for m in matches:
        grupo1 = m[0].strip() if m[0] else ""
        grupo2 = m[1].strip() if m[1] else ""
        if grupo1:
            candidate = grupo1 + ("-" + grupo2 if grupo2 else "")
            candidates.append(candidate)
    
    if not candidates:
        return ""
    
    # Primero, si alguna de las ocurrencias coincide exactamente con los permitidos, la retorna.
    for cand in candidates:
        if cand in allowed:
            return cand
    
    # Si no hay coincidencias exactas, se evalúa cuál candidato se asemeja más a alguno permitido.
    best_candidate = ""
    best_score = 0
    for cand in candidates:
        for a in allowed:
            score = SequenceMatcher(None, cand, a).ratio()
            if score > best_score:
                best_score = score
                best_candidate = a  # Se retorna el valor permitido que mayor similitud tiene
    # Se establece un umbral (por ejemplo, 0.6) para aceptar el match.
    if best_score >= 0.6:
        return best_candidate
    return ""

def extraer_traccion(texto):
    """
    Extrae la tracción del texto, tolerando errores comunes de OCR como TRACCI6N o TRACClON.
    """
    texto = texto.upper()
    
    # Patrón extendido con variantes OCR: TRACCI[ÓO0Q6CL]
    patron = r"TRACCI[ÓO06CLN]{1,2}[\s:\-]*((?:10X4|4X(?:2|4)|6X(?:2|4|6)|8X(?:2|4|6|8)))"

    match = re.search(patron, texto)
    return match.group(1).strip() if match else ""

def cargar_diccionario_carrocerias(archivo):
    """
    Carga el diccionario de carrocerías desde el CSV y devuelve un conjunto de valores en mayúsculas.
    Se asume que el CSV tiene una columna llamada "carroceria".
    """
    try:
        df = pd.read_csv(archivo, encoding="utf-8-sig")
        # Se asume que la columna se llama "carroceria"; convertir a mayúsculas y limpiar espacios
        allowed = set(df["carroceria"].str.upper().str.strip())
        return allowed
    except Exception as e:
        print("Error al cargar el diccionario de carrocerías:", e)
        return set()
    
def extraer_tipo_carroceria(texto, allowed_carrocerias=None, debug=False):
    """
    Extrae el tipo de carrocería desde el texto OCR. Se apoya en un diccionario de carrocerías permitidas
    y utiliza búsqueda exacta, por línea, y aproximada en caso de errores ortográficos.
    """

    if allowed_carrocerias is None:
        allowed_carrocerias = cargar_diccionario_carrocerias()

    texto_upper = texto.upper()
    lineas = texto_upper.splitlines()

    # Paso 1: Buscar etiqueta explícita
    patron = r"(?:TIPO\s+CARROCERIA|CARROCERIA)[\s:\-]*([A-Z\s\(\)]{4,50})"
    match = re.search(patron, texto_upper)
    if match:
        candidato = match.group(1).strip()
        posibles = [c for c in allowed_carrocerias if c in candidato]
        if posibles:
            resultado = max(posibles, key=len)
            if debug:
                print(f"✔ Etiqueta encontrada. Candidato: '{candidato}' → Coincidencia: '{resultado}'")
            return resultado

    # Paso 2: Buscar línea que contenga alguna carrocería directamente
    for linea in lineas:
        for carroceria in allowed_carrocerias:
            if carroceria in linea:
                if debug:
                    print(f"✔ Coincidencia directa en línea: '{linea.strip()}' → '{carroceria}'")
                return carroceria

    # Paso 3: Coincidencia aproximada con difflib
    palabras = re.findall(r"[A-ZÁÉÍÓÚÑ]{4,}", texto_upper)
    joined_text = " ".join(palabras)
    aproximadas = difflib.get_close_matches(joined_text, allowed_carrocerias, n=1, cutoff=0.75)
    if aproximadas:
        if debug:
            print(f"🤖 Coincidencia aproximada por similitud: '{aproximadas[0]}'")
        return aproximadas[0]

    if debug:
        print("⚠️ No se detectó tipo de carrocería.")
    return ""

def extraer_cilindrada(texto, debug=False):
    texto_upper = texto.upper()

    # 1. Buscar con "CC"
    patron_cc = r"CILINDRADA[\s:\-]*([\d\.]+\s*CC)"
    match = re.search(patron_cc, texto_upper)
    if match:
        if debug:
            print(f"[CC] CILINDRADA detectada: {match.group(1).strip()}")
        return match.group(1).strip()

    # 2. Buscar solo número si no hay "CC"
    patron_numero = r"CILINDRADA[\s:\-]*([\d\.]+)"
    match = re.search(patron_numero, texto_upper)
    if match:
        if debug:
            print(f"[SIN CC] CILINDRADA detectada: {match.group(1).strip()}")
        return match.group(1).strip()

    if debug:
        print("❌ No se encontró cilindrada.")
    return ""
    patron = r"CILINDRADA[\s:\-]*([\d\.]+\s*CC)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_transmision(texto, debug=False):
    """
    Extrae el tipo de transmisión del texto OCR.
    1. Busca etiquetas explícitas como 'TRANSMISIÓN'.
    2. Detecta términos comunes como MANUAL, AUTOMÁTICA, M/T, A/T.
    3. Evalúa todas las coincidencias posibles y prioriza MANUAL si hay ambigüedad.
    """
    texto_upper = texto.upper()
    etiquetas = [
        r"TRANSMISI[ÓOÕÒ´`']N", r"TRANSMICION", r"TRANSMISON", r"TRANSMICIÓN", r"TRANSMSION"
    ]
    patron_tipo = r"(MANUAL|AUTOMATICA|MECANICA|MECANIKO|AUTOMATIKO|MAN|AUTOM|M/T|A/T)"

    # 1. Buscar con etiqueta explícita
    for etiqueta in etiquetas:
        patron = rf"(?:{etiqueta})[\s:\-]*({patron_tipo})"
        match = re.search(patron, texto_upper)
        if match:
            valor = match.group(1).strip()
            normal = normalizar_transmision(valor)
            if debug:
                print(f"✔ Etiqueta: {match.group(1)} → {normal}")
            return normal

    # 2. Buscar todas las coincidencias posibles
    matches = re.findall(patron_tipo, texto_upper)
    normalizados = [normalizar_transmision(m.strip()) for m in matches if normalizar_transmision(m.strip())]

    if debug:
        print(f"🔎 Coincidencias encontradas: {normalizados}")

    if "MANUAL" in normalizados:
        return "MANUAL"
    if "AUTOMATICA" in normalizados:
        return "AUTOMATICA"

    # 3. Intento por similitud general (fallback)
    posibles = re.findall(r"[A-Z]{5,}", texto_upper)
    matches = difflib.get_close_matches(" ".join(posibles), ["MANUAL", "AUTOMATICA"], cutoff=0.6)
    if matches:
        if debug:
            print(f"🤖 Inferida por similitud: {matches[0]}")
        return matches[0]

    if debug:
        print("⚠️ No se detectó transmisión.")
    return ""

def normalizar_transmision(valor):
    valor = valor.upper()
    sinonimos = {
        "MECANICA": "MANUAL",
        "MECANIKO": "MANUAL",
        "MECANICO": "MANUAL",
        "MAN": "MANUAL",
        "M/T": "MANUAL",
        "AUTOM": "AUTOMATICA",
        "AUTOMATICO": "AUTOMATICA",
        "AUTOMATIKO": "AUTOMATICA",
        "A/T": "AUTOMATICA"
    }
    return sinonimos.get(valor, valor if valor in ["MANUAL", "AUTOMATICA"] else "")

def extraer_monto_neto(texto, debug=False):
    """
    Extrae el monto neto desde el texto. Acepta variantes como:
    - MONTO NETO
    - TOTAL NETO
    - SUB-TOTAL
    El valor puede estar hasta 3 líneas después de la etiqueta.
    Si existe más de una coincidencia, prioriza las etiquetas que contienen "NETO".
    Retorna un valor numérico si es mayor a 100, de lo contrario retorna "".
    """
    etiquetas_prioritarias = [
        r"TOTAL[\s\-\.]*NETO",
        r"MONTO[\s\-\.]*NETO",
        r"\bNETO\b",
        r"\b0NETO\b",
        r"\bONETO\b",
    ]

    etiquetas_secundarias = [
        r"SUB[\s\-\.]?TOTAL",
        r"\bAFECTO\b",
        r"\bALECTO\b"
    ]

    patron_monto = r"([\d\.,]{3,})"  # acepta puntos y comas
    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_monto_neto ===")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

def extraer_monto_neto(texto, debug=False):
    """
    Extrae el monto neto desde el texto. Acepta variantes como:
    - MONTO NETO
    - TOTAL NETO
    - SUB-TOTAL
    El valor puede estar hasta 3 líneas después de la etiqueta.
    Si existe más de una coincidencia, prioriza las etiquetas que contienen "NETO".
    Retorna un valor numérico si es mayor a 100, de lo contrario retorna "".
    """

    # Etiquetas prioritarias que contienen explícitamente la palabra "NETO"
    etiquetas_prioritarias = [
        r"TOTAL[\s\-\.]*NETO",
        r"MONTO[\s\-\.]*NETO",
        r"\bNETO\b",
        r"\b0NETO\b",  # OCR puede confundir O con 0
        r"\bONETO\b"   # OCR puede distorsionar el inicio de "NETO"
    ]

    # Etiquetas secundarias que podrían referirse al monto neto
    etiquetas_secundarias = [
        r"SUB[\s\-\.]?TOTAL",
        r"\bAFECTO\b",
        r"\bALECTO\b"  # ALECTO: posible error OCR de "AFECTO"
    ]

    # Patrón que extrae montos que contengan al menos 3 caracteres y usen punto o coma como separador
    patron_monto = r"([\d\.,]{3,})"

    # Preprocesamiento del texto: dividirlo por líneas, eliminar espacios vacíos, y convertir a mayúsculas
    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_monto_neto ===")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

    # Función auxiliar que busca montos válidos a partir de un conjunto de etiquetas
    def buscar_monto(etiquetas):
        for i, linea in enumerate(lineas):
            for etiqueta in etiquetas:
                match_etiqueta = re.search(etiqueta, linea)
                if match_etiqueta:
                    if debug:
                        print(f"Etiqueta '{etiqueta}' encontrada en línea {i+1}: {linea}")

                    # Intentar extraer monto desde la misma línea
                    match = re.search(patron_monto, linea)
                    if match:
                        valor = match.group(1).replace('.', '').replace(',', '')
                        if valor.isdigit() and int(valor) > 100:
                            if debug:
                                print(f"Monto válido encontrado en misma línea: {valor}")
                            return int(valor)

                    # Si no se encontró en la misma línea, buscar en las 3 siguientes
                    for j in range(1, 4):
                        if i + j >= len(lineas):
                            break
                        siguiente = lineas[i + j]
                        match = re.search(patron_monto, siguiente)
                        if match:
                            valor = match.group(1).replace('.', '').replace(',', '')
                            if valor.isdigit() and int(valor) > 100:
                                if debug:
                                    print(f"Monto válido encontrado en línea {i+1+j}: {valor}")
                                return int(valor)
        return None

    # 1. Intentar primero con las etiquetas prioritarias
    resultado = buscar_monto(etiquetas_prioritarias)
    if resultado is not None:
        return resultado

    # 2. Si no se encontró nada, intentar con etiquetas secundarias
    resultado = buscar_monto(etiquetas_secundarias)
    if resultado is not None:
        return resultado

    # 3. Si nada fue encontrado, retornar vacío
    if debug:
        print("No se encontró monto neto válido.")
    return ""

def extraer_monto_iva(texto, debug=False):
    """
    Extrae el monto de IVA desde el texto, considerando diversas variantes ortográficas y etiquetas.
    Se asegura de cortar correctamente después de la etiqueta incluso si contiene "%".
    Descarta montos como '1900' que son probablemente tasas de porcentaje mal interpretadas.
    """
    etiquetas = [
        r"MONTO\s+[\.]?V\.?A\.?\s*%?",
        r"MONTO\s+I\.?V\.?A\.?\s*%?",
        r"MONTO\s+L\.?V\.?A\.?\s*%?",
        r"I\.?V\.?A\.?\s*%?",
        r"IVA\s*[:\-]?\s*19(?:[.,]0{1,2})?\s*%?",
        r"IVA\s*:?[\s\-]?",
        r"19\s*%\s*IVA",
        r"19\s*%\s*I\.?V\.?A\.?\s*\$?",
        r"19%1\.V\.A\.",
        r"\b19%1\.V\.A\.\b",
        r"\bTVA\s*19\s*%",
        r"\bVA19%",
        r"\b19%VA\b",
        r"VA19%",
        r"1\.V\.A\.19%",
    ]

    patron_monto = r"([\d\.,]{4,})"
    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_monto_iva ===")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

    for i, linea in enumerate(lineas):
        for etiqueta in etiquetas:
            match_etiqueta = re.search(etiqueta, linea)
            if match_etiqueta:
                if debug:
                    print(f"Etiqueta encontrada en línea {i+1}: {linea}")
                    print(f"Usando etiqueta: {match_etiqueta.group(0)}")

                linea_post = linea[match_etiqueta.end():]
                match = re.search(patron_monto, linea_post)
                if match:
                    raw = match.group(1)
                    valor = re.sub(r"[^\d]", "", raw)
                    if valor.isdigit():
                        numero = int(valor)
                        if debug:
                            print(f"Monto en misma línea (crudo): {raw} → limpio: {numero}")
                        if numero >= 1000 and numero != 1900:
                            return numero

                for j in range(1, 4):
                    if i + j >= len(lineas):
                        break
                    siguiente = lineas[i + j]
                    match = re.search(patron_monto, siguiente)
                    if match:
                        raw = match.group(1)
                        valor = re.sub(r"[^\d]", "", raw)
                        if valor.isdigit():
                            numero = int(valor)
                            if debug:
                                print(f"Monto en línea {i+1+j} (crudo): {raw} → limpio: {numero}")
                            if numero >= 1000 and numero != 1900:
                                return numero

    if debug:
        print("❌ No se encontró monto IVA válido.")
    return ""

def extraer_monto_total(texto, debug=False):
    from unidecode import unidecode

    def texto_a_numero(texto):
        texto = unidecode(texto.upper())
        texto = texto.split("SON:")[-1]
        texto = re.sub(r'[^A-Z\s]', '', texto)
        palabras = texto.strip().split()

        numeros = {
            "UNO": 1, "DOS": 2, "TRES": 3, "CUATRO": 4, "CINCO": 5,
            "SEIS": 6, "SIETE": 7, "OCHO": 8, "NUEVE": 9, "DIEZ": 10,
            "ONCE": 11, "DOCE": 12, "TRECE": 13, "CATORCE": 14, "QUINCE": 15,
            "DIECISEIS": 16, "DIECISIETE": 17, "DIECIOCHO": 18, "DIECINUEVE": 19,
            "VEINTE": 20, "TREINTA": 30, "CUARENTA": 40, "CINCUENTA": 50,
            "SESENTA": 60, "SETENTA": 70, "OCHENTA": 80, "NOVENTA": 90,
            "CIEN": 100, "CIENTO": 100, "DOSCIENTOS": 200, "TRESCIENTOS": 300,
            "CUATROCIENTOS": 400, "QUINIENTOS": 500, "SEISCIENTOS": 600,
            "SETECIENTOS": 700, "OCHOCIENTOS": 800, "NOVECIENTOS": 900,
            "MIL": 1000, "MILLON": 1000000, "MILLONES": 1000000
        }

        total, parcial = 0, 0
        for palabra in palabras:
            if palabra == "Y":
                continue
            if palabra in numeros:
                valor = numeros[palabra]
                if valor == 1000:
                    if parcial == 0:
                        parcial = 1
                    total += parcial * valor
                    parcial = 0
                elif valor == 1000000:
                    if parcial == 0:
                        parcial = 1
                    total += parcial * valor
                    parcial = 0
                else:
                    parcial += valor

        total += parcial
        return total if total > 0 else None

    monto_iva = extraer_monto_iva(texto) or 0
    monto_neto = extraer_monto_neto(texto) or 0
    suma_esperada = monto_iva + monto_neto

    etiquetas = [
        r"TOTAL\s+BRUTO",
        r"TOTAL\s+FACTURA",
        r"MONTO\s+TOTAL",
        r"\bTOTAL\b"
    ]
    patron_monto = r"(\d{1,3}(?:\.\d{3})+)"
    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_monto_total ===")
        print(f"IVA detectado: {monto_iva}")
        print(f"Neto detectado: {monto_neto}")
        print(f"Suma esperada (IVA + Neto): {suma_esperada}")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

    ult_idx = None
    ult_etiqueta = ""
    for i, linea in enumerate(lineas):
        for etiqueta in etiquetas:
            if re.search(etiqueta, linea):
                ult_idx = i
                ult_etiqueta = etiqueta

    if ult_idx is not None and debug:
        print(f"Última etiqueta '{ult_etiqueta}' encontrada en línea {ult_idx+1}: {lineas[ult_idx]}")

    posibles = []
    rangos = list(range(0, 4)) + list(range(-1, -4, -1))
    if ult_idx is not None:
        for offset in rangos:
            idx = ult_idx + offset
            if 0 <= idx < len(lineas):
                linea_eval = lineas[idx]
                if debug:
                    print(f"Evaluando línea {idx+1}: {linea_eval}")
                match = re.search(patron_monto, linea_eval)
                if match:
                    val = match.group(1).replace('.', '')
                    if val.isdigit():
                        val = int(val)
                        if val > max(monto_iva, monto_neto) and val not in [monto_iva, monto_neto]:
                            posibles.append(val)
        if posibles:
            total = max(posibles)
            if debug:
                print(f"Monto total válido encontrado: {total}")
            return total

        for offset in rangos:
            idx = ult_idx + offset
            if 0 <= idx < len(lineas):
                match = re.search(patron_monto, lineas[idx])
                if match:
                    val = int(match.group(1).replace('.', ''))
                    if abs(val - suma_esperada) <= 100:
                        if debug:
                            print(f"Monto cercano a suma esperada encontrado: {val}")
                        return val

    for idx, linea in enumerate(lineas):
        if "SON" in linea:
            if debug:
                print(f"Etiqueta 'SON' detectada en línea {idx+1}: {linea}")
            texto_son = linea
            for j in range(1, 3):
                if idx + j < len(lineas):
                    texto_son += " " + lineas[idx + j]
            numero = texto_a_numero(texto_son)
            if numero and numero > max(monto_neto, monto_iva):
                if debug:
                    print(f"Valor convertido desde texto: {numero}")
                return numero

    if debug:
        print("No se encontró monto total válido.")
    return ""

def extraer_num_contrato(texto, debug=False):
    """
    Extrae número de contrato u operación desde texto OCR, tolerando errores comunes y etiquetas pegadas.

    ▸ Ejemplos reconocidos:
        - CONTRATO N° 123456
        - CONTRATO NUMERO: 7789
        - CONTRATO:1234
        - OPERACI6N N 613665
        - OPERACIONNUM9988
        - OPERACION:
          9988

    Retorna:
        (valor_extraido: str, tipo: str)  → tipo puede ser "CONTRATO", "OPERACION", o "" si no se detecta nada.
    """

    texto = texto.upper()

    # Corrección de errores típicos OCR
    texto = (texto
      #  .replace("0", "O")
      #  .replace("1", "I")
      #  .replace("5", "S")
      #  .replace("6", "Ó")
      #  .replace("8", "B")
      )

    patrones = [
        (r"(?:CONTRATO|CONTRAT[ÓO]|CONTRAT0)\s*(?:N[°º\.\:]?\s*|NUM(?:ERO)?\.?\s*|NUM\.?\s*|:?\s*)?([A-Z0-9\-\/]{4,})", "CONTRATO"),
        ((
                r"(?:"
                r"(?:NRO\.?|NRO|N°|Nº|NUM(?:ERO)?\.?)\s*"
                r")?"
                r"(?:OPERACI[ÓO6]N|OPERAC1[ÓO6]N|OPERACION|OPERAC16N)"
                r"(?:\s+LEASING)?"
                r"[\s:\-]*"
                r"([A-Z0-9\-\/]{4,})"
                ), "OPERACION")
    ]

    lineas = texto.splitlines()

    for i, linea in enumerate(lineas):
        combinada = linea.strip()
        if i + 1 < len(lineas):
            combinada += " " + lineas[i + 1].strip()

        for patron, tipo in patrones:
            match = re.search(patron, combinada)
            if match:
                valor = match.group(1).strip().rstrip(" .:-")
                
                # Validación: debe contener al menos un número
                if not re.search(r"\d", valor):
                    continue

                # Excluir falsos positivos por contexto
                if valor not in {"CONTRATO", "OPERACION", "CODIGO", "INTERNO"}:
                    if debug:
                        print(f"✅ Detectado [{tipo}]: '{valor}' en línea {i+1}")
                    return valor
                    #return valor, tipo

                    
    if debug:
        print("❌ No se encontró número de contrato u operación.")
    return "", ""

# Función que limpia lo que retornan otras funciones con opción de debug
def limpiar_valor_extraido(valor, debug=False):
    if not isinstance(valor, str):
        valor = str(valor)

    original = valor
    valor = valor.strip()
    valor = valor.replace("[SEGMENTO_CABECERA]", "")

    if debug:
        print("=== DEBUG limpiar_valor_extraido ===")
        print(f"Valor original: '{original}'")
        print(f"Después de strip(): '{valor}'")

    # Eliminar ".0" al final (sólo si es un número decimal exacto)
    if valor.endswith(".0"):
        if debug:
            print("✔ Se elimina '.0' del final")
        valor = valor[:-2]

    # Eliminar guiones al final
    if re.search(r"[-]+$", valor):
        if debug:
            print("✔ Se eliminan guiones del final")
        valor = re.sub(r"[-]+$", "", valor)

    # Eliminar signos de exclamación al final
    if re.search(r"[!]+$", valor):
        if debug:
            print("✔ Se eliminan signos de exclamación del final")
        valor = re.sub(r"[!]+$", "", valor)

    # Eliminar puntos múltiples al final (.., ...)
    if re.search(r"[.]{2,}$", valor):
        if debug:
            print("✔ Se eliminan puntos múltiples del final")
        valor = re.sub(r"[.]{2,}$", "", valor)

    valor = valor.strip()

    # Eliminar ':' al inicio o al final
    if re.search(r"^:|:$", valor):
        if debug:
            print("✔ Se elimina ':' al inicio o final del valor")
        valor = re.sub(r"^:|:$", "", valor)

    valor = valor.strip()

    # Eliminar símbolos no alfanuméricos como ':', ';', '.', ',', '·', '-', '»', etc. al inicio o final
    # Busca cualquier carácter que no sea letra o número y lo remueve de los extremos
    if re.search(r"^[^A-Z0-9ÁÉÍÓÚÑ]+|[^A-Z0-9ÁÉÍÓÚÑ]+$", valor.upper()):
        if debug:
            print(f"✔ Se eliminan símbolos especiales del inicio/final: '{valor}'")
        valor = re.sub(r"^[^A-Z0-9ÁÉÍÓÚÑ]+|[^A-Z0-9ÁÉÍÓÚÑ]+$", "", valor, flags=re.IGNORECASE)

    valor = valor.strip()

    if debug:
        print(f"Valor final limpio: '{valor}'")
        print("=" * 40)

    return valor

def dividir_texto_en_cabecera_y_cuerpo_old(texto, debug=False, umbral_similitud=75):
    patrones_corte = [
        "DETALLE", "DETALLES", "CÓDIGO", "CODIGO", "DESCRIPCION", "DESCRIPCIÓN",
        "DOCUMENTO REF.", "DOCUMENTOS REFERENCIADOS", "NOMBRE ITEM", "NOMBRE ARTICULO",
        "POR LO SIGUIENTE"
    ]

    exclusiones = [
        "codigo ppu", "descripcion documento", "detalle cliente",
        "detalle comprador", "detalle vendedor", "codigo interno", "CÓDIGODECORRERCIO"
    ]

    texto_limpio = re.sub(r"[^\w\s]", "", texto).lower()

    # Verificar si contiene una exclusión: abortar corte
    for excl in exclusiones:
        if excl in texto_limpio:
            if debug:
                print(f"⚠️ Patrón de exclusión encontrado: '{excl}' → No se realiza corte")
            return texto.strip(), texto.strip()

    # Buscar mejor match de patrón
    mejor_match = process.extractOne(texto_limpio, patrones_corte, scorer=fuzz.partial_ratio)

    if mejor_match:
        patron, score, _ = mejor_match
        if debug:
            print(f"🔍 Mejor match: '{patron}' (score: {score})")

        if score >= umbral_similitud:
            match_re = re.search(re.escape(patron), texto, re.IGNORECASE)
            if match_re:
                corte_idx = match_re.start()
                largo_texto = len(texto)

                # Si el corte está más allá de la mitad del texto, se anula
                if corte_idx > largo_texto // 2:
                    if debug:
                        print(f"⚠️ Corte encontrado en la segunda mitad {patron}(posición {corte_idx}/{largo_texto}) → Se ignora")
                    return texto.strip(), texto.strip()

                if debug:
                    print(f"✂️ Corte textual detectado en carácter {corte_idx} por patrón '{patron}'")
                cabecera = " [SEGMENTO_CABECERA] "+texto[:corte_idx].strip()
                cuerpo = " [SEGMENTO_CUERPO] "+texto[corte_idx:].strip()
                return cabecera, cuerpo

    # No se encontró corte válido
    if debug:
        print("❌ No se encontró patrón de corte válido. Se retorna texto completo en ambas variables.")
    return texto.strip(), texto.strip()

def dividir_texto_en_cabecera_y_cuerpo(texto, debug=False):

    patrones_corte = [
        "DETALLE", "DETALLES", "DETALL1E", "DETAL1ES",
        "CÓDIGO", "CODIGO", "C0DIG0", "CÓD1G0",
        "DESCRIPCION", "DESCRIPCIÓN", "DESCR1PC1ON", "DESCR1PCION",
        "DOCUMENTO REF.", "DOCUMENTOS REFERENCIADOS", "DOCUMENT0", "D0CUMENT0",
        "NOMBRE ITEM", "NOMBRE ARTICULO", "N0MBRE", "ART1CULO",
        "POR LO SIGUIENTE", "P0R L0 S1GU1ENTE", "P0R L0 S1G.", "PORSIGUIENTE", "POR VENTA DE"
    ]

    exclusiones = [
        "CODIGO PPU", "DESCRIPCION DOCUMENTO", "DETALLE CLIENTE",
        "DETALLE COMPRADOR", "DETALLE VENDEDOR", "CODIGO INTERNO",
        "CÓDIGODECORRERCIO", "CÓDIGO DE COMERCIO"
    ]

    texto_limpio = re.sub(r"[^\w\s]", "", texto).upper()
    ''' for excl in exclusiones:
        if excl in texto_limpio:
            if debug:
                print(f"⚠️ Patrón de exclusión encontrado: '{excl}' → No se realiza corte")
            return texto.strip(), texto.strip()
    '''
    lineas_original = texto.splitlines()
    coincidencias = []

    for i, linea in enumerate(lineas_original):
        linea_upper = linea.upper()
        for patron in patrones_corte:
            if patron in linea_upper:
                corte_idx = sum(len(lineas_original[j]) + 1 for j in range(i))
                coincidencias.append((corte_idx, patron, i))

    if debug and coincidencias:
        print("🔍 Coincidencias encontradas:")
        for idx, patron, linea_idx in coincidencias:
            print(f"  - Línea {linea_idx+1}: patrón '{patron}' en posición {idx}")

    coincidencias_validas = [c for c in coincidencias if c[0] <= len(texto) // 2]
    if not coincidencias_validas:
        if debug:
            print("❌ No se encontró patrón de corte válido antes de la mitad del texto.")
        return texto.strip(), texto.strip()

    corte_idx, patron_usado, linea_idx = sorted(coincidencias_validas, key=lambda x: x[0])[0]

    if debug:
        print(f"✂️ Corte detectado en carácter {corte_idx} (línea {linea_idx+1}) por patrón '{patron_usado}'")

    cabecera = " [SEGMENTO_CABECERA] " + texto[:corte_idx].strip()
    cuerpo = " [SEGMENTO_CUERPO] " + texto[corte_idx:].strip()

    # Reemplazar múltiples espacios por un solo espacio
    cabecera = re.sub(r"\s{2,}", " ", cabecera)
    cuerpo = re.sub(r"\s{2,}", " ", cuerpo)
    return cabecera, cuerpo

# Función para extraer el footer de totales
def extraer_footer_totales(texto_completo, debug=False):
    """
    Extrae el bloque de texto desde la etiqueta [SEGMENTO_TOTALES] hasta el final.
    Si no se encuentra la etiqueta, retorna el texto completo.

    Parámetros:
        texto_completo (str): Texto completo del documento.
        debug (bool): Si es True, imprime mensajes de depuración.

    Retorna:
        str: Contenido desde [SEGMENTO_TOTALES] hasta el final, o el texto completo si no se encuentra.
    """
    marcador = "[SEGMENTO_TOTALES]"
    index = texto_completo.find(marcador)

    if index != -1:
        footer_totales = texto_completo[index:].strip()
        if debug:
            print("✅ Footer detectado correctamente desde [SEGMENTO_TOTALES].")
    else:
        footer_totales = texto_completo.strip()
        if debug:
            print("⚠️ No se encontró el marcador [SEGMENTO_TOTALES], se devuelve el texto completo.")

    return footer_totales
    
# Función central que utiliza todas las funciones anteriores para extraer los datos
def extraer_datos(texto, archivo_origen="", id_documento=None, metodo=None):
    """
    Extrae los campos estructurados desde el texto OCR.
    Recibe adicionalmente el id del documento, el metodo y el archivo_origen.
    """

    # Se cargan los diccionarios dinámicamente
    allowed = cargar_diccionario_colores(ruta_colores)
    allowed_carrocerias = cargar_diccionario_carrocerias(ruta_carrocerias)
    allowed_comunas = cargar_diccionario_comunas(ruta_comunas)
    allowed_ciudades = cargar_diccionario_ciudades(ruta_ciudades)
    allowed_marcas = cargar_diccionario_marcas(ruta_marcas)

    cabecera, cuerpo = dividir_texto_en_cabecera_y_cuerpo(texto, debug=False)
    footer_totales = extraer_footer_totales(texto, debug=False)
    #texto=cuerpo

    marca = extraer_marca(cuerpo, allowed_marcas, debug=False)
    datos = {
        'documento_id': id_documento,
        'metodo': metodo,
        'archivo_origen': archivo_origen,
        'tipo_doc': extraer_tipo_documento(texto[:415], debug=False),
        'numero_documento': limpiar_valor_extraido(extraer_numero_documento(cabecera), debug=False),
        'localidad': extraer_localidad(cabecera, allowed_comunas),
        'fecha_documento': extraer_fecha_documento(cabecera, debug=False),
        'nombre_proveedor': limpiar_valor_extraido(extraer_nombre_proveedor(cabecera, debug=False), debug=False),  # Aquí se podría aplicar lógica personalizada o función adicional
        'rut_proveedor': extraer_rut_proveedor(cabecera),
        'nombre_comprador': limpiar_valor_extraido(extraer_nombre_comprador(cabecera, debug=False), debug=False),  # Se puede agregar lógica adicional si es necesario
        'rut_comprador': extraer_rut_comprador(cabecera),
        'direccion_comprador': limpiar_valor_extraido(extraer_direccion_comprador(cabecera, debug=False),debug=False),
        'telefono_comprador': extraer_telefono_comprador(cabecera, debug=False),
        'comuna_comprador': extraer_comuna_comprador(cabecera, allowed_comunas),
        'ciudad_comprador': extraer_ciudad_comprador(cabecera, allowed_ciudades),
        'placa_patente': extraer_placa_patente(cuerpo),
        'tipo_vehiculo': extraer_tipo_vehiculo(cuerpo, debug=False),
        'marca': marca,
        'modelo': extraer_modelo(cuerpo),
        'n_motor': extraer_n_motor(cuerpo, debug=False),
        'n_chasis': extraer_n_chasis(cuerpo, debug=False),
        'vin': extraer_vin(cuerpo),
        'serie': extraer_serie(cuerpo),
        'color': extraer_color(cuerpo, allowed),
        'anio': limpiar_valor_extraido(extraer_anio(cuerpo), debug=False),
        'unidad_pbv': extraer_unidad_pbv(cuerpo),
        'pbv': extraer_pbv(cuerpo, debug=False),
        'cit': extraer_cit(cuerpo, marca,  debug=False),
        'combustible': limpiar_valor_extraido(extraer_combustible(cuerpo, debug=False), debug=False),
        'unidad_carga': extraer_unidad_carga(cuerpo),
        'carga': limpiar_valor_extraido(extraer_carga(cuerpo), debug=False),
        'asientos': limpiar_valor_extraido(extraer_asientos(cuerpo), debug=False),
        'puertas': limpiar_valor_extraido(extraer_puertas(cuerpo, debug=False), debug=False),
        'unidad_potencia': extraer_unidad_potencia(cuerpo),
        'potencia_motor': extraer_potencia_motor(cuerpo),
        'ejes': extraer_ejes(cuerpo),
        'traccion': extraer_traccion(cuerpo),
        'tipo_carroceria': extraer_tipo_carroceria(cuerpo, allowed_carrocerias, debug=False),
        'cilindrada': extraer_cilindrada(cuerpo),
        'transmision': extraer_transmision(cuerpo),

        'monto_neto': limpiar_valor_extraido(extraer_monto_neto(footer_totales, debug=False), debug=False),
        'monto_iva': limpiar_valor_extraido(extraer_monto_iva(footer_totales, debug=False), debug=False),
        'monto_total': limpiar_valor_extraido(extraer_monto_total(footer_totales, debug=False), debug=False),

        'num_contrato': limpiar_valor_extraido(extraer_num_contrato(texto, debug=False), debug=False)
    }
    return datos

def cargar_textos_desde_bd(forzar_id=None):
    try:
        connection = pymysql.connect(
            host=config.get('database', 'host'),
            user=config.get('database', 'user'),
            password=config.get('database', 'password'),
            database=config.get('database', 'dbname'),
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            if forzar_id:
                # 1. Soft-delete en extracciones_campos, para que despues en el visor se muestre la extraccion nueva
                sql_soft_delete = """
                    UPDATE extracciones_campos
                    SET deleted_at = %s
                    WHERE documento_id = %s AND deleted_at IS NULL
                """
                ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(sql_soft_delete, (ahora, forzar_id))
                connection.commit()

                # 2. Cargar texto asociado al documento
                sql = """
                    SELECT id, documento_id, metodo, texto_extraccion
                    FROM extracciones_texto_total
                    WHERE documento_id = %s AND deleted_at IS NULL
                """
                cursor.execute(sql, (forzar_id,))
            else:
                sql = """
                    SELECT extracciones_texto_total.id, documento_id, metodo, texto_extraccion
                    FROM extracciones_texto_total
                    inner join documentos on documentos.id = extracciones_texto_total.documento_id
                    WHERE documentos.estado = 2 AND documentos.deleted_at IS NULL 
                """
                cursor.execute(sql)

            resultados = cursor.fetchall()

        return resultados

    except Exception as e:
        logging.error(f"[ERROR] No se pudo conectar o consultar la base de datos: {e}")
        print(f"[ERROR] No se pudo conectar o consultar la base de datos: {e}")
        sys.exit(1)
    finally:
        if connection:
            connection.close()
            
# Función nueva para exportar a base de datos
def exportar_datos_bd(datos):
    if not datos:
        print("[WARNING] No hay datos estructurados para insertar.")
        logging.warning("Intento de exportar a BD sin datos estructurados.")
        return

    try:
        connection = pymysql.connect(
            host=config.get('database', 'host'),
            user=config.get('database', 'user'),
            password=config.get('database', 'password'),
            database=config.get('database', 'dbname'),
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            for registro in datos:
                documento_id = registro.get("documento_id")
                metodo = registro.get("metodo", "desconocido")
                archivo_origen = registro.get("archivo_origen", None)

                for campo, valor in registro.items():
                    if campo in ["documento_id", "metodo", "archivo_origen"]:
                        continue  # no es un campo a insertar como valor

                    if not valor:
                        continue  # no insertamos campos vacíos

                    sql = """
                        INSERT INTO extracciones_campos (documento_id, metodo, campo, valor, score, archivo_origen)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (documento_id, metodo, campo, valor, None, archivo_origen))

        connection.commit()
        print("[INFO] Datos exportados a base de datos exitosamente.")
        logging.info("Datos exportados a base de datos exitosamente.")

    except Exception as e:
        print(f"[ERROR] Error al insertar en la base de datos: {e}")
        logging.error(f"Error al insertar en la base de datos: {e}")
    finally:
        if connection:
            connection.close()

def marcar_como_texto_extraido(ids, documento_ids):
    try:
        connection = pymysql.connect(
            host=config.get('database', 'host'),
            user=config.get('database', 'user'),
            password=config.get('database', 'password'),
            database=config.get('database', 'dbname'),
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            # Update extracciones_texto_total
            formato_ids_update = ','.join(['%s'] * len(ids))
            sql_update_extracciones = f"UPDATE extracciones_texto_total SET estado = 3, updated_at = NOW() WHERE id IN ({formato_ids_update})"
            cursor.execute(sql_update_extracciones, tuple(ids))
            logging.info(f"Actualizados {len(ids)} registros en extracciones_texto_total a estado 3.")

            # Update documentos
            if documento_ids:
                # Remove duplicates
                documento_ids_unicos = list(set(documento_ids))
                formato_doc_ids = ','.join(['%s'] * len(documento_ids_unicos))
                sql_update_documentos = f"UPDATE documentos SET estado = 3, updated_at = NOW() WHERE id IN ({formato_doc_ids})"
                cursor.execute(sql_update_documentos, tuple(documento_ids_unicos))
                logging.info(f"Actualizados {len(documento_ids_unicos)} registros en documentos a estado 3.")

        connection.commit()
    except Exception as e:
        logging.error(f"Error actualizando estado de registros: {e}")
    finally:
        if connection:
            connection.close()

def obtener_documento_id(nombre_archivo, cursor):
    sql = "SELECT id FROM documentos WHERE nombre_archivo = %s"
    cursor.execute(sql, (nombre_archivo,))
    result = cursor.fetchone()
    return result[0] if result else None

def clasificar_facturas(json_file, output_file):

    # Cargar datos OCR desde el archivo JSON
    with open(json_file, "r", encoding="utf-8") as f:
        ocr_data = json.load(f)
    
    datos_estructurados = []
    # Procesar cada registro extraído (cada factura o página)
    for registro in ocr_data:
        # Se utiliza "texto_ocr" o, en su defecto, "texto"
        #texto = registro.get("texto_ocr", registro.get("texto", ""))
        #extraidos = extraer_datos(texto)
        texto = registro.get("texto_ocr", registro.get("texto", ""))
        archivo = registro.get("archivo", registro.get("archivo_pdf", ""))
        extraidos = extraer_datos(texto, archivo_origen=archivo)
        # Agregar información adicional:
        # Se extrae el nombre del archivo origen usando la clave "archivo" o "archivo_pdf"
        extraidos["archivo_origen"] = registro.get("archivo", registro.get("archivo_pdf", ""))
        extraidos["pagina"] = registro.get("pagina", "")
        datos_estructurados.append(extraidos)
    
    # Exportar los datos estructurados a un archivo CSV
    df = pd.DataFrame(datos_estructurados)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Clasificación completada. Datos guardados en {output_file}")
    return ocr_data, datos_estructurados

def exportar_json_entidades(ocr_data, datos_estructurados, output_file_json):
    resultados = []
    for registro, datos in zip(ocr_data, datos_estructurados):
        texto = registro.get("texto_ocr", registro.get("texto", ""))
        archivo = registro.get("archivo", registro.get("archivo_pdf", ""))
        entidades = {k: v for k, v in datos.items() if v and k not in ["archivo_origen", "pagina"]}
        resultados.append({
            "archivo": archivo,
            "texto": texto,
            "entidades": entidades
        })

    with open(output_file_json, "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=4, ensure_ascii=False)
    print(f"Datos estructurados en formato JSON guardados en {output_file_json}")


if __name__ == "__main__":
    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Argumentos
    parser = argparse.ArgumentParser(description="Procesar campos desde textos extraídos.")
    parser.add_argument("--forzar_id", type=int, help="ID único de extracciones_texto_total a procesar.")
    parser.add_argument("--modo_simulacion", action="store_true", help="Simula el proceso de extracción sin insertar en BD.")
    parser.add_argument("--debug", action="store_true", help="Activa mensajes de depuración")
    parser.add_argument("--procesar_todos", action="store_true", help="Procesar todos los documentos pendientes")
    args = parser.parse_args()
    debug = args.debug

    directorio = directorio_salida_csv

    try:
        archivos = [f for f in os.listdir(directorio) if f.startswith("ocr_datos_") and f.endswith(".json")]
    except Exception as e:
        print(f"[ERROR] No se pudo listar archivos en {directorio}: {e}")
        logging.error(f"No se pudo listar archivos en {directorio}: {e}")
        sys.exit(1)

    if not archivos:
        #print("[WARNING] No se encontró ningún archivo JSON en el directorio configurado.")
        if not GUARDAR_BD:
            print("[ERROR] No hay datos para procesar ni salida a BD configurada. Verifica configuración y ejecución previa.")
            logging.error("No se encontraron archivos para procesar y guardar_bd está en False.")
            sys.exit(1)
        else:
            #print("[INFO] No se encontró JSON, pero guardar_bd = True. Se procesará texto desde Base de Datos.")
            print("[INFO] Se procesa texto desde Base de Datos.")
            logging.info("Procesando texto directamente desde Base de Datos.")

            if args.forzar_id:
                    textos = cargar_textos_desde_bd(forzar_id=args.forzar_id)
            # Procesa el ID específico
            elif args.procesar_todos or not archivos:
                 # Procesa todos los documentos pendientes
                 textos = cargar_textos_desde_bd(forzar_id=0)

            
            if not textos:
                print("[WARNING] No se encontraron registros en la BD con estado = 'pendiente' (o ID no existe).")
                logging.warning("No hay registros con estado 'pendiente' en extracciones_texto_total.")
                sys.exit(0)

            datos_estructurados = []
            ids_procesados = []
            documento_ids_procesados = []

            for registro in textos:
                texto = registro['texto_extraccion']
                documento_id = registro['documento_id']
                metodo = registro['metodo']
                archivo_origen = f"BD_doc_{documento_id}_metodo_{metodo}"

                if debug: print(f"[DEBUG] documento_id: {documento_id}")
                if debug: print(f"[DEBUG] metodo: {metodo}")
                if debug: print(f"[DEBUG] archivo_origen: {archivo_origen}")
                #print(f"[DEBUG] TEXTO OCR:\\n{texto[:1000]}")  # Puedes ajustar longitud

                extraidos = extraer_datos(texto, archivo_origen=archivo_origen, id_documento=documento_id, metodo=metodo)
                for k, v in extraidos.items():
                    if debug: print(f"[DEBUG] {k}: {v}")
                datos_estructurados.append(extraidos)
                ids_procesados.append(registro['id'])
                documento_ids_procesados.append(documento_id)

            ocr_data = textos

            if GUARDAR_BD and datos_estructurados:
                if debug: print(f"[DEBUG] Total registros estructurados: {len(datos_estructurados)}")
                if datos_estructurados:
                    if debug: print(f"[DEBUG] Campos extraídos del primero:\\n{datos_estructurados[0]}")
                exportar_datos_bd(datos_estructurados)
                print(f"[INFO] Datos exportados a base de datos exitosamente.")
                logging.info("Datos exportados exitosamente a la base de datos.")
                marcar_como_texto_extraido(ids_procesados, documento_ids_procesados)

            print("[INFO] Proceso terminado.")
            logging.info("Proceso de extracción de campos terminado correctamente.")
            sys.exit(0)

    # Si hay JSON, continuar procesamiento tradicional
    archivo_mas_reciente = max(archivos)
    json_file = os.path.join(directorio, archivo_mas_reciente)

    try:
        output_file_csv = os.path.join(directorio_salida_csv, f"facturas_estructuradas_{fecha_hora}.csv")
        output_file_json = os.path.join(directorio_salida_json, f"facturas_estructuradas_{fecha_hora}.json")

        ocr_data, datos_estructurados = clasificar_facturas(json_file, output_file_csv)
        logging.info(f"Archivo OCR procesado: {archivo_mas_reciente}")
        logging.info(f"Cantidad de registros estructurados: {len(datos_estructurados)}")

        if GUARDAR_CSV:
            df = pd.DataFrame(datos_estructurados)
            df.to_csv(output_file_csv, index=False, encoding="utf-8-sig")
            print(f"[INFO] Datos CSV guardados en {output_file_csv}")
            logging.info(f"Archivo CSV generado exitosamente: {output_file_csv}")

        if GUARDAR_JSON:
            exportar_json_entidades(ocr_data, datos_estructurados, output_file_json)
            print(f"[INFO] Datos JSON guardados en {output_file_json}")
            logging.info(f"Archivo JSON generado exitosamente: {output_file_json}")

        if datos_estructurados:
            if debug: print(f"[DEBUG] Total registros estructurados: {len(datos_estructurados)}")
            if datos_estructurados:
                if debug: print(f"[DEBUG] Campos extraídos del primero:\\n{datos_estructurados[0]}")

            if not args.modo_simulacion and GUARDAR_BD:
                exportar_datos_bd(datos_estructurados)
                print(f"[INFO] Datos exportados a base de datos exitosamente.")
                logging.info("Datos exportados exitosamente a la base de datos.")
                documento_ids_procesados = [d['documento_id'] for d in datos_estructurados]
                ids_procesados = [d['id'] for d in ocr_data] # Assuming ocr_data has the ids
                marcar_como_texto_extraido(ids_procesados, documento_ids_procesados)
            else:
                print("[INFO] Simulación completa: NO se insertaron datos en BD.")
                logging.info("Modo simulación activo: no se insertaron datos.")

    except Exception as e:
        print(f"[ERROR] Error durante procesamiento: {e}")
        logging.error(f"Error durante procesamiento: {e}")
        sys.exit(1)

    print("[INFO] Proceso terminado.")
    logging.info("Proceso de extracción de campos terminado correctamente.")