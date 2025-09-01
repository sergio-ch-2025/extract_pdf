import os
import json
import re
import pandas as pd
import sys
from datetime import datetime
from difflib import SequenceMatcher
from num2words import num2words

# Funciones individuales para extraer cada campo

def extraer_numero_documento(texto):
    # Incluye "Nº", "N°", "N9", "N o", "FOLIO", "NO"
    # Si se modifica este patron, tambien se debe modificar el que esta en "extraer_nombre_comprador"
    patron = r"(?<!\w)(?:N[º°9]?\s*[:\-]?\s*|N\s*[Oo]\s*[:\-]?\s*|FOLIO\s*[:\-]?\s*|NO\s*[:\-]?\s*)(\d{5,})(?!\w)"    
    match = re.search(patron, texto.upper())
    return str(match.group(1).strip()) if match else ""

def extraer_fecha_documento(texto, debug=False):
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
    patrones_fecha = [
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
                    sys.exit("Debug mode: fecha extraída.")
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
                        sys.exit("Debug mode: fecha extraída.")
                    return m.group(1).strip()
        # Si encontramos etiqueta pero no fecha, continuar buscando otras etiquetas

    if debug:
        print("No se encontró fecha de emisión.")
        sys.exit("Debug mode: terminando sin fecha.")
    return ""
    
def extraer_nombre_proveedor(texto, debug=False):
    
    # Lista de formas jurídicas (normalizadas: sin puntos, en mayúsculas)
    formas_juridicas = ['SA', 'SPA', 'LTDA', 'EIRL', 'SOCIEDAD', 'LIMITADA', 'EMPRESA', 'INVERSIONES']
    
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
            sys.exit("Debug mode: terminando script por falta de RUT.")
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
                        sys.exit("Debug mode: terminando script después de extraer el nombre del proveedor (segunda estrategia).")
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
            sys.exit("Debug mode: terminando script por falta de número de documento.")
        return ""
    
    # Definir la lista de etiquetas alternativas para el nombre del comprador.
    # Se usan expresiones regulares con grupos opcionales para abarcar variantes (por ejemplo, con o sin paréntesis).
    etiquetas = [r'\bNOMBRE\b', r'\bSE[ÑN]OR(?:\s*\(ES\))?\b', r'\bCLIENTE\b', r'\bRAZ[ÓO]N\b']
    
    # A partir de la línea siguiente al número de documento, buscar alguna de las etiquetas.
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

    if debug:
        print("No se encontró ninguna de las etiquetas ('NOMBRE', 'SEÑOR(es)', 'CLIENTE') después del número de documento.")
        sys.exit("Debug mode: terminando script por falta de etiqueta para nombre del comprador.")
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

def cargar_diccionario_comunas(archivo="diccionarios/Diccionario_comunas.csv"):
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
    # ⚠️ Si usas esta función, asegúrate de cargar comunas de nuevo
    comunas = cargar_diccionario_comunas("diccionarios/Diccionario_comunas.csv")

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
            sys.exit("Debug mode: terminando por falta de RUT.")
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
                    sys.exit("Debug mode: dirección extraída.")
                return direccion

    if debug:
        print("No se encontró ninguna dirección posterior al RUT.")
        sys.exit("Debug mode: sin dirección encontrada.")
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
        sys.exit("Debug mode: finalizando extracción de teléfono.")

    return telefonos[0] if solo_uno and telefonos else "" if solo_uno else telefonos

def extraer_comuna_comprador(texto, comunas_permitidas=None):
    """
    Extrae la comuna del comprador a partir del texto, buscando la etiqueta "COMUNA"
    y retornando únicamente el valor si se encuentra en el listado de comunas permitidas.
    Si no se encuentra o no es válido, se retorna una cadena vacía.
    """
    if comunas_permitidas is None:
        comunas_permitidas = cargar_diccionario_comunas()

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

def cargar_diccionario_ciudades(archivo="diccionarios/Diccionario_ciudades.csv"):
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

def extraer_placa_patente(texto):
    
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
        "MOTOCICLETA", "MOTO", "VEHÍCULO ELÉCTRICO", "MAQUINA INDUSTRIAL", "CARRO DE ARRASTRE", "MAQUINA AGRICOLA",
        "BICICLETA MOTOR", "CHASIS CABINADO", "COCHE MORTUORIO", "MINIBUS PESADO", "TRICICLO MOTOR", "STATION WAGON",
        "CASA RODANTE", "SEMIREMOLQUE", "TRACTOCAMION", "AMBULANCIA", "CARROBOMBA", "CUADRIMOTO", "CUATRIMOTO",
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

    # 1. Buscar con contexto explícito
    for tipo in tipos_vehiculo:
        patron_etiquetado = rf"(TIPO(?:\s+DE)?\s+VEHICULO[\s:\-]*)\b{re.escape(tipo)}\b"
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

def extraer_marca(texto, debug=False):
    # Lista básica de marcas comunes, puedes ampliarla
    marcas_comunes = [
        "TOYOTA", "HYUNDAI", "FORD", "CHEVROLET", "NISSAN", "MITSUBISHI", "JEEP",
        "KIA", "PEUGEOT", "RENAULT", "FIAT", "VOLKSWAGEN", "BMW", "MERCEDES", 
        "HONDA", "MAZDA", "SSANGYONG", "CITROEN", "JAC", "DFSK", "SUBARU", "CHERY",
        "SUZUKI", "BYD", "VOLVO", "FOTON", "MAXUS", "GEELY", "CHANGAN", "JETOUR",
        "FAW", "IVECO", "SCANIA", "DAEWOO", "MAN", "ISUZU", "RAM"
    ]

    texto_upper = texto.upper()

    # 1. Buscar por etiqueta explícita
    patron = r"(?:MARCA[\s:\-]*)([A-ZÁÉÍÓÚÑ0-9]{2,20})"
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

def extraer_modelo(texto):
    patron = r"(?:MODELO[\s:\-]*)([A-Z0-9\-./ ]{3,40})"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_n_motor(texto, debug=False):
    #patron = r"(?:MOTOR[\s:\-]*)([A-Z0-9\-]{8,20})"
    patron = r"(?:#?\s*(?:MOTOR|MOTCR|NRO. MOTOR|MOT0R))[\s:\-]*([A-Z0-9\-]{7,20})"
    match = re.search(patron, texto.upper())
    
    if match:
        candidato = match.group(1).strip()
        if re.search(r"\d", candidato):  # Debe contener al menos un dígito
            if debug:
                print(f"Número de motor válido encontrado: {candidato}")
            return candidato
        else:
            if debug:
                print(f"Descartado porque no contiene dígitos: {candidato}")
    
    if debug:
        print("No se encontró número de motor válido.")
    return ""

def extraer_n_chasis(texto):
    patron = r"(?:CHASIS|CHASSIS|N[ÚU]?M(?:ERO)?\s*CH\.?)\s*[:\-]*\s*([A-Z0-9\- ]{8,18})"
    match = re.search(patron, texto.upper())
    if match:
        return match.group(1).strip()
    return ""

def extraer_vin(texto):
    patron = r"(?:V\.?\s*I\.?\s*N\.?)\s*[:\-]*([A-HJ-NPR-Z0-9]{17})"
    #patron = r"VIN[\s:\-]*([A-HJ-NPR-Z0-9]{17})"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_serie(texto):
    patron = r"(?:SERIE[\s:\-]*)([A-Z0-9\-]{8,20})"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def cargar_diccionario_colores(archivo="diccionarios/Diccionario_colores.csv"):
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

def extraer_anio(texto):
    #patron = r"(?:AÑO COMERCIAL|ANO COMERCIAL|AÑO|ANO)[\s:\-]*([1-2][0-9]{3})"
    patron = r"(?:A[ÑN\?]O COMERCIAL|A[ÑN\?]O)[\s:\-]*([1-2][0-9]{3})"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_unidad_pbv(texto):
    # Se asume que se indica la unidad en el mismo campo que PBV
    patron = r"(KG|KGS|TON|KILOS)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_pbv(texto):
    
    # El patrón acepta "PESO BRUTO VEHICULAR", "PBV" o "P.B.V.", "PESO BRUTO EN KILOS" (con o sin puntos y espacios)
    patron = r"(?:PESO\s*BRUTO\s*VEHICULAR|PESOBRUTOVEHICULAR|PESO BRUTO VEH?CULAR|PESO\s*BRUTO|PESO\s*BRUTO\s*EN\s*KILOS|P\.?\s*B\.?\s*V\.?|PBV)[\s:\-]*([\d\.,]+)"
    #patron = r"(?:PESO BRUTO VEHICULAR|PESO BRUTO|PESO BRUTO EN KILOS|P\.?\s*B\.?\s*V\.?|PBV)[\s:\-]*([\d\.]+)"    
    match = re.search(patron, texto.upper())
    if match:
        valor = match.group(1).strip()
        # Se elimina cualquier punto que se use como separador de miles.
        # Si se requiere preservar decimales en otro formato, habría que aplicar una lógica adicional.
        valor = valor.replace(".", "").replace(",", "")
        return str(valor)
    return ""

def extraer_cit(texto):
    #patron = r"(?:CIT|INFORME TÉCNICO|CODIGO INFORME TÉCNICO)[\s:\-]*([A-Z0-9\-]{6,})"
    patron = (
    r"(?:CIT|"  # CIT
    r"I[NÑ]FORME\s+[TÉE]CNICO|"  # INFORME TÉCNICO o INFORME TECNICO
    r"C[ÓO]DIGO\s+I[NÑ]FORME\s+[TÉE]CNICO"  # CÓDIGO INFORME TÉCNICO o variantes
    r")[\s:\-]*([A-Z0-9\-]{6,})"
)
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_combustible(texto, debug=False):
    opciones = r"GASOLINA|DIESEL|ELECTRICO|HIBRIDO|GAS LICUADO|GLP|ELECTRICA|PETROLEO|PETROLERO|BENCINA"

    texto_upper = texto.upper()

    # 1. Buscar tras una etiqueta conocida
    patron_etiqueta = rf"(?:TIPO\s+DE\s+COMBUSTIBLE|TIPO\s+COMBUSTIBLE|COMBUSTIBLE)[\s:\-]*((?:(?:{opciones})\b(?:\W+)?)+)"
    match = re.search(patron_etiqueta, texto_upper)
    if match:
        valor = match.group(1).strip()
        if debug:
            print("[Etiqueta] Combustible detectado:", valor)
        return valor

    # 2. Buscar líneas individuales con palabras válidas (sin etiqueta)
    for linea in texto_upper.split("\n"):
        for palabra in opciones.split("|"):
            if re.fullmatch(rf"{palabra}\b.*", linea.strip()):
                if debug:
                    print("[Línea suelta] Combustible detectado en línea:", linea)
                return palabra

    if debug:
        print("No se encontró combustible.")
    return ""

def extraer_unidad_carga(texto):
    patron = r"(Toneladas|Kilos|Ton|KG)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_carga(texto):
    
    # Se busca cualquiera de las etiquetas: "CARGA" (con opción "UTIL") o "CAPACIDAD DE CARGA",
    # seguida de espacios, dos puntos o guiones, y luego una secuencia numérica que puede incluir puntos.
    patron = r"(?:CARGA\s*UTIL(?:\s*ADMISIBLE)?|CARGAUTILADMISIBLE|CAPACIDAD DE CARGA)[\s:\-]*([\d\.,]+)"
    #patron = r"(?:(?:CARGA(?: UTIL)?)|CAPACIDAD DE CARGA)[\s:\-]*([\d\.]+)"
    match = re.search(patron, texto.upper())
    if match:
        valor = match.group(1).strip()
        # Elimina los separadores de miles (puntos)
        valor = valor.replace(".", "")
        valor = valor.replace(",", "")
        return str(valor)
    return ""

def extraer_asientos(texto):
    patrones = [
        r"ASIENTOS[\s:\-]*([0-9]+)",      # ASIENTOS: 7
        r"([0-9]+)[\s\-]*ASIENTOS",       # 7 ASIENTOS o 7Asientos
    ]
    texto_upper = texto.upper()
    for patron in patrones:
        match = re.search(patron, texto_upper)
        if match:
            return match.group(1).strip()
    return ""

def extraer_puertas(texto, debug=False):
    texto_upper = texto.upper()
    # Detecta 'PUERTAS' seguido o precedido de número
    patron = r"(?:PUERTAS[\s:\-]*(\d+)|(\d+)[\s\-]*PUERTAS)"
    match = re.search(patron, texto_upper)
    if match:
        resultado = match.group(1) or match.group(2)
        if debug:
            print("✅ Coincidencia encontrada:", resultado)
        return resultado
    if debug:
        print("❌ No se encontró coincidencia.")
    return ""

def extraer_potencia_motor(texto):
    patron = r"POTENCIA[\s:\-.\n]*([\d\.,]+)"
    match = re.search(patron, texto.upper())
    if match:
        valor = match.group(1).strip().replace(",", "").replace(".", "")
        return valor
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
        "0-D16", "S4-C16", "E24", "S4-D10"
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
    
    # El patrón busca la palabra "TRACCION" seguida de espacios, dos puntos o guiones,
    # y luego captura uno de los valores permitidos.
    #patron = r"TRACCION[\s:\-]*((?:10X4|4X(?:2|4)|6X(?:2|4|6)|8X(?:2|4|6|8)))"
    #patron = r"TRACCI[ÓO\?N][\s:\-]*(?:DELANTERA|TRASERA|4X2|4X4|6X2|6X4|6X6|8X2|8X4|8X6|8X8|10X4)"
    patron = r"TRACCI[ÓO]?N(?:\s+DELANTERA)?[\s:\-]*((?:10X4|4X(?:2|4)|6X(?:2|4|6)|8X(?:2|4|6|8)))"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def cargar_diccionario_carrocerias(archivo="diccionarios/carrocerias.csv"):
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
    
def extraer_tipo_carroceria(texto, allowed_carrocerias=None):
    """
    Extrae el tipo de carrocería a partir del texto, buscando la etiqueta "TIPO CARROCERIA" y
    retornando únicamente un valor que se encuentre en el diccionario de carrocerías.
    Si no se encuentra un valor permitido, se retorna cadena vacía.
    """
    if allowed_carrocerias is None:
        allowed_carrocerias = cargar_diccionario_carrocerias()
    
    # Patrón que busca "TIPO CARROCERIA" seguido de una secuencia de letras y espacios
    #patron = r"(?:TIPO CARROCERIA)[\s:\-]*([A-Z\s]+)"
    patron = r"(?:TIPO\s+CARROCERIA|CARROCERIA)[\s:\-]*([A-Z\s]+)"
    match = re.search(patron, texto.upper())
    if match:
        candidato = match.group(1).strip()
        # Se recorre el diccionario de carrocerías para ver si alguna de las opciones está contenida en el candidato
        for carroceria in allowed_carrocerias:
            # Se puede usar "in" para permitir que el candidato incluya palabras adicionales
            if carroceria in candidato:
                return carroceria
        # Si no se encontró coincidencia exacta, se podría retornar el candidato o cadena vacía; en este ejemplo, se retorna vacío.
        return ""
    return ""

def extraer_cilindrada(texto):
    patron = r"CILINDRADA[\s:\-]*([\d\.]+\s*CC)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_transmision(texto):
    patron = r"(?:TRANSMISI[ÓOÕÒ´`'?]N)[\s:\-]*([A-Z]+)"
    match = re.search(patron, texto.upper())
    return match.group(1).strip() if match else ""

def extraer_monto_neto(texto, debug=False):
    """
    Extrae el monto neto desde el texto. Acepta variantes como:
    - MONTO NETO
    - TOTAL NETO
    - SUB-TOTAL
    El valor puede estar hasta 3 líneas después de la etiqueta.
    Retorna un valor numérico si es mayor a 100, de lo contrario retorna "".
    """
    etiquetas = [
        r"MONTO\s+NETO",
        r"TOTAL\s+NETO",
        r"SUB[\s\-]?TOTAL",
        r"\bNETO\b",
        r"\b0NETO\b",
        r"\bONETO\b"
    ]
    patron_monto = r"\$?\s*([\d\.]+)"
    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_monto_neto ===")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

    for i, linea in enumerate(lineas):
        for etiqueta in etiquetas:
            if re.search(etiqueta, linea):
                if debug:
                    print(f"Etiqueta encontrada en línea {i+1}: {linea}")
                # Buscar monto en la misma línea
                match = re.search(patron_monto, linea)
                if match:
                    valor = match.group(1).replace('.', '')
                    if valor.isdigit() and int(valor) > 100:
                        if debug:
                            print(f"Monto válido encontrado en misma línea: {valor}")
                        return int(valor)

                # Buscar en hasta 3 líneas siguientes
                for j in range(1, 4):
                    if i + j >= len(lineas):
                        break
                    siguiente = lineas[i + j]
                    match = re.search(patron_monto, siguiente)
                    if match:
                        valor = match.group(1).replace('.', '')
                        if valor.isdigit() and int(valor) > 100:
                            if debug:
                                print(f"Monto válido encontrado en línea {i+1+j}: {valor}")
                            return int(valor)

    if debug:
        print("No se encontró monto neto válido.")
    return ""

def extraer_monto_iva(texto, debug=False):
    import re

    etiquetas = [
        r"MONTO\s+I\.?V\.?A\.?",             # MONTO IVA
        r"Monto I\.V\.A\.",                  # Monto I.V.A.
        r"I\.?V\.?A\.?",                     # I.V.A. o IVA
        r"IVA\s*:?[\s\-]?",                  # IVA: o IVA -
        r"IVA\s*\(?19\s*%?\)?",              # IVA (19%), IVA 19%
        r"19\s*%\s*IVA",                     # 19% IVA
        r"19\s*%\s*I\.?V\.?A\.?\s*\$?",      # 19% I.V.A.$
        r"\b19%1\.V\.A\.\b",                 # 19%1.V.A.
        r"\bIVA 19.0%\b",                    # IVA 19.0%
        r"\bTVA\s*19\s*%",                   # TVA 19%
        r"\bVA19%",                          # VA19%
        r"\b19%VA\b",                        # 19%VA
        r"VA19%",                            # VA19%
        r"MONTO L\.V\.A",                    # MONTO L.V.A
        r"1\.V\.A\.19%",                     # 1.V.A.19%
    ]

    patron_monto = r"\$?\s*([\d\.\s]{4,15})"  # acepta números largos con espacios o puntos

    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]

    if debug:
        print("=== DEBUG extraer_monto_iva ===")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

    for i, linea in enumerate(lineas):
        for etiqueta in etiquetas:
            if re.search(etiqueta, linea):
                if debug:
                    print(f"Etiqueta encontrada en línea {i+1}: {linea}")

                etiqueta_pos = re.search(etiqueta, linea)
                if etiqueta_pos:
                    linea_post = linea[etiqueta_pos.end():]
                    match = re.search(patron_monto, linea_post)
                    if match:
                        valor = re.sub(r"[^\d]", "", match.group(1))
                        if debug:
                            print(f"Monto en misma línea (crudo): {match.group(1)} -> limpio: {valor}")
                        if valor.isdigit() and len(valor) >= 4:
                            return int(valor)

                # Buscar en las siguientes 3 líneas
                for j in range(1, 4):
                    if i + j >= len(lineas):
                        break
                    siguiente = lineas[i + j]
                    match = re.search(patron_monto, siguiente)
                    if match:
                        valor = re.sub(r"[^\d]", "", match.group(1))
                        if debug:
                            print(f"Monto en línea {i+1+j} (crudo): {match.group(1)} -> limpio: {valor}")
                        if valor.isdigit() and len(valor) >= 4:
                            return int(valor)

    if debug:
        print("No se encontró monto IVA válido.")
    return ""

'''import re
import sys

def extraer_monto_total(texto, debug=False):
    """
    Extrae el monto total desde el texto.
    - Busca la última etiqueta válida y evalúa hacia abajo primero, luego hacia arriba si no encuentra.
    - Soporta etiquetas comunes como TOTAL BRUTO, TOTAL FACTURA, MONTO TOTAL, TOTAL.
    - Retorna el valor numérico si es mayor a 100, sino retorna "".
    """
    etiquetas = [
        r"TOTAL\s+BRUTO",
        r"TOTAL\s+FACTURA",
        r"MONTO\s+TOTAL",
        r"\bTOTAL\b",
        r"TOTALS"
    ]
    patron_monto = r"\$?\s*([\d\.]+)"
    lineas = [linea.strip() for linea in texto.upper().split('\n') if linea.strip()]
    ult_idx = None
    ult_etiqueta = ""

    if debug:
        print("=== DEBUG extraer_monto_total ===")
        for i, l in enumerate(lineas):
            print(f"{i+1:02d}: {l}")
        print("-" * 40)

    # Buscar la última ocurrencia de una etiqueta
    for i, linea in enumerate(lineas):
        for etiqueta in etiquetas:
            if re.search(etiqueta, linea):
                ult_idx = i
                ult_etiqueta = etiqueta

    if ult_idx is not None and debug:
        print(f"Última etiqueta '{ult_etiqueta}' encontrada en línea {ult_idx+1}: {lineas[ult_idx]}")

    if ult_idx is not None:
        # Buscar hacia abajo (hasta 3 líneas)
        for j in range(0, 4):
            if ult_idx + j >= len(lineas):
                break
            match = re.search(patron_monto, lineas[ult_idx + j])
            if match:
                valor = match.group(1).replace('.', '')
                if valor.isdigit() and int(valor) > 100:
                    if debug:
                        print(f"Monto total válido encontrado hacia abajo en línea {ult_idx+1+j}: {valor}")
                    return int(valor)

        # Buscar hacia arriba (hasta 3 líneas)
        for j in range(1, 4):
            if ult_idx - j < 0:
                break
            match = re.search(patron_monto, lineas[ult_idx - j])
            if match:
                valor = match.group(1).replace('.', '')
                if valor.isdigit() and int(valor) > 100:
                    if debug:
                        print(f"Monto total válido encontrado hacia arriba en línea {ult_idx+1-j}: {valor}")
                    return int(valor)

    if debug:
        print("No se encontró monto total válido.")
    return ""
'''

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

# Función que limpia lo que retornan otras funciones con opción de debug
def limpiar_valor_extraido(valor, debug=False):
    if not isinstance(valor, str):
        valor = str(valor)

    original = valor
    valor = valor.strip()

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

    if debug:
        print(f"Valor final limpio: '{valor}'")
        print("=" * 40)

    return valor

# Función central que utiliza todas las funciones anteriores para extraer los datos
def extraer_datos(texto, archivo_origen=""):
    # Se carga el diccionario (puedes ajustar la ruta del archivo)
    allowed = cargar_diccionario_colores("diccionarios/Diccionario_colores.csv")
    allowed_carrocerias = cargar_diccionario_carrocerias("diccionarios/carrocerias.csv")
    allowed_comunas = cargar_diccionario_comunas("diccionarios/Diccionario_comunas.csv")
    allowed_ciudades = cargar_diccionario_ciudades("diccionarios/Diccionario_ciudades.csv")

    
    datos = {
        'archivo_origen': archivo_origen,
        'numero_documento': limpiar_valor_extraido(extraer_numero_documento(texto), debug=False),
        'fecha_documento': extraer_fecha_documento(texto, debug=False),
        'nombre_proveedor': extraer_nombre_proveedor(texto, debug=False),  # Aquí se podría aplicar lógica personalizada o función adicional
        'rut_proveedor': extraer_rut_proveedor(texto),
        'nombre_comprador': limpiar_valor_extraido(extraer_nombre_comprador(texto, debug=False), debug=False),  # Se puede agregar lógica adicional si es necesario
        'rut_comprador': extraer_rut_comprador(texto),
        'direccion_comprador': extraer_direccion_comprador(texto, debug=False),
        'telefono_comprador': extraer_telefono_comprador(texto, debug=False),
        'comuna_comprador': extraer_comuna_comprador(texto, allowed_comunas),
        'ciudad_comprador': extraer_ciudad_comprador(texto, allowed_ciudades),
        'placa_patente': extraer_placa_patente(texto),
        'tipo_vehiculo': extraer_tipo_vehiculo(texto),
        'marca': extraer_marca(texto, debug=False),
        'modelo': extraer_modelo(texto),
        'n_motor': extraer_n_motor(texto, debug=False),
        'n_chasis': extraer_n_chasis(texto),
        'vin': extraer_vin(texto),
        'serie': extraer_serie(texto),
        'color': extraer_color(texto, allowed),
        'anio': limpiar_valor_extraido(extraer_anio(texto), debug=False),
        'unidad_pbv': extraer_unidad_pbv(texto),
        'pbv': extraer_pbv(texto),
        'cit': extraer_cit(texto),
        'combustible': limpiar_valor_extraido(extraer_combustible(texto), debug=False),
        'unidad_carga': extraer_unidad_carga(texto),
        'carga': limpiar_valor_extraido(extraer_carga(texto), debug=False),
        'asientos': limpiar_valor_extraido(extraer_asientos(texto), debug=False),
        'puertas': limpiar_valor_extraido(extraer_puertas(texto, debug=False), debug=False),
        'unidad_potencia': extraer_unidad_potencia(texto),
        'potencia_motor': extraer_potencia_motor(texto),
        'ejes': extraer_ejes(texto),
        'traccion': extraer_traccion(texto),
        'tipo_carroceria': extraer_tipo_carroceria(texto, allowed_carrocerias),
        'cilindrada': extraer_cilindrada(texto),
        'transmision': extraer_transmision(texto),
        'monto_neto': limpiar_valor_extraido(extraer_monto_neto(texto, debug=False), debug=False),
        'monto_iva': limpiar_valor_extraido(extraer_monto_iva(texto, debug=False), debug=False),
        'monto_total': limpiar_valor_extraido(extraer_monto_total(texto, debug=False), debug=False)
    }
    return datos

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
    # Buscar en el directorio "resultados" el archivo JSON con la fecha más reciente
    directorio = "resultados"
    archivos = [f for f in os.listdir(directorio) if f.startswith("ocr_datos_") and f.endswith(".json")]
    if not archivos:
        print("No se encontró ningún archivo JSON en el directorio 'resultados'.")
        exit(1)
    archivo_mas_reciente = max(archivos)
    json_file = os.path.join(directorio, archivo_mas_reciente)
    
# Crear nombre de archivo de salida con fecha y hora y guardarlo en "resultados"
fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = os.path.join("resultados", f"facturas_estructuradas_{fecha_hora}.csv")

output_file_json = output_file.replace(".csv", ".json")


 # ✅ Recuperar OCR y resultados
ocr_data, datos_estructurados =clasificar_facturas(json_file, output_file)
exportar_json_entidades(ocr_data, datos_estructurados, output_file_json)