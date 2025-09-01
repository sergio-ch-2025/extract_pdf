## Procesar solo documento_id = 1071
#python3 evaluador_score.py --id=1071
# Procesar todos los documentos sin score
#python3 evaluador_score.py
#
#
#
import pymysql
import logging
import configparser
import os
import re
from datetime import datetime
import difflib
from datetime import datetime
import csv
import argparse


# ============================
# Configuración global
# ============================
CONFIG = configparser.ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), '../config/config.cf'))

DB_CONFIG = {
    'host': CONFIG.get('database', 'host'),
    'user': CONFIG.get('database', 'user'),
    'password': CONFIG.get('database', 'password'),
    'database': CONFIG.get('database', 'dbname'),
    'cursorclass': pymysql.cursors.DictCursor
}


ruta_marcas = CONFIG.get('extraccion', 'ruta_diccionario_marcas', fallback='../diccionarios/marcas.csv')

def cargar_marcas_desde_csv(ruta):
    marcas = []
    try:
        with open(ruta, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                marca = row.get('marca', '').strip().upper()
                if marca:
                    marcas.append(marca)
    except Exception as e:
        print(f"[ERROR] No se pudo cargar el archivo de marcas: {e}")
    return marcas

MARCAS_VALIDAS = cargar_marcas_desde_csv(ruta_marcas)



def evaluar_score_old(campo, valor):
    if not valor:
        return 0.0

    valor = valor.strip()

    if campo == 'rut_proveedor':
        return 1.0 if len(valor) >= 9 and '-' in valor else 0.1

    elif campo == 'anio':
        try:
            anio = int(valor)
            return 1.0 if 1900 <= anio <= datetime.now().year + 1 else 0.1
        except:
            return 0.0

    elif campo == 'color':
        return 1.0 if valor.isalpha() and len(valor) >= 3 else 0.1

    elif campo == 'marca':
        return 1.0 if valor.isalpha() and len(valor) >= 2 else 0.1

    elif campo == 'monto_total':
        return 1.0 if valor.replace('.', '').isdigit() else 0.1

    elif campo == 'n_chasis':
        if len(valor) == 17:
            decima = valor[9].upper()
            return 1.0 if decima in VIN_AÑO_MAP else 0.1
        return 0.3

    else:
        return 0.1


# Mapeo para VIN (posición 10)
VIN_AÑO_MAP = {
    'A': 1980, 'B': 1981, 'C': 1982, 'D': 1983, 'E': 1984, 'F': 1985,
    'G': 1986, 'H': 1987, 'J': 1988, 'K': 1989, 'L': 1990, 'M': 1991,
    'N': 1992, 'P': 1993, 'R': 1994, 'S': 1995, 'T': 1996, 'V': 1997,
    'W': 1998, 'X': 1999, 'Y': 2000,
    '1': 2001, '2': 2002, '3': 2003, '4': 2004, '5': 2005,
    '6': 2006, '7': 2007, '8': 2008, '9': 2009
}



MARCAS_VALIDAS = [
    "TOYOTA", "HYUNDAI", "FORD", "CHEVROLET", "NISSAN", "MITSUBISHI",
    "JEEP", "KIA", "PEUGEOT", "RENAULT", "FIAT", "VOLKSWAGEN", "BMW",
    "MERCEDES", "HONDA", "MAZDA", "SSANGYONG", "CITROEN", "JAC", "DFSK",
    "SUBARU", "CHERY", "SUZUKI", "BYD", "VOLVO", "FOTON", "MAXUS",
    "GEELY", "CHANGAN", "JETOUR", "FAW", "IVECO", "SCANIA", "DAEWOO",
    "MAN", "ISUZU", "RAM"
]

TIPOS_DOCUMENTO_VALIDOS = [
    "FACTURA ELECTRONICA", "NOTA DE CREDITO ELECTRONICA", "NOTA DE CREDITO",
    "ORDEN DE COMPRA", "HOMOLOGADO", "CEDULA DE IDENTIDAD", "CONTRATO",
    "ROL UNICO TRIBUTARIO"
]

COLORES_VALIDOS = {"ROJO", "AZUL", "VERDE", "GRIS", "NEGRO", "BLANCO", "AMARILLO", "BEIGE", "CAFÉ", "PLATEADO"}
UNIDADES_VALIDAS = {"KG", "CV", "KW"}

# =========================
# FUNCIONES DE VALIDACION
# =========================

def validar_dv(rut):
    """
    Valida el dígito verificador de un RUT chileno usando algoritmo módulo 11.
    """
    cuerpo, dv = rut.split("-")
    suma = 0
    multiplo = 2
    for c in reversed(cuerpo):
        suma += int(c) * multiplo
        multiplo = 9 if multiplo == 7 else multiplo + 1
    resto = suma % 11
    verificador = 11 - resto
    verificador = "K" if verificador == 10 else "0" if verificador == 11 else str(verificador)
    return dv.upper() == verificador

def score_rut_proveedor(valor):
    """
    Evalúa el RUT por etapas:
    - 0.0 si está vacío
    - 0.1 si contiene algo pero no tiene guión
    - +0.2 si es numérico con al menos 7 dígitos
    - +0.3 si tiene formato RUT válido (NNNNNNNN-DV)
    - +0.4 si el DV es válido según módulo 11
    Máximo score: 1.0
    """
    logging.debug("[VAL] Validando rut_proveedor")

    if not valor or not valor.strip():
        return 0.0

    valor = valor.strip()
    score = 0.1  # Base si contiene algo

    # Verifica largo y que tenga al menos 7 dígitos
    numeros = re.sub(r"[^\d]", "", valor)
    if len(numeros) >= 7:
        score += 0.2

    # Verifica formato de RUT
    if re.fullmatch(r"\d{7,8}-[\dkK]", valor):
        score += 0.3

        # Verifica el DV
        if validar_dv(valor):
            score += 0.4

    return min(score, 1.0)

def score_anio(valor):
    """
    Valida si el año es un número de cuatro dígitos dentro de un rango razonable (1900 - año actual + 1).
    Penaliza valores como 0000 o 9999.
    """
    logging.debug("[VAL] Validando anio")
    if not valor.isdigit():
        return 0.0
    anio = int(valor)
    if anio in (0, 9999):
        return 0.1
    return 1.0 if 1900 <= anio <= datetime.now().year + 1 else 0.1

def score_fecha(valor):
    """
    Valida si el valor es una fecha válida en formato dd/mm/yyyy o yyyy-mm-dd
    y que el año esté en un rango lógico (>= 2000).
    """
    logging.debug("[VAL] Validando fecha")
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            fecha = datetime.strptime(valor, fmt)
            return 1.0 if fecha.year >= 2000 and fecha.year <= datetime.now().year else 0.3
        except:
            continue
    return 0.0

def score_placa_patente(valor):
    """
    Valida si la patente tiene formato válido chileno (ej: AA1234, BBBB99).
    """
    logging.debug("[VAL] Validando placa_patente")
    return 1.0 if re.fullmatch(r"^[A-Z]{2,4}\d{2,4}$", valor.upper()) else 0.3

def score_marca(valor):
    """
    Verifica si la marca coincide exactamente con las marcas válidas cargadas desde CSV.
    Si coincide exactamente, retorna 1.0.
    Si tiene una coincidencia aproximada (similitud), retorna 0.6.
    En otro caso, retorna 0.1.
    """
    logging.debug("[VAL] Validando marca")
    valor = valor.strip().upper()
    if valor in MARCAS_VALIDAS:
        return 1.0
    elif difflib.get_close_matches(valor, MARCAS_VALIDAS, cutoff=0.8):
        return 0.6
    return 0.1

def score_tipo_vehiculo(valor):
    """
    Evalúa si el tipo de vehículo es específico y no contiene términos genéricos como "AUTO" o "VEHICULO".
    """
    logging.debug("[VAL] Validando tipo_vehiculo")
    valor = valor.upper()
    palabras_genericas = {"AUTO", "VEHICULO", "VEHICULO MOTORIZADO"}
    if any(p in valor for p in palabras_genericas):
        return 0.2
    return 1.0 if len(valor) >= 3 and valor.isalpha() else 0.1

def score_vin(valor):
    """
    Valida que el VIN tenga 17 caracteres, que no contenga I, O, Q
    y que el carácter 10 esté en el mapa de años.
    """
    logging.debug("[VAL] Validando vin")
    if len(valor) == 17 and not re.search(r"[IOQ]", valor.upper()):
        return 1.0 if valor[9].upper() in VIN_AÑO_MAP else 0.5
    return 0.3

def score_color(valor):
    """
    Valida si el color extraído coincide exactamente con el listado de colores válidos.
    """
    logging.debug("[VAL] Validando color")
    valor = valor.upper()
    return 1.0 if valor in COLORES_VALIDOS else 0.3

def score_tipo_doc(valor):
    """
    Evalúa si el tipo de documento coincide con una lista blanca o si es una variante similar aceptada.
    """
    logging.debug("[VAL] Validando tipo_doc")
    if valor.upper() in TIPOS_DOCUMENTO_VALIDOS:
        return 1.0
    elif difflib.get_close_matches(valor.upper(), TIPOS_DOCUMENTO_VALIDOS, cutoff=0.75):
        return 0.6
    return 0.1

def score_unidad_pbv(valor):
    """
    Verifica si la unidad de peso bruto vehicular es válida (ej: KG, CV).
    """
    logging.debug("[VAL] Validando unidad_pbv")
    return 1.0 if valor.upper() in UNIDADES_VALIDAS else 0.3

def score_unidad_carga(valor):
    """
    Evalúa si la unidad de carga pertenece al conjunto autorizado de unidades.
    """
    logging.debug("[VAL] Validando unidad_carga")
    return 1.0 if valor.upper() in UNIDADES_VALIDAS else 0.3

def score_unidad_potencia(valor):
    """
    Verifica si la unidad de potencia es reconocida dentro de los estándares aceptados.
    """
    logging.debug("[VAL] Validando unidad_potencia")
    return 1.0 if valor.upper() in UNIDADES_VALIDAS else 0.3

def score_anio(valor):
    logging.debug("[VAL] Validando anio")
    try:
        anio = int(valor)
        return 1.0 if 1900 <= anio <= datetime.now().year + 1 else 0.1
    except:
        return 0.0

def score_color(valor):
    logging.debug("[VAL] Validando color")
    return 1.0 if valor.isalpha() and len(valor) >= 3 else 0.1

def score_marca(valor):
    logging.debug("[VAL] Validando marca")
    return 1.0 if valor.isalpha() and len(valor) >= 2 else 0.1

def score_monto(valor):
    logging.debug("[VAL] Validando monto")
    return 1.0 if valor.replace('.', '').isdigit() else 0.1

def score_generico(valor):
    logging.debug("[VAL] Validando con score genérico")
    return 0.6 if len(valor.strip()) >= 3 else 0.1

def score_n_chasis(valor):
    logging.debug("[VAL] Validando n_chasis")
    if len(valor) == 17:
        decima = valor[9].upper()
        return 1.0 if decima in VIN_AÑO_MAP else 0.1
    return 0.3

def score_fecha(valor):
    logging.debug("[VAL] Validando fecha")
    try:
        fecha = datetime.strptime(valor, '%d/%m/%Y')
        return 1.0 if fecha.year >= 1900 and fecha.year <= datetime.now().year else 0.1
    except:
        return 0.0
 
def score_transmision(valor):
    logging.debug("[VAL] Validando transmision")
    return 1.0 if valor.isalpha() and len(valor) >= 2 else 0.1

def score_combustible(valor):
    logging.debug("[VAL] Validando combustible")
    return 1.0 if valor.isalpha() and len(valor) >= 2 else 0.1

def score_carga(valor):
    logging.debug("[VAL] Validando carga")
    return 1.0 if valor.isdigit() and len(valor) >= 2 else 0.1

def score_asientos(valor):
    logging.debug("[VAL] Validando asientos")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_puertas(valor):
    logging.debug("[VAL] Validando puertas")    
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_potencia(valor):
    logging.debug("[VAL] Validando potencia")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_ejes(valor):
    logging.debug("[VAL] Validando ejes")       
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1  

def score_tipo_carroceria(valor):
    logging.debug("[VAL] Validando tipo_carroceria")    
    return 1.0 if valor.isalpha() and len(valor) >= 2 else 0.1 

def score_cilindrada(valor):
    logging.debug("[VAL] Validando cilindrada")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_traccion(valor):
    logging.debug("[VAL] Validando traccion")
    return 1.0 if valor.isalpha() and len(valor) >= 2 else 0.1      

# Nueva función score_cit
def score_cit(valor):
    logging.debug("[VAL] Validando cit")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def potencia_motor(valor):
    logging.debug("[VAL] Validando potencia_motor")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

#def score_cit(valor):
 #   logging.debug("[VAL] Validando cit")
  #  return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_serie(valor):
    logging.debug("[VAL] Validando serie")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_unidad_pbv(valor):
    logging.debug("[VAL] Validando unidad_pbv")
    return 1.0 if valor.isalpha() and len(valor) >= 1 else 0.1

def score_pbv(valor):
    logging.debug("[VAL] Validando pbv")    
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1  

def score_monto_neto(valor):
    logging.debug("[VAL] Validando monto_neto")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_monto_iva(valor):
    logging.debug("[VAL] Validando monto_iva")  
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1  

def score_monto_total(valor):
    logging.debug("[VAL] Validando monto_total")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_placa_patente(valor):
    logging.debug("[VAL] Validando placa_patente")
    return 1.0 if valor.isalpha() and len(valor) >= 3 else 0.1

def score_n_motor(valor):
    logging.debug("[VAL] Validando n_motor")    
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_vin(valor):
    logging.debug("[VAL] Validando vin")
    if len(valor) == 17:
        decima = valor[9].upper()
        return 1.0 if decima in VIN_AÑO_MAP else 0.1
    return 0.3

def score_tipo_doc(valor):
    logging.debug("[VAL] Validando tipo_doc")
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1

def score_numero_documento(valor):
    logging.debug("[VAL] Validando numero_documento")   
    return 1.0 if valor.isdigit() and len(valor) >= 1 else 0.1  


def evaluar_score(campo, valor):
    '''tipo_doc', 'numero_documento', 'fecha_documento', 'nombre_proveedor', 'nombre_comprador',
        'rut_comprador', 'direccion_comprador', 'telefono_comprador', 'comuna_comprador',
        'ciudad_comprador', 'placa_patente', 'tipo_vehiculo', 'modelo', 'n_motor', 'n_chasis',
        'vin', 'serie', 'unidad_pbv', 'pbv', 'cit', 'combustible', 'unidad_carga', 'carga',
        'asientos', 'puertas', 'unidad_potencia', 'potencia_motor', 'ejes', 'traccion',
        'tipo_carroceria', 'cilindrada', 'transmision', 'monto_neto', 'monto_iva'
    ]: '''

    if not valor:
        logging.debug(f"[VAL] Valor vacío para campo: {campo}")
        return 0.0

    valor = valor.strip()

    funciones = {
        'tipo_doc': score_tipo_doc,
        'rut_proveedor': score_rut_proveedor,
        'rut_comprador': score_rut_proveedor,
        'numero_documento': score_numero_documento,
        'fecha_documento': score_fecha,

        'tipo_vehiculo': score_tipo_vehiculo,
        'placa_patente': score_placa_patente,
        'anio': score_anio,
        'color': score_color,
        'marca': score_marca,
        'n_chasis': score_n_chasis,
        'transmision': score_transmision,
        'combustible': score_combustible,
        'carga': score_carga,
        'asientos': score_asientos,
        'puertas': score_puertas,
        'potencia_motor': potencia_motor,
        'ejes': score_ejes,
        'tipo_carroceria': score_tipo_carroceria,
        'cilindrada': score_cilindrada,
        'traccion': score_traccion,
        'cit': score_cit,
        'serie': score_serie,
        'unidad_pbv': score_unidad_pbv,
        'pbv': score_pbv,
        'n_motor': score_n_motor,
        'vin': score_vin,
        'monto_total': score_monto,
        'monto_neto': score_monto,
        'monto_iva': score_monto
        
        
    }

    if campo in funciones:
        return funciones[campo](valor)
    else:
        return score_generico(valor)
    

def actualizar_estado_documento_a_evaluado(documento_id):
    """
    Actualiza el estado de un documento a 4 (evaluado) en la tabla 'documentos'.
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = "UPDATE documentos SET estado = 4, updated_at = NOW() WHERE id = %s"
            cursor.execute(sql, (documento_id,))
        connection.commit()
        logging.info(f"Estado del documento {documento_id} actualizado a 4 (evaluado).")
        print(f"[INFO] Estado del documento {documento_id} actualizado a 4 (evaluado).")
    except Exception as e:
        logging.error(f"Error al actualizar estado del documento {documento_id} a evaluado: {e}")
        print(f"[ERROR] No se pudo actualizar el estado del documento {documento_id}: {e}")
    finally:
        if connection:
            connection.close()

def actualizar_scores(documento_id=None):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        documentos_actualizados = set()

        with connection.cursor() as cursor:
            sql = """
                SELECT id, campo, valor, metodo, documento_id FROM extracciones_campos
                WHERE (score IS NULL OR score = 0)
            """
            valores = []

            if documento_id:
                sql += " AND documento_id = %s"
                valores.append(documento_id)

            cursor.execute(sql, valores)
            registros = cursor.fetchall()

            for reg in registros:
                score = evaluar_score(reg['campo'], reg['valor'])
                if reg['campo'] == 'tipo_doc' and 'metodo' in reg and reg['metodo'] == 'paddleocr':
                    score = min(score + 0.20, 1.0)
                cursor.execute("""
                    UPDATE extracciones_campos SET score = %s, updated_at = NOW()
                    WHERE id = %s
                """, (score, reg['id']))
                if reg['documento_id']:
                    documentos_actualizados.add(reg['documento_id'])

        connection.commit()
        logging.info(f"Se actualizaron {len(registros)} scores.")
        print(f"[INFO] Scores actualizados: {len(registros)} registros.")

        # Update status for all processed documents
        for doc_id in documentos_actualizados:
            actualizar_estado_documento_a_evaluado(doc_id)

    except Exception as e:
        logging.error(f"[ERROR] Actualizando scores: {e}")
        print(f"[ERROR] Actualizando scores: {e}")

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    
    logging.basicConfig(filename='../logs/actividad.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description="Actualiza los scores de extracciones_campos.")
    parser.add_argument("--id", type=int, help="Filtrar por documento_id específico (opcional)")
    args = parser.parse_args()

    actualizar_scores(documento_id=args.id)

