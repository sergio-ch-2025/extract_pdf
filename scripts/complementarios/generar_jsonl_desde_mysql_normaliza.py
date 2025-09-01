import pymysql
import json
import re
from datetime import datetime

def normaliza_fecha(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.strip()
    meses = {
        'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04', 'MAY': '05', 'JUN': '06',
        'JUL': '07', 'AGO': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12',
        'ENERO': '01', 'FEBRERO': '02', 'MARZO': '03', 'ABRIL': '04', 'MAYO': '05', 'JUNIO': '06',
        'JULIO': '07', 'AGOSTO': '08', 'SEPTIEMBRE': '09', 'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12'
    }

    valor = valor.upper()
    valor = re.sub(r"[^\w\s/-]", "", valor)  # permite letras, números, espacios, guiones y slash
    valor = re.sub(r"\s+", " ", valor).strip()

    # YYYY-MM-DD
    try:
        dt = datetime.strptime(valor, '%Y-%m-%d')
        if 1900 <= dt.year <= 2100:
            return dt.strftime('%Y-%m-%d')
    except:
        pass

    # DD-MM-YYYY
    match = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", valor)
    if match:
        dia, mes, anio = match.groups()
        if 1900 <= int(anio) <= 2100:
            return f"{anio}-{mes.zfill(2)}-{dia.zfill(2)}"

    # DD-MMM-YYYY o DD MMM YYYY
    match = re.match(r"^(\d{1,2})[-\s]?([A-Z]+)[-\s]?(\d{4})$", valor)
    if match:
        dia, mes, anio = match.groups()
        mes_num = meses.get(mes.strip(), None)
        if mes_num and 1900 <= int(anio) <= 2100:
            return f"{anio}-{mes_num.zfill(2)}-{dia.zfill(2)}"

    # DD/MM/YYYY
    match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", valor)
    if match:
        dia, mes, anio = match.groups()
        if 1900 <= int(anio) <= 2100:
            return f"{anio}-{mes.zfill(2)}-{dia.zfill(2)}"

    # DD DE MES DE YYYY (ej. "29 DE ABRIL DE 2025" o "29 ABRIL 2025")
    match = re.match(r"^(\d{1,2})\s*(?:DE\s*)?([A-Z]+)\s*(?:DE\s*)?(\d{4})$", valor)
    if match:
        dia, mes, anio = match.groups()
        mes_num = meses.get(mes.strip(), None)
        if mes_num and 1900 <= int(anio) <= 2100:
            return f"{anio}-{mes_num.zfill(2)}-{dia.zfill(2)}"

    # YYYY only
    match = re.match(r"^(\d{4})$", valor)
    if match:
        anio = match.group(1)
        if 1900 <= int(anio) <= 2100:
            return anio

    return ""

    

def valida_rut_dv(rut):
    rut = rut.replace('.', '').replace('-', '').upper()
    if not rut[:-1].isdigit():
        return False
    cuerpo = rut[:-1]
    dv = rut[-1]
    suma = 0
    multiplo = 2
    for c in reversed(cuerpo):
        suma += int(c) * multiplo
        multiplo = 9 if multiplo == 7 else multiplo + 1
    res = 11 - (suma % 11)
    dv_calc = {10: 'K', 11: '0'}.get(res, str(res))
    return dv == dv_calc

def normaliza_rut(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.upper().replace(" ", "").replace(".", "").replace("RUT", "").replace("R.U.T", "").replace("R.U.T.", "").replace(":", "")
    valor = valor.replace("-", "")

    match = re.match(r"^(\d{7,8})([0-9K])$", valor)
    if match:
        cuerpo, dv = match.groups()
        rut_formateado = f"{int(cuerpo):,}".replace(",", ".") + f"-{dv}"
        if valida_rut_dv(rut_formateado):
            return rut_formateado
        else:
            print(f"⚠️ RUT inválido (DV no cuadra): {valor}")
            return rut_formateado  # lo devolvemos igual
    else:
        print(f"⚠️ RUT no reconocido: {valor}")
        return valor  # devolvemos el valor crudo como fallback

def normaliza_unidad(unidad):
    if not unidad or not isinstance(unidad, str):
        return ""
    unidad = unidad.upper()
    unidades = {
        "KG": ["KG", "KGS", "KILOS", "KILO"],
        "HP": ["HP", "CV", "HORSEPOWER"],
        "CC": ["CC", "CM3", "CILINDRADA"],
        "L": ["L", "LITRO", "LITROS"],
        "KM": ["KM", "KILOMETRO", "KILOMETROS"]
    }
    for norm, variantes in unidades.items():
        if unidad in variantes:
            return norm
    return unidad

def normaliza_texto_simple(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.strip().upper()
    valor = re.sub(r"\s+", " ", valor)
    valor = valor.replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U").replace("Ñ", "N")
    valor = re.sub(r"[^\w\s\-\.]", "", valor)
    return valor

def normaliza_cilindrada(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.replace(".", "").replace(",", "").strip()
    if valor.isdigit():
        if int(valor) > 50 and int(valor) < 20000:  # rango cilindrada razonable
            return valor
    return ""

def normaliza_numero(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.replace(".", "").replace(",", "").strip()
    if valor.isdigit():
        if int(valor) > 0:
            return str(int(valor))
    return ""

def normaliza_potencia_motor(valor):
    if not valor or not isinstance(valor, str):
        return ""
    match = re.search(r"(\d+)", valor)
    if match:
        potencia = int(match.group(1))
        if 20 < potencia < 2000:
            return str(potencia)
    return ""

def normaliza_transmision(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.upper().replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
    if "MANUAL" in valor:
        return "MANUAL"
    if "AUTOMATICA" in valor or "AUTOMATICO" in valor or "AUTOMÁTICA" in valor or "AUTOMÁTICO" in valor:
        return "AUTOMATICA"
    if "CVT" in valor:
        return "CVT"
    return normaliza_texto_simple(valor)

def normaliza_combustible(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.upper().replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
    if "DIESEL" in valor:
        return "DIESEL"
    if "GASOLINA" in valor or "BENCINA" in valor:
        return "GASOLINA"
    if "ELECTRICO" in valor:
        return "ELECTRICO"
    if "HIBRIDO" in valor:
        return "HIBRIDO"
    return normaliza_texto_simple(valor)

def normaliza_vin(valor):
    if not valor or not isinstance(valor, str):
        return ""
    valor = valor.strip().upper().replace(" ", "")
    if len(valor) == 17 and re.match(r"^[A-Z0-9]+$", valor):
        return valor
    return ""

def normaliza_campos(campos):
    campos_norm = {}
    for k, v in campos.items():
        if k in ["rut_proveedor", "rut_comprador"]:
            campos_norm[k] = normaliza_rut(v)
        elif k == "fecha_documento":
            campos_norm[k] = normaliza_fecha(v)
        elif k in ["unidad_pbv", "unidad_carga", "unidad_potencia"]:
            campos_norm[k] = normaliza_unidad(v)
        elif k in ["marca", "modelo", "color", "tipo_vehiculo", "tipo_carroceria"]:
            campos_norm[k] = normaliza_texto_simple(v)
        elif k in ["nombre_proveedor", "nombre_comprador"]:
            campos_norm[k] = normaliza_texto_simple(v)
        elif k in ["pbv", "carga", "asientos", "puertas", "anio"]:
            campos_norm[k] = normaliza_numero(v)
        elif k in ["cilindrada"]:
            campos_norm[k] = normaliza_cilindrada(v)
        elif k == "potencia_motor":
            campos_norm[k] = normaliza_potencia_motor(v)
        elif k == "traccion":
            campos_norm[k] = normaliza_texto_simple(v)
        elif k == "transmision":
            campos_norm[k] = normaliza_transmision(v)
        elif k == "combustible":
            campos_norm[k] = normaliza_combustible(v)
        elif k == "vin":
            campos_norm[k] = normaliza_vin(v)
        elif k in ["n_motor", "n_chasis", "serie", "cit", "ejes"]:
            # Mayúsculas, sin espacios dobles
            campos_norm[k] = normaliza_texto_simple(v)
        else:
            campos_norm[k] = v.strip() if isinstance(v, str) else v
    return campos_norm

# Configuración de conexión a la base de datos
conn = pymysql.connect(
    host='localhost',
    user='sch_virtual',
    password='Linux3468',
    db='extraccion_pdf',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

output_path = "llama_factura_prompts_train_v3_normalizado.jsonl"
metodo_preferido = 'paddleocr'

campos_objetivo = [
    "numero_documento", "fecha_documento", "rut_proveedor", "rut_comprador", "nombre_proveedor",
    "nombre_comprador", "tipo_vehiculo", "marca", "modelo", "color", "anio", "n_motor", "n_chasis",
    "vin", "serie", "pbv", "unidad_pbv", "carga", "unidad_carga", "asientos", "puertas",
    "potencia_motor", "unidad_potencia", "traccion", "cilindrada", "transmision",
    "tipo_carroceria", "combustible", "cit", "ejes"
]

try:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT et.documento_id
            FROM extracciones_texto_total et
            JOIN extraccion_campos_consolidada ec
              ON et.documento_id = ec.documento_id
            WHERE et.metodo = %s
              AND ec.campo = 'tipo_doc'
              AND ec.valor = 'FACTURA ELECTRONICA'
              AND et.documento_id >= 1071
        """, (metodo_preferido,))
        documentos = cursor.fetchall()

        with open(output_path, "w", encoding="utf-8") as fout:
            for doc in documentos:
                documento_id = doc["documento_id"]

                cursor.execute("SELECT texto_extraccion FROM extracciones_texto_total WHERE documento_id = %s AND metodo = %s LIMIT 1", (documento_id, metodo_preferido))
                texto_row = cursor.fetchone()
                if not texto_row or not texto_row["texto_extraccion"]:
                    continue
                texto = texto_row["texto_extraccion"].strip()

                # Obtener todos los campos disponibles para el documento
                cursor.execute("""
                    SELECT campo, valor, metodo
                    FROM extraccion_campos_consolidada
                    WHERE documento_id = %s
                """, (documento_id,))
                filas = cursor.fetchall()

                # Agrupar por campo, priorizando el método preferido
                campos_extraidos = {}
                for fila in filas:
                    campo = fila["campo"]
                    valor = fila["valor"]
                    metodo = fila["metodo"]

                    if campo not in campos_objetivo:
                        continue

                    if campo not in campos_extraidos:
                        campos_extraidos[campo] = (valor, metodo)
                    else:
                        # Priorizar si es del método preferido
                        if metodo == metodo_preferido and campos_extraidos[campo][1] != metodo_preferido:
                            campos_extraidos[campo] = (valor, metodo)

                # Extraer solo valores, no métodos
                campos_completos = {campo: campos_extraidos.get(campo, ("", ""))[0] for campo in campos_objetivo}

                print("⚠️ Valor crudo de fecha_documento:", campos_completos.get("fecha_documento"))

                # NORMALIZACIÓN mejorada
                campos_norm = normaliza_campos(campos_completos)

                prompt = (
                    "Extrae los siguientes campos desde una factura chilena de vehículo. El contenido proviene de un OCR en español.\n\n"
                    "[OCR]\n"
                    f"{texto}\n\n"
                    "[Campos a extraer]\n"
                    + ", ".join(campos_objetivo)
                )
                response = json.dumps(campos_norm, ensure_ascii=False)

                fout.write(json.dumps({
                    "prompt": prompt,
                    "response": response
                }, ensure_ascii=False) + "\n")

    print(f"✅ Archivo generado correctamente: {output_path}")

finally:
    conn.close()