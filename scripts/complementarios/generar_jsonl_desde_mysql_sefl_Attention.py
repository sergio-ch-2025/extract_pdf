import pymysql
import json

# Configuración de conexión a la base de datos
conn = pymysql.connect(
    host='localhost',
    user='sch_virtual',
    password='Linux3468',
    db='extraccion_pdf',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

output_path = "llama_factura_prompts_train_v2.jsonl"
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

                cursor.execute("SELECT campo, valor FROM extraccion_campos_consolidada WHERE documento_id = %s AND metodo = %s", (documento_id, metodo_preferido))
                filas = cursor.fetchall()
                campos_extraidos = {fila["campo"]: fila["valor"] for fila in filas if fila["campo"] in campos_objetivo}
                campos_completos = {campo: campos_extraidos.get(campo, "") for campo in campos_objetivo}

                prompt = (
                    "Extrae los siguientes campos desde una factura chilena de vehículo. El contenido proviene de un OCR en español.\n\n"
                    "[OCR]\n"
                    f"{texto}\n\n"
                    "[Campos a extraer]\n"
                    + ", ".join(campos_objetivo)
                )
                response = json.dumps(campos_completos, ensure_ascii=False)

                fout.write(json.dumps({
                    "prompt": prompt,
                    "response": response
                }, ensure_ascii=False) + "\n")

    print(f"✅ Archivo generado correctamente: {output_path}")

finally:
    conn.close()