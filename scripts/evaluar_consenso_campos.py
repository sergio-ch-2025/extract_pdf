#!/usr/bin/env python3
# Script: evaluar_consenso_campos.py
# Objetivo: Evaluar coincidencias entre métodos de extracción campo por campo para un documento específico o todos
# Parámetros soportados:
#   --id=1071     -> Procesa el documento con ID específico
#   --all         -> Procesa todos los documentos con campos sin evaluar
#   --debug       -> Muestra información detallada del análisis

import pymysql
import logging
import argparse
import os
from configparser import ConfigParser
from collections import Counter

# === Configuración Global ===
CONFIG = ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), '../config/config.cf'))

DB_CONFIG = {
    'host': CONFIG.get('database', 'host'),
    'user': CONFIG.get('database', 'user'),
    'password': CONFIG.get('database', 'password'),
    'database': CONFIG.get('database', 'dbname'),
    'cursorclass': pymysql.cursors.DictCursor
}

# Campos a evaluar por consenso
CAMPOS_RELEVANTES = [
    'tipo_doc', 'numero_documento','localidad', 'fecha_documento', 'nombre_proveedor', 'rut_proveedor',
    'nombre_comprador', 'rut_comprador', 'direccion_comprador', 'telefono_comprador',
    'comuna_comprador', 'ciudad_comprador', 'placa_patente', 'tipo_vehiculo', 'marca', 'modelo',
    'n_motor', 'n_chasis', 'vin', 'serie', 'color', 'anio', 'unidad_pbv', 'pbv', 'cit',
    'combustible', 'unidad_carga', 'carga', 'asientos', 'puertas', 'unidad_potencia',
    'potencia_motor', 'ejes', 'traccion', 'tipo_carroceria', 'cilindrada', 'transmision',
    'monto_neto', 'monto_iva', 'monto_total'
]

def obtener_documentos_pendientes():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT documento_id
                FROM extracciones_campos
                WHERE score IS NULL
            """)
            resultados = cursor.fetchall()
            return [fila['documento_id'] for fila in resultados]
    finally:
        connection.close()

def evaluar_coincidencias_por_documento(doc_id, debug=False):
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            for campo in CAMPOS_RELEVANTES:
                cursor.execute("""
                    SELECT metodo, valor FROM extracciones_campos
                    WHERE documento_id = %s AND campo = %s
                """, (doc_id, campo))

                filas = cursor.fetchall()
                if not filas:
                    continue

                valores_validos = [f for f in filas if f['valor'] and f['valor'].strip() != '']
                if not valores_validos:
                    if debug:
                        print(f"[SKIP] Campo '{campo}' vacío en todos los métodos.")
                    continue

                conteo = Counter([f['valor'].strip() for f in valores_validos])
                max_valor, max_count = conteo.most_common(1)[0]
                total_validos = len(valores_validos)
                metodos_max = [f['metodo'] for f in valores_validos if f['valor'].strip() == max_valor]

                if debug:
                    print(f"\n--- Campo: {campo} ---")
                    print("Total válidos:", total_validos)
                    print("Valores:", conteo)

                if len(conteo) == total_validos:
                    for f in filas:
                        score = 0.2 if f['valor'] and f['valor'].strip() else 0.0
                        cursor.execute("""
                            UPDATE extracciones_campos
                            SET score = %s, updated_at = NOW()
                            WHERE documento_id = %s AND campo = %s AND metodo = %s
                        """, (score, doc_id, campo, f['metodo']))
                    if debug:
                        print(f"[BAJO] Todos los valores distintos para '{campo}', score 0.2 asignado.")
                    continue

                for f in filas:
                    metodo = f['metodo']
                    valor = f['valor'].strip() if f['valor'] else ''

                    if not valor:
                        score = 0.0
                    elif valor == max_valor:
                        if max_count >= 2:
                            score = 1.0
                        elif total_validos > 2 and max_count == total_validos // 2:
                            otros = [v for v in conteo if v != max_valor]
                            if len(otros) == 1 and conteo[otros[0]] == max_count:
                                score = 0.6
                            else:
                                score = 0.5
                        else:
                            score = 0.6
                    else:
                        score = 0.3

                    if debug:
                        print(f"Método: {metodo:10} | Valor: {valor[:40]:40} | Score: {score}")

                    cursor.execute("""
                        UPDATE extracciones_campos
                        SET score = %s, updated_at = NOW()
                        WHERE documento_id = %s AND campo = %s AND metodo = %s
                    """, (score, doc_id, campo, metodo))

        connection.commit()
        print(f"[OK] Evaluación completada para documento_id = {doc_id}")

    except Exception as e:
        logging.error(f"[ERROR] Evaluando consensos: {e}")
        print(f"[ERROR] Evaluando consensos: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evalúa score por consenso entre métodos para cada campo")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--id", type=int, help="ID del documento a evaluar")
    group.add_argument("--all", action="store_true", help="Evaluar todos los documentos pendientes")
    parser.add_argument("--debug", action="store_true", help="Mostrar detalle del proceso")
    args = parser.parse_args()

    logging.basicConfig(filename='../logs/actividad.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    if args.all:
        pendientes = obtener_documentos_pendientes()
        if not pendientes:
            print("[INFO] No hay documentos pendientes para evaluar.")
        else:
            print(f"[INFO] Evaluando {len(pendientes)} documentos pendientes...")
            for doc_id in pendientes:
                evaluar_coincidencias_por_documento(doc_id, args.debug)
    else:
        evaluar_coincidencias_por_documento(args.id, args.debug)