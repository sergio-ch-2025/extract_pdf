#python3 consolidar_por_score.py --forzar_id=123
#python3 consolidar_por_score.py --solo_campo=marca
#python3 consolidar_por_score.py --forzar_id=123 --solo_campo=marca
#

import pymysql
import logging
import argparse
from configparser import ConfigParser
import os

# ============================
# Configuración global
# ============================
CONFIG = ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), '../config/config.cf'))

DB_CONFIG = {
    'host': CONFIG.get('database', 'host'),
    'user': CONFIG.get('database', 'user'),
    'password': CONFIG.get('database', 'password'),
    'database': CONFIG.get('database', 'dbname'),
    'cursorclass': pymysql.cursors.DictCursor
}

PRIORIDAD_METODOS = ["paddleocr", "doctr", "easyocr"]


def consolidar_campos(forzar_id=None, solo_campo=None):
    try:
        connection = pymysql.connect(**DB_CONFIG)
        documentos_a_actualizar = set()

        with connection.cursor() as cursor:
            sql_base = """
                SELECT ec.documento_id, ec.campo
                FROM extracciones_campos ec
                JOIN documentos d ON ec.documento_id = d.id
            """
            condiciones = ["d.estado = 4"]
            valores = []

            if forzar_id:
                condiciones.append("ec.documento_id = %s")
                valores.append(forzar_id)
            if solo_campo:
                condiciones.append("ec.campo = %s")
                valores.append(solo_campo)

            if condiciones:
                sql_base += " WHERE " + " AND ".join(condiciones)

            sql_base += " GROUP BY ec.documento_id, ec.campo"
            cursor.execute(sql_base, valores)
            combinaciones = cursor.fetchall()

            for combo in combinaciones:
                doc_id = combo['documento_id']
                campo = combo['campo']
                documentos_a_actualizar.add(doc_id)

                orden_prioridad = "FIELD(metodo, {}) DESC".format(
                    ', '.join(["'%s'" % m for m in PRIORIDAD_METODOS])
                )

                sql = f"""
                    SELECT metodo, valor, score
                    FROM extracciones_campos
                    WHERE documento_id = %s AND campo = %s AND valor IS NOT NULL AND valor <> ''
                    ORDER BY score DESC, {orden_prioridad}
                    LIMIT 1
                """
                cursor.execute(sql, (doc_id, campo))
                mejor = cursor.fetchone()

                if not mejor:
                    continue

                cursor.execute("""
                    INSERT INTO extraccion_campos_consolidada (documento_id, metodo, campo, valor)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        valor = VALUES(valor),
                        metodo = VALUES(metodo),
                        updated_at = NOW()
                """, (doc_id, mejor['metodo'], campo, mejor['valor']))

                mensaje = f"[OK] Consolidado campo '{campo}' del documento {doc_id} usando método '{mejor['metodo']}' con score {mejor['score']}"
                print(mensaje)
                logging.info(mensaje)

        connection.commit()
        logging.info("Consolidación de campos por score completada.")
        print(f"[INFO] Consolidación completada para {len(combinaciones)} campos.")

        # Actualizar estado de los documentos procesados
        for doc_id in documentos_a_actualizar:
            actualizar_estado_documento_a_consolidado(doc_id)

    except Exception as e:
        logging.error(f"[ERROR] Consolidando campos por score: {e}")
        print(f"[ERROR] Consolidando campos por score: {e}")

    finally:
        if connection:
            connection.close()

def actualizar_estado_documento_a_consolidado(documento_id):
    """
    Actualiza el estado de un documento a 5 (consolidado) en la tabla 'documentos'.
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = "UPDATE documentos SET estado = 5, updated_at = NOW() WHERE id = %s"
            cursor.execute(sql, (documento_id,))
        connection.commit()
        logging.info(f"Estado del documento {documento_id} actualizado a 5 (consolidado).")
        print(f"[INFO] Estado del documento {documento_id} actualizado a 5 (consolidado).")
    except Exception as e:
        logging.error(f"Error al actualizar estado del documento {documento_id} a consolidado: {e}")
        print(f"[ERROR] No se pudo actualizar el estado del documento {documento_id}: {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolida campos de extracciones_campos hacia extraccion_campos_consolidada evaluando por score.")
    parser.add_argument("--forzar_id", type=int, help="Consolidar sólo este documento_id")
    parser.add_argument("--solo_campo", type=str, help="Consolidar sólo este campo (por ejemplo: marca, rut_proveedor, etc.)")
    args = parser.parse_args()

    logging.basicConfig(filename='../logs/actividad.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    consolidar_campos(forzar_id=args.forzar_id, solo_campo=args.solo_campo)
