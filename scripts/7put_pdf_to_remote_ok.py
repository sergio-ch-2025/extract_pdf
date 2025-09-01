#
# Script para mover o copiar archivos PDF procesados a su destino final
# Puede ejecutarse en modo local (mover archivos) o modo remoto (subir vía SFTP/SCP)
#
#!/usr/bin/env python3
# ESTE ARCHIVO DEBE EJECUTARSE DESPUES DE PROCESAR EL ARCHIVO PDF DE LAS DISTINTAS FORMAS DE EXTRACCION
# Ejecución ejemplos:
#   python3 scripts/put_pdf_to_remote_ok.py --archivo ejemplo.pdf --debug
#   python3 scripts/put_pdf_to_remote_ok.py --allpdf --debug
#
#!/usr/bin/env python3
# ESTE ARCHIVO DEBE EJECUTARSE PARA ENVIAR ARCHIVOS PDF PROCESADOS A CARPETA REMOTA FINAL

import paramiko
from scp import SCPClient
import os
import sys
import argparse
import configparser
import logging
import time
import pymysql
import traceback

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

REMOTE_DB_CONFIG = {
    'host': CONFIG.get('database_lectura', 'host'),
    'user': CONFIG.get('database_lectura', 'user'),
    'password': CONFIG.get('database_lectura', 'password'),
    'database': CONFIG.get('database_lectura', 'dbname'),
    'cursorclass': pymysql.cursors.DictCursor
}

# Configurar logging
logging.basicConfig(filename='../logs/actividad.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def debug_log(message, debug):
    if debug:
        print("[DEBUG]", message)
    logging.debug(message)

def leer_configuracion(config_file, debug=False):

    # Lee y valida los parámetros de configuración del archivo config.cf
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        msg = f"ERROR: Archivo de configuración no encontrado: {config_file}"
        print(msg)
        logging.error(msg)
        sys.exit(1)

    config.read(config_file)

    if 'conexion' not in config or 'paths' not in config:
        msg = "ERROR: Archivo de configuración inválido. Faltan secciones [conexion] o [paths]."
        print(msg)
        logging.error(msg)
        sys.exit(1)

    try:
        parametros = {
            'usuario': config['conexion']['usuario'],
            'host': config['conexion']['host'],
            'password': config['conexion'].get('password', None),
            'port': config['conexion'].getint('port', 22),
            'key_filename': config['conexion'].get('key_filename', None),
            'carpeta_local': config['paths']['carpeta_procesados'],
            'directorio_remoto_leidos': config['paths']['directorio_remoto_leidos'],
            'carpeta_procesados': config['paths']['carpeta_procesados'],
            'directorio_local_para_procesar': config['paths']['directorio_local_para_procesar'],
            'metodo_transferencia': config['conexion'].get('scp_o_sftp', 'sftp'),
        }
    except KeyError as e:
        msg = f"ERROR: Falta clave en configuración: {e}"
        print(msg)
        logging.error(msg)
        sys.exit(1)

    #debug_log(f"Configuración cargada: {parametros}", debug)
    return parametros

def marcar_archivo_en_bd(nombre_archivo, cursor, conexion, debug=False):
    # Marca un archivo como copiado en la base de datos local, actualizando los campos correspondientes
    try:
        sql = "UPDATE documentos SET copiado_remoto = 1, estado = 6, fecha_copia_remoto = NOW() WHERE nombre_archivo = %s"
        cursor.execute(sql, (nombre_archivo,))
        conexion.commit()
        if debug:
            print(f"[DEBUG] Marcado en BD como copiado: {nombre_archivo}")
        logging.info(f"Marcado en BD como copiado: {nombre_archivo}")
    except Exception as e:
        logging.error(f"Error marcando archivo en BD: {nombre_archivo}: {e}")
        if debug:
            print(f"[DEBUG] Error al marcar en BD: {e}")

def conectar_ssh_y_transport(config, debug=False):
    # Establece conexión SSH/SFTP con el servidor remoto usando los parámetros de configuración
    try:
        transport = paramiko.Transport((config['host'], config['port']))
        if config['password']:
            transport.connect(username=config['usuario'], password=config['password'])
        else:
            private_key = paramiko.RSAKey.from_private_key_file(config['key_filename'])
            transport.connect(username=config['usuario'], pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(config['host'], port=config['port'], username=config['usuario'], password=config['password'])
        return transport, sftp, ssh
    except Exception as e:
        print(f"ERROR: No se pudo conectar al servidor remoto: {e}")
        logging.error("ERROR conectando SSH/SFTP: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

def archivo_remoto_existe(sftp, remote_path):
    # Verifica si el archivo ya existe en el servidor remoto usando SFTP
    try:
        sftp.stat(remote_path)
        return True
    except FileNotFoundError:
        return False

def subir_archivo_sftp(sftp, local_path, remote_path, force=False, debug=False):
    # Sube un archivo al servidor remoto usando SFTP. Si force es False, no sobrescribe archivos existentes.
    if archivo_remoto_existe(sftp, remote_path) and not force:
        logging.warning(f"Ya existe: {remote_path}. Use --force para sobrescribir.")
        if debug:
            print(f"[DEBUG] Ya existe (omitido): {remote_path}")
        return -1  # Valor especial
    try:
        sftp.put(local_path, remote_path)
        tamano = os.path.getsize(local_path)
        logging.info(f"Archivo subido (SFTP): {os.path.basename(local_path)} ({tamano} bytes)")
        return tamano
    except Exception as e:
        logging.error(f"ERROR subiendo archivo SFTP {local_path}: {e}")
        return 0

def subir_archivo_scp(ssh, local_path, remote_path, force=False, debug=False):
    # Sube un archivo al servidor remoto usando SCP. Si force es False, no sobrescribe archivos existentes.
    try:
        if not force:
            sftp_check = ssh.open_sftp()
            try:
                sftp_check.stat(remote_path)
                logging.warning(f"Ya existe: {remote_path}. Use --force para sobrescribir.")
                return 0
            except FileNotFoundError:
                pass
            finally:
                sftp_check.close()

        with SCPClient(ssh.get_transport()) as scp:
            scp.put(local_path, remote_path)
        tamano = os.path.getsize(local_path)
        logging.info(f"Archivo subido (SCP): {os.path.basename(local_path)} ({tamano} bytes)")
        return tamano
    except Exception as e:
        logging.error(f"ERROR subiendo archivo SCP {local_path}: {e}")
        return 0

def main():
    # ============================ 
    # Flujo principal del script
    # ============================ 
    inicio = time.time()
    parser = argparse.ArgumentParser(description="Sube archivos PDF procesados a carpeta remota final.")
    parser.add_argument("--config", default="../config/config.cf", help="Ruta al archivo de configuración")
    parser.add_argument("--archivo", help="Nombre del archivo a subir")
    parser.add_argument("--allpdf", action="store_true", help="Subir todos los archivos PDF desde carpeta local")
    parser.add_argument("--debug", action="store_true", help="Habilitar debug")
    parser.add_argument("--force", action="store_true", help="Sobrescribir archivos existentes en destino")
    parser.add_argument("--modo", choices=["local", "remoto"], default="remoto", help="Modo de operación: local o remoto")
    args = parser.parse_args()

    if not args.archivo and not args.allpdf:
        print("Debe especificar --archivo o --allpdf")
        sys.exit(1)

    config = leer_configuracion(args.config, args.debug)

    if args.modo == "local":
        # Usamos la clave 'carpeta_procesados' del config para modo local.
        carpeta_procesados_local = config['carpeta_procesados']
        os.makedirs(carpeta_procesados_local, exist_ok=True)

    # Conexión a la base de datos local para obtener los archivos pendientes
    try:
        conexion = pymysql.connect(**DB_CONFIG)
        cursor = conexion.cursor()
    except Exception as e:
        logging.error(f"Error conectando a la base de datos: {e}\n{traceback.format_exc()}")
        sys.exit(1)

    if args.modo == "remoto":
        transport, sftp, ssh = conectar_ssh_y_transport(config, args.debug)

    total_archivos = 0
    total_tamano = 0

    archivos_locales = []
    # Itera sobre los archivos PDF pendientes y los mueve o copia según el modo
    if args.allpdf:
        try:
            cursor.execute("SELECT nombre_archivo FROM documentos WHERE estado = 5  and deleted_at is null order by id asc")
            resultados = cursor.fetchall()
            archivos_locales = [r["nombre_archivo"] for r in resultados]
            if args.debug:
                print(f"[DEBUG] Archivos pendientes de copia desde la base de datos: {archivos_locales}")
            logging.info(f"Archivos cargados desde la base de datos: {len(archivos_locales)}")
            # Verificación si no hay archivos pendientes
            if not archivos_locales:
                print("📭 No se encontraron archivos PDF pendientes para mover o copiar.")
                logging.info("No se encontraron archivos PDF pendientes para procesar en modo %s.", args.modo)
                cursor.close()
                conexion.close()
                return  # Finaliza normalmente sin error
        except Exception as e:
            logging.error(f"Error consultando la base de datos: {e}")
            sys.exit(1)
    else:
        archivos_locales = [args.archivo]

    # Bloque principal de procesamiento de archivos
    for archivo in archivos_locales:
        # Verifica la existencia del archivo local
        # Según el modo de operación, sube el archivo al servidor remoto o lo mueve localmente
        # Luego, si el movimiento fue exitoso, inserta el documento en la base remota y lo marca como copiado
        local_path = os.path.join(config['carpeta_procesados'], archivo)
        remote_path = os.path.join(config['directorio_remoto_leidos'], archivo)

        if not os.path.exists(local_path):
            logging.warning(f"Archivo no encontrado localmente: {local_path}")
            continue

        if args.modo == "remoto":
            if config['metodo_transferencia'].lower() == 'scp':
                tamano = subir_archivo_scp(ssh, local_path, remote_path, force=args.force, debug=args.debug)
            else:
                tamano = subir_archivo_sftp(sftp, local_path, remote_path, force=args.force, debug=args.debug)
        else:
            # Simula éxito y mueve el archivo localmente
            tamano = os.path.getsize(local_path)
            destino_local = os.path.join(carpeta_procesados_local, archivo)
            try:
                os.rename(local_path, destino_local)
                logging.info(f"Archivo movido localmente al directorio: {destino_local}")
            except Exception as e:
                logging.error(f"Error moviendo archivo local: {e}")
                tamano = 0

        if tamano > 0:
            if args.modo == "remoto":
                print(f"✔ Subido: {archivo} ({tamano} bytes)")
            else:
                print(f"✔ Movido localmente: {archivo} ({tamano} bytes)")
            total_archivos += 1
            total_tamano += tamano
            # --- Agregado: insertar en lectura_documentos_pdf ---
            try:
                cursor.execute("SELECT id FROM documentos WHERE nombre_archivo = %s", (archivo,))
                resultado_doc = cursor.fetchone()
                if resultado_doc:
                    try:
                        conexion_remota = pymysql.connect(**REMOTE_DB_CONFIG)
                        cursor_remoto = conexion_remota.cursor()
                        exito_insercion = insertar_en_lectura_documentos_pdf(resultado_doc["id"], cursor_remoto, conexion_remota, args.debug)
                        cursor_remoto.close()
                        conexion_remota.close()
                    except Exception as e:
                        logging.error(f"Error conectando a la base remota: {e}")
                        exito_insercion = False
                    if exito_insercion:
                        marcar_archivo_en_bd(archivo, cursor, conexion, args.debug)
                        try:
                            # os.remove(local_path)
                            logging.info(f"Archivo eliminado localmente: {local_path}")
                        except Exception as e:
                            logging.error(f"Error al eliminar archivo local {local_path}: {e}")
                    else:
                        print(f"✘ Inserción en lectura_documentos_pdf falló, archivo no se marcará como copiado: {archivo}")
                        logging.warning(f"Inserción en lectura_documentos_pdf falló: {archivo}")
            except Exception as e:
                logging.error(f"Error al insertar en lectura_documentos_pdf para archivo {archivo}: {e}")
            # --- Fin agregado ---
        elif tamano == -1:
            print(f"↪ Ya existe (omitido): {archivo}")
        else:
            if args.modo == "remoto":
                print(f"✘ Fallo al subir: {archivo}")
            else:
                print(f"✘ Fallo al mover localmente: {archivo}")

    duracion = time.time() - inicio
    print(f"Proceso completado en {duracion:.2f} segundos.")
    logging.info(f"Subida finalizada. Total archivos: {total_archivos}, Total bytes: {total_tamano}, Duración: {duracion:.2f} seg")

    if args.modo == "remoto":
        sftp.close()
        transport.close()
        ssh.close()
    cursor.close()
    conexion.close()

# --- Agregado: función insertar_en_lectura_documentos_pdf ---
def insertar_en_lectura_documentos_pdf(documento_id, cursor, conexion, debug=False):
    # Toma un documento consolidado y lo inserta en la tabla final lectura_documentos_pdf de la base remota
    try:
        db_local = pymysql.connect(**DB_CONFIG)
        cursor_local = db_local.cursor()
        # Obtener los datos base del documento y campos consolidados
        sql = """
            SELECT d.id, d.nombre_archivo, d.archivo_padre, d.tamaño_bytes, d.estado, d.created_at, c.campo, c.valor
            FROM documentos d
            INNER JOIN extraccion_campos_consolidada c ON c.documento_id = d.id
            WHERE d.id = %s
        """
        cursor_local.execute(sql, (documento_id,))
        resultados = cursor_local.fetchall()

        if debug:
            print(f"[DEBUG] Datos base obtenidos para documento_id={documento_id}: {resultados}")

        if not resultados:
            logging.warning(f"No se encontraron datos para documento_id={documento_id}")
            cursor_local.close()
            db_local.close()
            return False

        # Inicializar campos para inserción
        campos_dict = {row["campo"]: row["valor"] for row in resultados}
        documento_info = resultados[0]  # Datos base del documento

        # Corregir formato de fecha_documento si es necesario
        if 'fecha_documento' in campos_dict:
            try:
                fecha_original = campos_dict['fecha_documento']
                if isinstance(fecha_original, str) and "-" in fecha_original:
                    partes = fecha_original.strip().split("-")
                    if len(partes) == 3 and len(partes[2]) == 4:
                        # Asumimos formato DD-MM-YYYY
                        campos_dict['fecha_documento'] = f"{partes[2]}-{partes[1]}-{partes[0]}"
            except Exception as e:
                logging.warning(f"Error al convertir fecha_documento: {e}")

        columnas_tabla = [
            'documento_id', 'nombre_archivo', 'archivo_padre', 'tamaño_bytes', 'estado', 'fecha_insercion', 'metodo', 'archivo_origen',
            'tipo_doc', 'numero_documento', 'localidad', 'fecha_documento', 'nombre_proveedor', 'rut_proveedor',
            'nombre_comprador', 'rut_comprador', 'direccion_comprador', 'telefono_comprador', 'comuna_comprador', 'ciudad_comprador',
            'placa_patente', 'tipo_vehiculo', 'marca', 'modelo', 'n_motor', 'n_chasis', 'vin', 'serie', 'color', 'anio',
            'unidad_pbv', 'pbv', 'cit', 'combustible', 'unidad_carga', 'carga', 'asientos', 'puertas', 'unidad_potencia',
            'potencia_motor', 'ejes', 'traccion', 'tipo_carroceria', 'cilindrada', 'transmision',
            'monto_neto', 'monto_iva', 'monto_total', 'num_contrato'
        ]

        valores = [documento_info['id'], documento_info['nombre_archivo'], documento_info['archivo_padre'],
                   documento_info['tamaño_bytes'], documento_info['estado'], documento_info['created_at'], "script_pdf_copy",
                   documento_info['nombre_archivo']]  # metodo y archivo_origen

        # Agregar valores por campo, en orden
        for campo in columnas_tabla[8:]:
            valores.append(campos_dict.get(campo, None))

        placeholders = ', '.join(['%s'] * len(valores))
        columnas_str = ', '.join(columnas_tabla)
        insert_sql = f"INSERT INTO lectura_documentos_pdf ({columnas_str}) VALUES ({placeholders})"

        if debug:
            print(f"[DEBUG] Query SQL:\n{insert_sql}")
            #print(f"[DEBUG] Valores:\n{valores}")

        cursor.execute(insert_sql, valores)
        conexion.commit()

        logging.info(f"Insertado en lectura_documentos_pdf para documento_id={documento_id}")
        if debug:
            print(f"[DEBUG] Insertado en lectura_documentos_pdf: documento_id={documento_id}")
        cursor_local.close()
        db_local.close()
        return True
    except Exception as e:
        logging.error(f"Error insertando lectura_documentos_pdf: {e}\n{traceback.format_exc()}")
        if debug:
            print(f"[DEBUG] Error insertando en lectura_documentos_pdf: {e}")
        try:
            cursor_local.close()
            db_local.close()
        except:
            pass
        return False
# --- Fin agregado ---

if __name__ == "__main__":
    main()
