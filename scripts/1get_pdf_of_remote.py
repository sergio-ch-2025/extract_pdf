#!/usr/bin/env python3
# ESTE ARCHIVO SE EJECUTA AL INICIO DEL PROCESO PARA IR ABASTECIENDO LOS ARCHIVOS DESDE EL SERVER DE ORIGEN AL SERVIDOR DE PROCESAMIENTO DE LA LECTURA DEL PDF

import paramiko
import scp
import os
import sys
import argparse
import configparser
import hashlib
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

def debug_log(message, debug):
    if debug:
        print("[DEBUG]", message)
    logging.debug(message)

def leer_configuracion(config_file, debug=False):
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        print(f"ERROR: Archivo de configuración no encontrado: {config_file}")
        sys.exit(1)

    config.read(config_file)

    if 'conexion' not in config or 'paths' not in config:
        print("ERROR: Archivo de configuración inválido. Faltan secciones [conexion] o [paths].")
        sys.exit(1)

    try:
        parametros = {
            'usuario': config['conexion']['usuario'],
            'host': config['conexion']['host'],
            'password': config['conexion'].get('password', None),
            'port': config['conexion'].getint('port', 22),
            'key_filename': config['conexion'].get('key_filename', None),
            'local_dir': config['paths']['directorio_local_para_procesar'],
            'remote_dir': config['paths']['directorio_remoto_origen'],
            'log_file': config['logs'].get('archivo_log', '../logs/actividad.log')
        }
    except KeyError as e:
        print(f"ERROR: Falta clave en configuración: {e}")
        sys.exit(1)

    if debug:
        print("[DEBUG] Configuración cargada:", parametros)

    return parametros

def conectar_ssh(config, debug=False):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        debug_log(f"Conectando a {config['host']}:{config['port']} como {config['usuario']}...", debug)

        connect_params = {
            "hostname": config['host'],
            "port": config['port'],
            "username": config['usuario'],
            "timeout": 30,
            "look_for_keys": False,
            "allow_agent": False
        }

        if config['password']:
            connect_params['password'] = config['password']

        if config['key_filename']:
            connect_params['key_filename'] = config['key_filename']

        ssh.connect(**connect_params)
        return ssh

    except Exception as e:
        logging.error(f"ERROR conexión SSH: {e}")
        print(f"ERROR: No se pudo conectar al servidor SSH: {e}")
        sys.exit(1)

def listar_pdfs_remotos(config, debug=False):
    ssh = conectar_ssh(config, debug)
    remote_dir = config['remote_dir']
    cmd = f"ls {remote_dir}/*.pdf"
    debug_log(f"Listando PDFs con comando: {cmd}", debug)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    salida = stdout.read().decode().splitlines()
    error = stderr.read().decode()
    ssh.close()

    if error:
        print(f"ERROR listando archivos PDF: {error}")
        return []

    return salida

def calcular_md5_local(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def calcular_md5_remoto(ssh, remote_path):
    cmd = f"md5sum '{remote_path}' | awk '{{print $1}}'"
    stdin, stdout, stderr = ssh.exec_command(cmd)
    return stdout.read().decode().strip()

def eliminar_archivo_remoto(ssh, remote_path, debug=False):
    cmd = f"rm -f '{remote_path}'"
    debug_log(f"Eliminando archivo remoto: {cmd}", debug)
    ssh.exec_command(cmd)

def descargar_y_validar(config, remote_path, debug=False, force=False, delete_remote=False):
    # Cada hilo abre y cierra su propia conexión SSH
    ssh = conectar_ssh(config, debug)
    local_file = os.path.join(config['local_dir'], os.path.basename(remote_path))

    if os.path.exists(local_file) and not force:
        motivo = f"SKIP {os.path.basename(remote_path)} (ya existe)"
        logging.info(motivo)
        ssh.close()
        return motivo

    try:
        with scp.SCPClient(ssh.get_transport()) as scp_client:
            debug_log(f"Descargando {remote_path}...", debug)
            scp_client.get(remote_path, local_path=config['local_dir'])

        md5_local = calcular_md5_local(local_file)
        md5_remoto = calcular_md5_remoto(ssh, remote_path)

        if md5_local == md5_remoto:
            if delete_remote:
                eliminar_archivo_remoto(ssh, remote_path, debug)
            ssh.close()
            return f"OK {os.path.basename(remote_path)}"
        else:
            ssh.close()
            return f"MD5 MISMATCH {os.path.basename(remote_path)}"

    except Exception as e:
        ssh.close()
        return f"ERROR descargando {os.path.basename(remote_path)}: {e}"

def main():
    parser = argparse.ArgumentParser(description="Descargar archivos PDF con control de integridad.")
    parser.add_argument("--config", default="../config/config.cf", help="Ruta al archivo de configuración")
    parser.add_argument("--archivo", help="Nombre de un archivo específico a descargar")
    parser.add_argument("--allpdf", action="store_true", help="Descargar todos los PDFs del directorio remoto")
    parser.add_argument("--force", action="store_true", help="Forzar descarga aunque el archivo ya exista localmente")
    parser.add_argument("--delete-remote", action="store_true", help="Eliminar el archivo remoto si se descarga correctamente")
    parser.add_argument("--debug", action="store_true", help="Habilitar debug")
    parser.add_argument("--threads", type=int, default=4, help="Número de descargas paralelas")
    args = parser.parse_args()

    config = leer_configuracion(args.config, args.debug)
    logging.basicConfig(filename=config['log_file'], level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if not args.archivo and not args.allpdf:
        print("ERROR: Debes especificar --archivo nombre.pdf o --allpdf")
        sys.exit(1)

    if not os.path.exists(config['local_dir']):
        os.makedirs(config['local_dir'])

    start_time = time.time()

    try:
        resultados = []

        if args.allpdf:
            archivos = listar_pdfs_remotos(config, args.debug)
            if not archivos:
                print("No se encontraron PDFs en el servidor remoto.")
                sys.exit(0)

            with ThreadPoolExecutor(max_workers=args.threads) as executor:
                futures = [
                    executor.submit(
                        descargar_y_validar, config, remote_path, args.debug, args.force, args.delete_remote
                    ) for remote_path in archivos
                ]
                resultados = [f.result() for f in futures]

        else:
            remote_path = os.path.join(config['remote_dir'], args.archivo)
            resultado = descargar_y_validar(config, remote_path, args.debug, args.force, args.delete_remote)
            resultados.append(resultado)

        print("\nResumen de descargas:")
        for r in resultados:
            print(" -", r)

        # Contador y resumen
        stats = Counter()
        for r in resultados:
            if r.startswith("OK"):
                stats['ok'] += 1
            elif r.startswith("SKIP"):
                stats['skip'] += 1
            elif r.startswith("MD5"):
                stats['md5'] += 1
            else:
                stats['error'] += 1

        print(f"\n📦 Archivos procesados: {len(resultados)}")
        print(f"✔️  OK: {stats['ok']} | 🔁 SKIP: {stats['skip']} | ❌ MD5 Error: {stats['md5']} | 🚫 Errores: {stats['error']}")
        logging.info(f"Descarga finalizada - OK: {stats['ok']}, SKIP: {stats['skip']}, MD5 FAIL: {stats['md5']}, Errores: {stats['error']}")

    except Exception as e:
        logging.error(f"ERROR general en el proceso: {e}")
        print(f"ERROR general en el proceso: {e}")
        sys.exit(1)

    duracion = round(time.time() - start_time, 2)
    print(f"\n⏱️ Tiempo total: {duracion} segundos")
    logging.info(f"Proceso completo en {duracion} segundos.")

if __name__ == "__main__":
    main()