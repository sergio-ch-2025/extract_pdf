import os
import sys
import configparser
import logging
from collections import defaultdict
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def cargar_configuracion():
    config = configparser.ConfigParser()
    config.read('../config/config.cf')
    archivo_log = config.get('logs', 'archivo_log', fallback='../logs/actividad.log')
    dias_log = config.getint('logs', 'dias_log', fallback=7)
    enviar_correo_auto = config.getboolean('logs', 'enviar_correo_automatico', fallback=False)

    smtp_host = config.get('email', 'smtp_host')
    smtp_port = config.getint('email', 'smtp_port')
    smtp_user = config.get('email', 'smtp_user')
    smtp_password = config.get('email', 'smtp_password')
    email_destino = config.get('email', 'email_destino')

    return archivo_log, dias_log, smtp_host, smtp_port, smtp_user, smtp_password, email_destino, enviar_correo_auto

def limpiar_logs_antiguos(archivo_log, dias_log):
    if not os.path.isfile(archivo_log):
        return

    hoy = datetime.now()
    nuevas_lineas = []

    with open(archivo_log, "r", encoding="utf-8") as f:
        for linea in f:
            try:
                fecha_str = linea.split(' ')[0]
                fecha_linea = datetime.strptime(fecha_str, "%Y-%m-%d")
                if hoy - fecha_linea <= timedelta(days=dias_log):
                    nuevas_lineas.append(linea)
            except Exception:
                nuevas_lineas.append(linea)

    with open(archivo_log, "w", encoding="utf-8") as f:
        f.writelines(nuevas_lineas)

def analizar_log(archivo_log, fecha_filtro=None):
    if not os.path.isfile(archivo_log):
        print(f"[ERROR] No se encontró el archivo de log: {archivo_log}")
        sys.exit(1)

    resumen = defaultdict(lambda: {"INFO": 0, "WARNING": 0, "ERROR": 0})

    with open(archivo_log, "r", encoding="utf-8") as f:
        for linea in f:
            try:
                fecha = linea.split(' ')[0]
                nivel = linea.split('|')[1].strip()
                if fecha_filtro and fecha != fecha_filtro:
                    continue
                if nivel in resumen[fecha]:
                    resumen[fecha][nivel] += 1
            except Exception:
                continue

    return resumen

def mostrar_resumen(resumen):
    if not resumen:
        print("\nNo se encontraron registros para la fecha especificada.")
        return

    print("\nResumen de actividad por día:\n")
    print(f"{'Fecha':<12} | {'INFO':<5} | {'WARNING':<7} | {'ERROR':<5}")
    print("-" * 36)
    for fecha in sorted(resumen.keys()):
        info = resumen[fecha]['INFO']
        warning = resumen[fecha]['WARNING']
        error = resumen[fecha]['ERROR']
        print(f"{fecha:<12} | {info:<5} | {warning:<7} | {error:<5}")
    print("-" * 36)

def generar_cuerpo_resumen(resumen):
    cuerpo = "Resumen de actividad OCR:\n\n"
    cuerpo += f"{'Fecha':<12} | {'INFO':<5} | {'WARNING':<7} | {'ERROR':<5}\n"
    cuerpo += "-" * 36 + "\n"
    for fecha in sorted(resumen.keys()):
        info = resumen[fecha]['INFO']
        warning = resumen[fecha]['WARNING']
        error = resumen[fecha]['ERROR']
        cuerpo += f"{fecha:<12} | {info:<5} | {warning:<7} | {error:<5}\n"
    cuerpo += "-" * 36
    return cuerpo

def enviar_correo(asunto, cuerpo, smtp_host, smtp_port, smtp_user, smtp_password, email_destino):
    # Verificar configuración mínima antes de enviar
    if not smtp_host or not smtp_port or not smtp_user or not smtp_password or not email_destino:
        logging.warning("[WARNING] Configuración de correo incompleta. No se envía el resumen.")
        print("[WARNING] Configuración de correo incompleta. No se envía el resumen.")
        return

    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = email_destino
    msg['Subject'] = asunto

    msg.attach(MIMEText(cuerpo, 'plain'))

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        print(f"Correo enviado exitosamente a {email_destino}.")
        logging.info(f"Correo enviado exitosamente a {email_destino}.")
    except Exception as e:
        logging.error(f"Error enviando correo: {e}")
        print(f"[ERROR] No se pudo enviar el correo: {e}")

def main():
    archivo_log, dias_log, smtp_host, smtp_port, smtp_user, smtp_password, email_destino, enviar_correo_auto = cargar_configuracion()

    limpiar_logs_antiguos(archivo_log, dias_log)

    fecha_filtro = input("¿Deseas filtrar por una fecha específica (YYYY-MM-DD)? (Enter para no filtrar): ").strip()
    if fecha_filtro == "":
        fecha_filtro = None

    resumen = analizar_log(archivo_log, fecha_filtro)
    mostrar_resumen(resumen)

    if resumen:
        if enviar_correo_auto:
            cuerpo = generar_cuerpo_resumen(resumen)
            enviar_correo("Resumen Diario de Procesamiento OCR", cuerpo, smtp_host, smtp_port, smtp_user, smtp_password, email_destino)
        else:
            enviar = input("\n¿Deseas enviar este resumen por correo? (s/n): ").strip().lower()
            if enviar == 's':
                cuerpo = generar_cuerpo_resumen(resumen)
                enviar_correo("Resumen Diario de Procesamiento OCR", cuerpo, smtp_host, smtp_port, smtp_user, smtp_password, email_destino)

if __name__ == "__main__":
    main()