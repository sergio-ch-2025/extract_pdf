# Manual de Instalación Rápida

Este documento explica cómo preparar el entorno para ejecutar el sistema de Extracción de PDFs y Procesamiento OCR.

---

## 1. Requisitos del Sistema

- Python 3.8 o superior
- Acceso a Internet (para descarga de modelos OCR si es necesario)
- Acceso al servidor de almacenamiento de PDFs remoto (SSH)

---

## 2. Preparar entorno virtual (opcional, recomendado)

```bash
python3 -m venv venv
source venv/bin/activate
```

---

## 3. Instalación de dependencias

Instalar los paquetes necesarios:

```bash
pip install -r requirements.txt
```

Si paddleocr no instala automáticamente sus dependencias adicionales, ejecuta:

```bash
pip install paddlepaddle
```

(En Linux, puede ser necesario instalar también `poppler-utils` para soporte de PDF si usas pdf2image o pdftotext).

---

## 4. Configuración inicial

Editar el archivo de configuración:

```bash
nano config/config.cf
```

Y ajustar:

- Rutas de entrada y salida de archivos
- Configuración de conexión SSH
- Parámetros de conexión a Base de Datos
- Parámetros de conexión SMTP para envío de correos (opcional)

---

## 5. Flujo básico de trabajo

- Descargar PDFs remotos: 
  ```bash
  python3 scripts/get_pdf_of_remote.py
  ```
- Procesar PDFs y extraer texto:
  ```bash
  python3 scripts/process_pdf.py
  ```
- Extraer campos estructurados:
  ```bash
  python3 scripts/texts_parse_campos.py
  ```

---

## 6. Monitoreo de Actividad

Ver logs:

```bash
python3 scripts/ver_logs.py
```

Dashboard resumen:

```bash
python3 scripts/ver_logs_dashboard.py
```

(Con opción de enviar resumen por correo).

---

## 7. Notas Finales

- Asegúrate de tener correctamente configurado el `config.cf`.
- Mantén el archivo de logs bajo control para evitar crecimiento excesivo.
- Verifica periódicamente que los modelos OCR estén actualizados si es necesario.

---

✨ Listo para producción en pocos minutos.
