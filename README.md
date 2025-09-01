# Proyecto: Extracción de PDFs desde Servidor Remoto

## Estructura del Proyecto

```plaintext
/AI/ia_sch/extraccion_pdf_prod/
├── config/
│   └── config.cf
├── scripts/
│   ├── get_pdf_of_remote.py
│   ├── move_pdf_to_leidos.py
│   ├── process_pdf.py
│   ├── extract_text.py
│   ├── texts_parse_campos.py
│   ├── ver_logs.py
│   ├── ver_logs_dashboard.py
│   └── orquestador_pipeline.py
├── para_procesar/
├── procesados/
├── logs/
│   └── actividad.log
├── resultados/
├── diccionarios/
│   ├── Diccionario_colores.csv
│   ├── Diccionario_carrocerias.csv
│   ├── Diccionario_comunas.csv
│   └── Diccionario_ciudades.csv
├── tmp/
├── output/
├── requirements.txt
└── README.md
```

## Descripción General

Sistema de extracción, procesamiento y monitoreo de documentos PDF provenientes de un servidor remoto. Diseñado para ser modular, escalable y preparado para entornos de producción.

## Características Principales

- 🔍 Extracción de datos OCR usando múltiples motores: PaddleOCR, DocTR.
- 🔧 Modularidad completa mediante `config.cf`.
- 💾 Exportación de datos estructurados a CSV, JSON y base de datos.
- 📅 Control de logs profesional con autolimpieza por cantidad de días.
- 📈 Dashboard de monitoreo de actividad OCR en consola.
- 📧 Opcional: Envío automático de resumen diario por correo.

## Configuración

Toda la configuración del sistema está centralizada en `config/config.cf`, incluyendo:

- Rutas de entrada y salida.
- Activación/desactivación de motores OCR.
- Directorios de exportación de resultados.
- Configuración de conexión SMTP para correos.
- Definición de limpieza automática de logs.
- Configuración del delay de ejecución automática (`[pipeline] delay_pipeline`)

### Ejemplo de Configuración (`config.cf`)

```ini
[conexion]
usuario = root
host = 192.168.100.186
password = *****
port = 22

[paths]
directorio_local_para_procesar = ../para_procesar
carpeta_procesados = ../procesados

[logs]
archivo_log = ../logs/actividad.log
dias_log = 7
enviar_correo_automatico = false

[database]
host = localhost
user = sch_virtual
password = *****
dbname = extraccion_pdf

[ocr]
use_doctr = true
use_paddleocr = true
use_tesseract = false
use_easyocr = false

[extraccion]
ruta_diccionario_colores = ../diccionarios/Diccionario_colores.csv
ruta_diccionario_carrocerias = ../diccionarios/carrocerias.csv
ruta_diccionario_comunas = ../diccionarios/Diccionario_comunas.csv
ruta_diccionario_ciudades = ../diccionarios/Diccionario_ciudades.csv
directorio_salida_csv = ../resultados
directorio_salida_json = ../resultados
guardar_csv = true
guardar_json = true
guardar_bd = true

[email]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_user = tu_email@gmail.com
smtp_password = tu_password_o_token
email_destino = destino@empresa.com

[pipeline]
delay_pipeline = 5
```

## Flujo de Procesamiento

1. **Extracción de PDFs:**
   - Descarga los PDFs remotos (`get_pdf_of_remote.py`).
   - Mueve los PDFs procesados a `procesados/` (`move_pdf_to_leidos.py`).

2. **Ejecución automática del pipeline al detectar nuevos archivos PDF**
   El script `orquestador_pipeline.py` monitorea el directorio configurado en `[paths] directorio_remoto_origen` del archivo `config.cf`.

   - Se ejecuta automáticamente cuando se copian archivos `.pdf` a esa carpeta.
   - Espera una cantidad de segundos definida en `[pipeline] delay_pipeline` antes de iniciar el procesamiento (por defecto 5 segundos).
   - Si se detectan nuevos archivos durante ese tiempo, el temporizador se reinicia para agrupar los archivos y procesarlos en conjunto.

   **Ejecución manual del monitor:**
   ```bash
   python3 orquestador_pipeline.py
   ```

3. **Procesamiento de PDFs:**
   - Extrae el texto OCR desde los PDFs (`extract_text.py`).
   - Estructura los campos y genera archivos CSV y/o JSON (`texts_parse_campos.py`).

4. **Monitoreo de Actividad:**
   - Visualización simple (`ver_logs.py`).
   - Dashboard resumen diario (`ver_logs_dashboard.py`).
   - Opcional: Envío automático de resumen por correo.

## Consideraciones de Operación

- **Control de errores:** Todo error o advertencia queda registrado en `logs/actividad.log`.
- **Limpieza automática:** El log elimina entradas más antiguas que `dias_log` días.
- **Validación de configuración:** El sistema verifica que todas las rutas de configuración existan antes de procesar.
- **Correo opcional:** El resumen puede ser enviado automáticamente o preguntado al usuario.

## Requerimientos de Instalación

Instalar dependencias usando `pip`:

```bash
pip install -r requirements.txt
```

Contenido mínimo de `requirements.txt`:

```plaintext
PyMuPDF
pytesseract
paddleocr
python-docx
pymysql
easyocr
opencv-python
pandas
```

---

# Estado Final del Proyecto

Sistema de Extracción OCR profesional: 
- ✅ Robusto
- ✅ Modular
- ✅ Escalable
- ✅ Preparado para producción real

---

✨ Proyecto desarrollado para operaciones estables y escalables de extracción y monitoreo documental.

graph LR
    A[get_pdf_of_remote.py\n⬇️ Descarga PDF] --> 
    B[registrar_documentos.py\n  genera el registro en la BD con sus caracteristicas] -->
    C[extract_text.py\n🔍 OCR + Metadatos]
    B --> C[texts_parse_campos.py\n🧠 Extrae Campos]
    C --> D[evaluador_score.py\n📊 Asigna Score]
    D --> E[consolidar_por_score.py\n🏆 Selecciona Mejor Valor]
    E --> F[put_pdf_to_remote_ok.py\n🌐 Coloca los PDF nuevamente en el directorio procesados del server]

    F --> G[generar_vista_html.py\n🌐 Crea HTML (PDF + Campos)]
    H --> I[visor_web.py\n🖥️ Servidor HTML + PDF]


    ORDEN_CAMPOS = [
    'tipo_doc', 'numero_documento', 'localidad', 'fecha_documento',
    'rut_proveedor','nombre_proveedor', 
    'rut_comprador', 'nombre_comprador','direccion_comprador', 'telefono_comprador', 'comuna_comprador', 'ciudad_comprador',
    'placa_patente', 'tipo_vehiculo', 'marca', 'modelo', 'n_motor', 'n_chasis', 'vin',
    'serie', 'color', 'anio', 'unidad_pbv', 'pbv', 'cit', 'combustible', 'unidad_carga',
    'carga', 'asientos', 'puertas', 'unidad_potencia', 'potencia_motor', 'ejes', 'traccion',
    'tipo_carroceria', 'cilindrada', 'transmision', 'monto_neto', 'monto_iva', 'monto_total'
]



# 🧠 Pipeline de Extracción de Datos desde PDFs

Este proyecto permite descargar, registrar, procesar, extraer texto, analizar campos, evaluar y consolidar información desde documentos PDF mediante múltiples motores OCR e inteligencia artificial.

---

## 🔁 Flujo de Trabajo Completo

1. **Descarga de PDFs remotos**
   ```bash
   python3 1get_pdf_of_remote.py --allpdf --delete-remote
   ```

2. **Ejecución automática del pipeline al detectar nuevos archivos PDF**
   El script `orquestador_pipeline.py` monitorea el directorio configurado en `[paths] directorio_remoto_origen` del archivo `config.cf`.

   - Se ejecuta automáticamente cuando se copian archivos `.pdf` a esa carpeta.
   - Espera una cantidad de segundos definida en `[pipeline] delay_pipeline` antes de iniciar el procesamiento (por defecto 5 segundos).
   - Si se detectan nuevos archivos durante ese tiempo, el temporizador se reinicia para agrupar los archivos y procesarlos en conjunto.

   **Ejecución manual del monitor:**
   ```bash
   python3 orquestador_pipeline.py
   ```

3. **Registro de documentos descargados en base de datos**
   ```bash
   python3 2registrar_documentos.py
   ```

4. **Extracción de texto desde PDFs con OCR**
   ```bash
   python3 3extract_text.py
   ```

5. **Parseo de campos desde el texto OCR**
   ```bash
   python3 4texts_parse_campos.py
   ```

6. **Evaluación de calidad de los campos extraídos**
   ```bash
   python3 5evaluador_score.py
   ```

7. **Consolidación del campo con mayor score por documento**
   ```bash
   python3 6consolidar_por_score.py
   ```

8. **Envío de PDFs procesados exitosamente al servidor remoto**
   ```bash
   python3 7put_pdf_to_remote_ok.py --all
   ```

---

## 📄 Scripts y Funcionalidades

### `1get_pdf_of_remote.py`
Descarga archivos PDF desde un servidor remoto vía SFTP.

**Parámetros:**
- `--allpdf`: descarga todos los PDF.
- `--delete-remote`: elimina del servidor después de descargar.
- `--onlyfiles [nombres]`: descarga solo archivos específicos.

---

### `2registrar_documentos.py`
Registra los PDFs en base de datos (evita duplicados).

---

### `3extract_text.py`
Extrae texto desde los PDFs usando múltiples motores OCR (`PaddleOCR`, `Tesseract`, `EasyOCR`, `DocTR`, `PyMuPDF`).

Configurable mediante el archivo `config.cf`.

---

### `4texts_parse_campos.py`
Extrae campos estructurados desde el texto OCR usando expresiones regulares, etiquetas, heurísticas.

---

### `5evaluador_score.py`
Asigna un score de calidad a cada campo extraído para priorizar los resultados más confiables.

---

### `6consolidar_por_score.py`
Consolida los campos por documento seleccionando aquel con mayor score.

---

### `7put_pdf_to_remote_ok.py`
Sube los PDFs procesados a una carpeta remota “OK” vía SFTP y actualiza su estado.

**Parámetros:**
- `--all`: sube todos los PDFs procesados.

---

### `orquestador_pipeline.py`
Script orquestador que observa el directorio remoto configurado y ejecuta automáticamente todos los scripts de procesamiento cuando se detectan nuevos archivos PDF. Soporta retraso configurable (`delay_pipeline`) para agrupar múltiples archivos antes de procesarlos.
---

## ⚙️ Requisitos
- Python 3.8+
- MySQL/MariaDB
- Librerías: `paramiko`, `pymysql`, `opencv-python`, `PaddleOCR`, `PyMuPDF`, `Tesseract`, etc.
- Archivo de configuración `config.cf` con credenciales y rutas.

---

## 📂 Estructura Esperada

```
/pdf_por_procesar/     # PDFs descargados
/pdf_procesados/       # PDFs ya tratados
/scripts/              # Scripts de procesamiento
/config/config.cf      # Configuración de conexión y opciones OCR
```

---

## 🧠 Recomendaciones
- Asegura que los OCR estén correctamente instalados (PaddleOCR, Tesseract, etc).
- Monitorea los logs para detectar errores de extracción o conexión.
- Reentrena los modelos de IA si deseas mayor precisión.
