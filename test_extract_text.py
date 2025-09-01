import os
import json
import numpy as np
from pdf2image import convert_from_path
from paddleocr import PaddleOCR
from datetime import datetime

# Configuración
CARPETA_PDFS = "Facturas"
DPI = 300

# Asegurarse de que el directorio "resultados" exista para el archivo de salida
directorio_resultados = "resultados"
if not os.path.exists(directorio_resultados):
    os.makedirs(directorio_resultados)


# Inicializa PaddleOCR en español
ocr = PaddleOCR(use_angle_cls=True, lang='es', use_gpu=False)

# Datos acumulados
datos = []

# Itera sobre los archivos PDF en la carpeta Facturas
for archivo in os.listdir(CARPETA_PDFS):
    if archivo.lower().endswith(".pdf"):
        ruta_pdf = os.path.join(CARPETA_PDFS, archivo)
        try:
            # Convertir PDF a imágenes
            imagenes = convert_from_path(ruta_pdf, dpi=300)
            texto_completo = ""
            # Procesar cada página del PDF
            for imagen in imagenes:
                # Convertir la imagen a un array de numpy, ya que PaddleOCR lo requiere
                imagen_np = np.array(imagen)
                resultado = ocr.ocr(imagen_np, cls=True)
                # Concatenar el texto extraído de cada línea
                for linea in resultado[0]:
                    texto_completo += linea[1][0] + "\n"
        except Exception as e:
            texto_completo = f"Error al procesar el archivo: {str(e)}"
        
        # Agregar el nombre del archivo y el texto extraído a la lista de datos
        datos.append({
            "archivo": archivo,
            "texto": texto_completo.strip()
        })

# Crear nombre de archivo de salida con fecha y hora
fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
nombre_archivo_salida = f"ocr_datos_{fecha_hora}.json"
ruta_salida = os.path.join(directorio_resultados, nombre_archivo_salida)

# Guardar los datos en un archivo JSON en el directorio "resultados"
with open(ruta_salida, "w", encoding="utf-8") as f:
    json.dump(datos, f, ensure_ascii=False, indent=4)

print(f"Extracción completada. Datos guardados en {ruta_salida}")