####   ejecucion 
#   python3 test_scores.py --campo color --valor Azul
#   python3 test_scores.py --campo anio --valor 2025
# 
# # 
#=== Evaluación Manual ===
#Campo evaluado    : color
#Valor recibido     : Azul
#Score obtenido     : 1.00
#Validación aplicada: score_color
#Validación aplicada: score_color
#⚠️  Observación       : El score es bajo. Revisa la calidad o formato del valor evaluado.
#
import argparse
from configparser import ConfigParser
import os
from evaluador_score import evaluar_score

# Cargar configuración para obtener score mínimo
CONFIG = ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), '../config/config.cf'))
SCORE_MINIMO_ACEPTABLE = float(CONFIG.get('evaluacion', 'score_minimo_aceptable', fallback='0.4'))

def evaluar_interactivo(campo, valor):
    score = evaluar_score(campo, valor)

    print("=== Evaluación Manual ===")
    print(f"Campo evaluado    : {campo}")
    print(f"Valor recibido     : {valor}")
    print(f"Score obtenido     : {score:.2f}")
    print(f"Score mínimo aceptable: {SCORE_MINIMO_ACEPTABLE:.2f}")

    if score < SCORE_MINIMO_ACEPTABLE:
        print("⚠️  Observación       : El score es bajo. Revisa la calidad o formato del valor evaluado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluar manualmente un campo extraído y obtener su score.")
    parser.add_argument("--campo", required=True, help="Nombre del campo a evaluar (ej: marca, anio, etc.)")
    parser.add_argument("--valor", required=True, help="Valor a evaluar (ej: Toyota, 2024, etc.)")

    args = parser.parse_args()
    evaluar_interactivo(args.campo, args.valor)
