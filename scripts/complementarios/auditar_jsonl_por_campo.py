import json
import sys

def auditar_jsonl_por_campo(archivo_jsonl, campo_objetivo, imprimir_ejemplos=False, ejemplos_max=5):
    total = 0
    vacios = 0
    ejemplos = []
    with open(archivo_jsonl, encoding="utf-8") as f:
        for linea in f:
            total += 1
            data = json.loads(linea)
            response = json.loads(data["response"])
            valor = response.get(campo_objetivo, "")
            if not valor:
                vacios += 1
                if imprimir_ejemplos and len(ejemplos) < ejemplos_max:
                    ejemplos.append({
                        "OCR": data.get("prompt", "")[:400],  # Solo primeros 400 chars para no saturar
                        "response": response
                    })
    print(f"Total registros: {total}")
    print(f"Registros con '{campo_objetivo}' vacío: {vacios} ({(vacios/total)*100:.1f}%)")
    print(f"Registros con '{campo_objetivo}' presente: {total - vacios} ({((total - vacios)/total)*100:.1f}%)")
    if imprimir_ejemplos and ejemplos:
        print(f"\nEjemplos donde '{campo_objetivo}' está vacío:")
        for ej in ejemplos:
            print("-" * 40)
            print("OCR:")
            print(ej["OCR"])
            print("Response:")
            print(json.dumps(ej["response"], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    archivo = "llama_factura_prompts_train_v3_normalizado.jsonl"
    campo = "numero_documento"  # <--- Cambia aquí el campo que quieres auditar
    # Para ver ejemplos, pon True en el tercer argumento
    auditar_jsonl_por_campo(archivo, campo, imprimir_ejemplos=True)