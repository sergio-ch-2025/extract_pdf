#!/usr/bin/env bash
set -euo pipefail

# ==== Config ====
PROJECT_DIR="/website/extract_pdf"
PYTHON_BIN="python3"     # cámbialo si usas otra ruta

echo "=== Detectando distribución Linux ==="
if [[ -f /etc/os-release ]]; then
  . /etc/os-release
else
  echo "No se encontró /etc/os-release. Abortando."
  exit 1
fi
DISTRO=${ID:-unknown}
VERSION=${VERSION_ID:-unknown}
echo "Distro: ${NAME:-$DISTRO}  Versión: $VERSION"

# ---------- Helpers ----------
install_rocky_like() {
  echo ">> Instalando dependencias para Rocky/CentOS/RHEL/Fedora..."
  sudo dnf install -y epel-release || true
  # En RHEL-like 9 conviene habilitar CRB (para dependencias de compilación)
  if command -v dnf >/dev/null 2>&1; then
    sudo dnf config-manager --set-enabled crb || true
  fi
  sudo dnf install -y \
    ${PYTHON_BIN} ${PYTHON_BIN}-pip ${PYTHON_BIN}-devel \
    gcc gcc-c++ make \
    tesseract tesseract-langpack-spa \
    mesa-libGL \
    glib2 \
    libSM libXrender libXext \
    git curl
}

install_debian_like() {
  echo ">> Instalando dependencias para Debian/Ubuntu..."
  sudo apt-get update -y
  sudo apt-get install -y \
    ${PYTHON_BIN} ${PYTHON_BIN}-venv python3-dev \
    build-essential \
    tesseract-ocr tesseract-ocr-spa \
    libgl1 \
    libglib2.0-0 \
    libsm6 libxrender1 libxext6 \
    git curl
}

case "$DISTRO" in
  rocky|rhel|centos|fedora) install_rocky_like ;;
  ubuntu|debian)            install_debian_like ;;
  *) echo "Distribución $DISTRO no soportada automáticamente. Salgo."; exit 1 ;;
esac

echo "=== Preparando entorno virtual ==="
mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"
if [[ ! -d ".venv" ]]; then
  ${PYTHON_BIN} -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel

# ---------- Detección de GPU / CUDA ----------
echo "=== Detectando GPU NVIDIA y versión de CUDA (si existe) ==="
HAS_NVIDIA="no"
CUDA_VERSION=""
if command -v nvidia-smi >/dev/null 2>&1; then
  HAS_NVIDIA="yes"
  # Parseo robusto de versión CUDA desde nvidia-smi
  CUDA_VERSION=$(nvidia-smi | awk '/CUDA Version:/ {print $NF; exit}' | sed 's/,//g' || true)
  echo "Se detectó nvidia-smi. CUDA Runtime reportado: ${CUDA_VERSION:-desconocido}"
else
  echo "No se detectó nvidia-smi. Asumiré instalación en CPU."
fi

# Normaliza versión mayor.minor (12.4, 12.2, 12.1, 11.8)
CUDA_TAG=""
PADDLE_GPU_PKG=""  # se setea solo si hay GPU
if [[ "$HAS_NVIDIA" == "yes" && -n "$CUDA_VERSION" ]]; then
  # Tomar los primeros 2 componentes
  CUDA_MM=$(echo "$CUDA_VERSION" | awk -F. '{print $1"."$2}')
  case "$CUDA_MM" in
    12.4) CUDA_TAG="cu124"; PADDLE_GPU_PKG="paddlepaddle-gpu==2.6.1.post124" ;;
    12.2) CUDA_TAG="cu122"; PADDLE_GPU_PKG="paddlepaddle-gpu==2.6.1.post122" ;;
    12.1) CUDA_TAG="cu121"; PADDLE_GPU_PKG="paddlepaddle-gpu==2.6.1.post121" ;;
    11.8) CUDA_TAG="cu118"; PADDLE_GPU_PKG="paddlepaddle-gpu==2.6.1.post118" ;;
    *)    echo "CUDA ${CUDA_MM} no mapeada. Intentaré GPU con cu121; si falla, instalo CPU."
          CUDA_TAG="cu121"; PADDLE_GPU_PKG="paddlepaddle-gpu==2.6.1.post121"
          ;;
  esac
fi

# ---------- Instalar PyTorch + Paddle según GPU/CPU ----------
echo "=== Instalando PyTorch y Paddle (GPU si posible, sino CPU) ==="
TORCH_OK="no"
PADDLE_OK="no"

if [[ -n "$CUDA_TAG" ]]; then
  echo "Intentando instalación GPU (PyTorch $CUDA_TAG + $PADDLE_GPU_PKG)..."
  # PyTorch GPU
  pip install --index-url "https://download.pytorch.org/whl/${CUDA_TAG}" torch torchvision && TORCH_OK="yes" || TORCH_OK="no"
  # Paddle GPU
  if [[ "$TORCH_OK" == "yes" ]]; then
    # Wheels oficiales de Paddle GPU para muchaslinux; si no funciona, caemos a CPU
    pip install "$PADDLE_GPU_PKG" && PADDLE_OK="yes" || PADDLE_OK="no"
  else
    PADDLE_OK="no"
  fi

  if [[ "$TORCH_OK" != "yes" || "$PADDLE_OK" != "yes" ]]; then
    echo "⚠️  No se pudo completar instalación GPU. Continuo con versión CPU."
    # Limpieza opcional mínima de intentos fallidos
    pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision
    pip install paddlepaddle==2.6.1
  fi
else
  echo "Instalando versiones CPU..."
  pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision
  pip install paddlepaddle==2.6.1
fi

# ---------- Instalar requirements del proyecto ----------
echo "=== Instalando requirements del proyecto ==="
if [[ -f "requirements.txt" ]]; then
  pip install -r requirements.txt
else
  echo "No se encontró requirements.txt en $PROJECT_DIR. Creo uno temporal con tus dependencias base."
  cat > requirements.txt <<'EOF'
scp>=0.14.5
PyMuPDF
pytesseract
paddleocr
python-docx
pymysql
easyocr
opencv-python
pandas
EOF
  pip install -r requirements.txt
fi

# ---------- Ajuste Tesseract (opcional) ----------
if [[ -d /usr/share/tesseract/tessdata ]]; then
  export TESSDATA_PREFIX=/usr/share/tesseract/tessdata
elif [[ -d /usr/share/tesseract-ocr/4.00/tessdata ]]; then
  export TESSDATA_PREFIX=/usr/share/tesseract-ocr/4.00/tessdata
fi

# ---------- Verificación rápida ----------
echo "=== Verificando imports clave ==="
python - <<'PY'
import sys
def ok(pkg):
    try:
        __import__(pkg)
        print(f"{pkg} OK")
    except Exception as e:
        print(f"{pkg} FAIL -> {e}", file=sys.stderr)

ok("fitz")           # PyMuPDF
ok("pytesseract")
ok("cv2")
ok("pandas")
ok("paddleocr")
ok("easyocr")
ok("docx")          # python-docx
ok("pymysql")
PY

echo
echo "✅ Setup finalizado."
echo "• Activa el entorno:  source ${PROJECT_DIR}/.venv/bin/activate"
echo "• Tesseract:         $(command -v tesseract || echo 'no encontrado')"
echo "• CUDA detectada:    ${CUDA_VERSION:-'sin GPU / no detectada'}"
