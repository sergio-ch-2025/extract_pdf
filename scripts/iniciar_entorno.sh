#!/bin/bash

# Listado de bibliotecas CUDA críticas
cuda_libs=(
  libcublas.so
  libcudart.so
  libcusparse.so
  libcurand.so
  libcufft.so
  libnvToolsExt.so
  libcupti.so
  libcupti.so.12
  libnccl.so
  libnvJitLink.so
)

	      # Posibles rutas de búsqueda
	      search_paths=(
		        /usr/local/cuda/lib64
			  /usr/local/cuda-11.8/lib64
			    /usr/local/cuda-11.8/extras/CUPTI/lib64
			      /usr/local/lib
			        /usr/lib
			)

			# Ruta destino común
			dest_lib_path=/usr/lib64

			echo "🔍 Iniciando revisión de bibliotecas CUDA..."

for lib in "${cuda_libs[@]}"; do
  found=""
  for path in "${search_paths[@]}"; do
    candidate=$(find "$path" -name "$lib*" -type f 2>/dev/null | head -n 1)
    if [[ -n "$candidate" ]]; then
      found="$candidate"
      break
    fi
  done

  if [[ -z "$found" ]]; then
    echo "❌ No se encontró $lib en ningún path conocido"
  else
    base_symlink="$dest_lib_path/$lib"
    # Eliminar symlink roto si existe
    if [[ -L "$base_symlink" && ! -e "$base_symlink" ]]; then
      echo "🔁 Symlink roto detectado en $base_symlink. Eliminando..."
      rm -f "$base_symlink"
    fi
    if [[ ! -e "$base_symlink" ]]; then
      echo "🔗 Creando symlink para $lib → $found"
      ln -s "$found" "$base_symlink"
    else
      echo "✅ $lib ya disponible en $base_symlink"
    fi

    # Si el archivo encontrado termina en .so.NN o .so.NN.X, crear también ese symlink
    if [[ "$lib" == libcupti.so.12 ]]; then
      version_symlink="$dest_lib_path/libcupti.so.12"
      # Eliminar symlink si apunta a un archivo inexistente
      if [[ -L "$version_symlink" && ! -e "$version_symlink" ]]; then
        echo "🔁 Symlink roto: $version_symlink. Eliminando..."
        rm -f "$version_symlink"
      fi
      # Crear symlink si no existe o fue eliminado
      if [[ ! -e "$version_symlink" ]]; then
        echo "🔗 (Re)Creando symlink para libcupti.so.12 → $found"
        ln -s "$found" "$version_symlink"
      else
        echo "✅ libcupti.so.12 ya disponible en $version_symlink"
      fi
    elif [[ "$found" =~ \.so\.([0-9]+)(\.[0-9]+)*$ ]]; then
      version_symlink="$dest_lib_path/$(basename "$found")"
      if [[ ! -e "$version_symlink" ]]; then
        echo "🔗 Creando symlink para $(basename "$found") → $found"
        ln -s "$found" "$version_symlink"
      else
        echo "✅ $(basename "$found") ya disponible en $version_symlink"
      fi
    fi
  fi
done

# Symlink adicional requerido por Torch
torch_expected_path="/lib64/libcupti.so.12"
real_path="/usr/local/cuda-12.2/targets/x86_64-linux/lib/libcupti.so.12"

if [[ ! -e "$real_path" ]]; then
  echo "❌ No se encontró la biblioteca real en $real_path. Verifica instalación CUDA."
else
  if [[ -L "$torch_expected_path" || -e "$torch_expected_path" ]]; then
    echo "🔁 Eliminando symlink o archivo existente en $torch_expected_path"
    rm -f "$torch_expected_path"
  fi
  echo "🔗 Creando symlink: $torch_expected_path → $real_path"
  ln -s "$real_path" "$torch_expected_path"
fi


# Forzar symlink de libcupti.so.12 al que contiene los símbolos correctos para Torch
fixed_cupti_path="/usr/local/lib/python3.9/site-packages/nvidia/cuda_cupti/lib/libcupti.so.12"
if [[ -e "$fixed_cupti_path" ]]; then
  echo "🔁 Reajustando symlink de libcupti.so.12 a versión válida de NVIDIA Python package..."
  rm -f /usr/lib64/libcupti.so.12
  ln -s "$fixed_cupti_path" /usr/lib64/libcupti.so.12

  rm -f /lib64/libcupti.so.12
  ln -s "$fixed_cupti_path" /lib64/libcupti.so.12
  echo "✅ Symlinks de libcupti.so.12 actualizados correctamente."
else
  echo "⚠️  No se encontró $fixed_cupti_path, no se actualizaron los symlinks para libcupti.so.12"
fi

echo "✅ Proceso finalizado. "
echo "🔧 Configurado entorno CUDA en el script Python..."