#!/usr/bin/env bash
set -euo pipefail

VENV=".venv"
PY="python3.12"

if ! command -v "$PY" >/dev/null 2>&1; then
  if command -v python3.13 >/dev/null 2>&1; then
    echo "Python 3.12 não encontrado; usando Python 3.13."
    PY="python3.13"
  else
    echo "Python 3.12/3.13 não encontrados. Instale um deles e tente novamente." >&2
    exit 1
  fi
fi

if [ ! -d "$VENV" ]; then
  echo "Criando virtualenv em $VENV com $PY..."
  if ! "$PY" -m venv "$VENV" 2>/dev/null; then
    echo "ensurepip ausente; tentando com virtualenv..."
    if ! command -v virtualenv >/dev/null 2>&1; then
      echo "virtualenv não encontrado. Instale-o com: pip install --user virtualenv" >&2
      exit 1
    fi
    virtualenv -p "$PY" "$VENV"
  fi
  "$VENV/bin/python" -m pip install --upgrade pip
fi

# Sempre garantir dependências atualizadas para a versão do Python em uso
"$VENV/bin/pip" install -r requirements.txt

# Isola de pacotes do usuário (~/.local) e garante o Python do venv
export PYTHONNOUSERSITE=1
export PATH="$(pwd)/$VENV/bin:$PATH"

exec "$VENV/bin/python" -m streamlit run app.py
