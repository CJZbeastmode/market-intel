#!/usr/bin/env sh
set -eu

# Build the pybind11 extension from ml/indicators.
# The compiled module is written to the repo root as indicators*.so.

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
    if [ -x ".venv/bin/python" ]; then
        PYTHON_BIN=".venv/bin/python"
    else
        PYTHON_BIN="python3"
    fi
fi

PYTHON_BIN="$("$PYTHON_BIN" -c 'import sys; print(sys.executable)')"

if ! "$PYTHON_BIN" -m pybind11 --cmakedir >/dev/null 2>&1; then
    echo "pybind11 is missing for $PYTHON_BIN" >&2
    echo "Install it with: $PYTHON_BIN -m pip install pybind11" >&2
    exit 1
fi

PYBIND11_CMAKE_DIR="$("$PYTHON_BIN" -m pybind11 --cmakedir)"
PY_TAG="$("$PYTHON_BIN" -c 'import sys; print(f"py{sys.version_info.major}{sys.version_info.minor}")')"
BUILD_DIR="ml/indicators/build-$PY_TAG"

cmake -S ml/indicators -B "$BUILD_DIR" \
    -DPython3_EXECUTABLE="$PYTHON_BIN" \
    -Dpybind11_DIR="$PYBIND11_CMAKE_DIR" \
    -DPYBIND11_FINDPYTHON=ON

cmake --build "$BUILD_DIR"

"$PYTHON_BIN" -c "import indicators; print(indicators.rsi([1, 2, 3, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 11, 13], 14)[-1])"
