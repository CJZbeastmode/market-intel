cmake_minimum_required(VERSION 3.18)
project(market_intel_indicators LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(indicators bindings.cpp)
target_compile_options(indicators PRIVATE -O3)

# Put the compiled extension in the repo root so `python -c "import indicators"` works.
set_target_properties(indicators PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../.."
)
