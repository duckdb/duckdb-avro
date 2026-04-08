cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME avro)

if (EMSCRIPTEN)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
  
  # _LINKED_LIBS influences only Wasm compilation
  # it's unclear why this is needed, but somehow global symbols aren't properly exported otherwise
  # this solves by basically re-linking (at the moment the Wasm binary is actually produced)
  set (DUCKDB_EXTENSION_AVRO_LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/lib*.a")
endif()

# Disable avro-c examples, tests, docs
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_DOCS OFF CACHE BOOL "" FORCE)

# Add avro-c from submodule
add_subdirectory(third_party/avro-c/lang/c EXCLUDE_FROM_ALL)

# Get avro-c include directory
set(AVRO_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/avro-c/lang/c/src")

# Disable all UBSan checks for avro-static to work around multiple UB issues in avro-c
if(TARGET avro-static)
  target_compile_options(avro-static PRIVATE -fno-sanitize=undefined)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/avro_extension.cpp src/avro_reader.cpp src/avro_copy.cpp
    src/field_ids.cpp src/avro_multi_file_info.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_include_directories(${EXTENSION_NAME} PRIVATE ${AVRO_INCLUDE_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${AVRO_INCLUDE_DIR})

# Link against avro-static - use plain signature to match DuckDB's build system
target_link_libraries(${EXTENSION_NAME} avro-static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} avro-static)

# Mark avro-static as part of the export set
install(
  TARGETS ${EXTENSION_NAME} avro-static
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")