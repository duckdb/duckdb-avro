cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME avro)

if (EMSCRIPTEN)
  # _LINKED_LIBS influences only Wasm compilation
  # it's unclear why this is needed, but somehow global symbols aren't properly exported otherwise
  # this solves by basically re-linking (at the moment the Wasm binary is actually produced)
  set (DUCKDB_EXTENSION_AVRO_LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/lib*.a")
endif()

find_path(
  AVRO_INCLUDE_DIR
  NAMES avro.h
  PATHS "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include"
  PATH_SUFFIXES avro REQUIRED)

if(MSVC) # endless screaming
  find_library(AVRO_LIBRARY avro.lib REQUIRED)
  find_library(JANSSON_LIBRARY jansson.lib REQUIRED)
  find_library(LZMA_LIBRARY lzma.lib REQUIRED)
  find_library(ZLIB_LIBRARY zlib.lib REQUIRED)
else()
  find_library(AVRO_LIBRARY libavro.a REQUIRED)
  find_library(JANSSON_LIBRARY libjansson.a REQUIRED)
  find_library(LZMA_LIBRARY liblzma.a REQUIRED)
  find_library(ZLIB_LIBRARY libz.a REQUIRED)
endif()

find_library(SNAPPY_LIBRARY snappy REQUIRED)
set(ALL_AVRO_LIBRARIES
    ${AVRO_LIBRARY}
    ${JEMALLOC_LIBRARY}
    ${JANSSON_LIBRARY}
    ${LZMA_LIBRARY}
    ${ZLIB_LIBRARY}
    ${SNAPPY_LIBRARY}
    ${GMP_LIBRARY}
    ${MATH_LIBRARY})

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
target_include_directories(${LOADABLE_EXTENSION_NAME}
                           PRIVATE ${AVRO_INCLUDE_DIR})
target_link_libraries(${EXTENSION_NAME} ${ALL_AVRO_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${ALL_AVRO_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
