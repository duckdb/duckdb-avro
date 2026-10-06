#include "duckdb_cpp_extension.hpp"

#include "avro_copy.hpp"
#include "avro_metadata.hpp"
#include "avro_reader.hpp"

DUCKDB_CPP_EXTENSION_ENTRYPOINT(duckdb::cxx::Extension &extension, duckdb::cxx::Context &context) {
	duckdb::avro::AvroReader::Register(extension, context);
	duckdb::avro::AvroMetadata::Register(extension, context);
	duckdb::avro::AvroCopyFunction::Register(extension);
}
