#pragma once

#include "avro_common.hpp"

namespace duckdb {

namespace avro {

struct AvroMetadata {
	//! Registers "avro_metadata", which lists the key-value metadata of a file
	static void Register(cxx::Extension &extension, cxx::Context &context);
};

} // namespace avro

} // namespace duckdb
