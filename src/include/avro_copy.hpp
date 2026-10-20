#pragma once

#include "avro_common.hpp"

namespace duckdb {

namespace avro {

struct AvroCopyFunction {
	//! Registers the "avro" format of COPY ... TO
	static void Register(cxx::Extension &extension);
};

} // namespace avro

} // namespace duckdb
