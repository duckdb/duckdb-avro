#include "avro_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "include/avro_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "avro_multi_file_info.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "avro_copy.hpp"

#include <avro.h>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto table_function = MultiFileFunction<AvroMultiFileInfo>("read_avro");
	table_function.projection_pushdown = true;
	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(table_function));
	loader.RegisterFunction(AvroCopyFunction::Create());
}

void AvroExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string AvroExtension::Name() {
	return "avro";
}

std::string AvroExtension::Version() const {
#ifdef EXT_VERSION_AVRO
	return EXT_VERSION_AVRO;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(avro, loader) {
	duckdb::LoadInternal(loader);
}
}
