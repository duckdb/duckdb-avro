#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string.hpp"
#include <avro.h>

namespace duckdb {

struct AvroMetadataBindData : public TableFunctionData {
	string file_path;
	OpenFileInfo file_info;
};

struct AvroMetadataGlobalState : public GlobalTableFunctionState {
	AvroMetadataGlobalState() : offset(0), metadata_count(0), reader(nullptr) {
	}
	~AvroMetadataGlobalState() {
		if (reader) {
			avro_file_reader_close(reader);
		}
	}

	idx_t offset;
	size_t metadata_count;
	avro_file_reader_t reader;
	AllocatedData local_buffer;
};

struct AvroMetadata {
	static TableFunction GetFunction();
};

} // namespace duckdb
