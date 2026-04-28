#pragma once

#include "duckdb/common/helper.hpp"
#include "avro_type.hpp"
#include "avro_multi_file_info.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {

class AvroReader : public BaseFileReader {
public:
	AvroReader(ClientContext &context, const OpenFileInfo file,
	           const AvroFileReaderOptions &options = AvroFileReaderOptions());

	~AvroReader() {
		avro_value_decref(&value);
		avro_file_reader_close(reader);
	}

public:
	void Read(DataChunk &output);

	string GetReaderType() const override {
		return "Avro";
	}

	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;

public:
	avro_file_reader_t reader;
	avro_value_t value;
	DataChunk read_chunk;

	BufferHandle buf_handle;
	AvroType avro_type;
	LogicalType duckdb_type;
};

} // namespace duckdb
