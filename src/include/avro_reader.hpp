#pragma once

#include "duckdb/common/helper.hpp"
#include "avro_type.hpp"
#include "avro_copy.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {

class AvroReader : public BaseFileReader {
public:
	AvroReader(ClientContext &context, const OpenFileInfo file);

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
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state, LocalTableFunctionState &local_state,
	          DataChunk &chunk) override;

public:
	avro_file_reader_t reader;
	avro_value_t value;
	unique_ptr<Vector> read_vec;

	unique_ptr<AvroInMemoryBuffer> file_buffer;
	idx_t file_size = 0;
	AvroType avro_type;
	LogicalType duckdb_type;
};

} // namespace duckdb
