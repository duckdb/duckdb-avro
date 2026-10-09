#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/helper.hpp"
#include "avro_type.hpp"
#include "avro_multi_file_info.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {

class AvroReader;

class AvroReaderScanState {
public:
	AvroReaderScanState(ClientContext &context, AvroReader &reader);
	~AvroReaderScanState();

	void SelectBlock(idx_t block_index);

public:
	AvroReader &reader;
	avro_file_block_reader_t block_reader = nullptr;
	avro_value_t value;
	DataChunk read_chunk;
};

class AvroReader : public BaseFileReader {
public:
	AvroReader(ClientContext &context, const OpenFileInfo file,
	           const AvroFileReaderOptions &options = AvroFileReaderOptions());

	~AvroReader() {
		avro_value_iface_decref(value_iface);
		avro_file_reader_close(reader);
	}

public:
	void Read(AvroReaderScanState &scan_state, DataChunk &output);

	idx_t NumBlocks() const {
		return block_count;
	}

	string GetReaderType() const override {
		return "Avro";
	}

	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	void PrepareScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                 LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) override;
	InsertionOrderPreservingMap<Value> GetMetadata() const override;

	string GetMetadataValue(const string &key) const;

public:
	avro_file_reader_t reader;
	avro_value_iface_t *value_iface;
	idx_t block_count;

	AllocatedData local_buffer;
	AvroType avro_type;
	LogicalType duckdb_type;
};

} // namespace duckdb
