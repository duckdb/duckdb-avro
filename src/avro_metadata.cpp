#include "avro_multi_file_info.hpp"
#include "avro_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "avro_metadata.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include <avro.h>

namespace duckdb {

static unique_ptr<FunctionData> AvroMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AvroMetadataBindData>();
	result->file_path = input.inputs[0].ToString();

	names.emplace_back("key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> AvroMetadataInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<AvroMetadataBindData>();
	auto result = make_uniq<AvroMetadataGlobalState>();

	auto caching_file_system = CachingFileSystem::Get(context);
	OpenFileInfo file_info;
	file_info.path = bind_data.file_path;

	auto caching_file_handle = caching_file_system.OpenFile(file_info, FileOpenFlags::FILE_FLAGS_READ);
	auto total_size = caching_file_handle->GetFileSize();
	data_ptr_t data = nullptr;

	auto buf_handle = caching_file_handle->Read(data, total_size);
	auto buffer_data = buf_handle.Ptr();

	auto avro_reader = avro_reader_memory(const_char_ptr_cast(buffer_data), total_size);
	if (avro_reader_reader(avro_reader, &result->reader)) {
		throw InvalidInputException("Failed to read Avro file: %s", avro_strerror());
	}

	if (avro_file_reader_get_metadata_count(result->reader, &result->metadata_count)) {
		throw InvalidInputException("Failed to get metadata count");
	}

	return std::move(result);
}

static void AvroMetadataFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<AvroMetadataGlobalState>();

	idx_t count = 0;
	while (gstate.offset < gstate.metadata_count && count < STANDARD_VECTOR_SIZE) {
		const char *key = nullptr;
		const char *value = nullptr;
		size_t value_size = 0;

		if (avro_file_reader_get_metadata_by_index(gstate.reader, gstate.offset, &key, &value, &value_size)) {
			throw InvalidInputException("Failed to get metadata at index %llu", gstate.offset);
		}

		output.SetValue(0, count, Value(key ? string(key) : string()));
		output.SetValue(1, count, Value(value ? string(value, value_size) : string()));

		gstate.offset++;
		count++;
	}

	output.SetCardinality(count);
}

TableFunction AvroMetadata::GetFunction() {
	TableFunction func("avro_metadata", {LogicalType::VARCHAR}, AvroMetadataFunction, AvroMetadataBind,
	                   AvroMetadataInit);
	return func;
}

} // namespace duckdb
