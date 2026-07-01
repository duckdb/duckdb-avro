#include "avro_multi_file_info.hpp"
#include "avro_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "avro_metadata.hpp"
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

	auto &fs = FileSystem::GetFileSystem(context);

	OpenFileInfo file;
	file.path = bind_data.file_path;

	FileOpenFlags flags = FileFlags::FILE_FLAGS_READ;
	flags.SetCachingMode(CachingMode::ALWAYS_CACHE);
	auto file_handle = fs.OpenFile(file, flags);
	auto total_size = file_handle->GetFileSize();

	auto &local_buffer = result->local_buffer;
	local_buffer = Allocator::DefaultAllocator().Allocate(total_size);
	fs.Read(*file_handle, local_buffer.get(), total_size);

	auto avro_reader = avro_reader_memory(const_char_ptr_cast(local_buffer.get()), total_size);

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

	output.SetChildCardinality(count);
}

TableFunction AvroMetadata::GetFunction() {
	TableFunction func("avro_metadata", {LogicalType::VARCHAR}, AvroMetadataFunction, AvroMetadataBind,
	                   AvroMetadataInit);
	return func;
}

} // namespace duckdb
