#include "avro_metadata.hpp"

#include <algorithm>

namespace duckdb {

namespace avro {

namespace {

struct AvroMetadataBindData {
	std::string file_path;
};

struct AvroMetadataGlobalState {
	AvroMetadataGlobalState(const cxx::Context &context, const std::string &file_path)
	    : buffer(AvroFileBuffer::Read(context, file_path)) {
		auto avro_reader = avro_reader_memory(buffer.data.get(), static_cast<int64_t>(buffer.size));
		if (avro_reader_reader(avro_reader, &reader)) {
			throw InvalidInputError(std::string("Failed to read Avro file: ") + avro_strerror());
		}
		if (avro_file_reader_get_metadata_count(reader, &metadata_count)) {
			avro_file_reader_close(reader);
			throw InvalidInputError("Failed to get metadata count");
		}
	}
	~AvroMetadataGlobalState() {
		avro_file_reader_close(reader);
	}

	AvroFileBuffer buffer;
	avro_file_reader_t reader = nullptr;
	idx_t offset = 0;
	size_t metadata_count = 0;
};

void AvroMetadataBind(cxx::TableFunction::BindInput &input) {
	auto context = input.GetContext();
	auto varchar = context.CreateType(LogicalTypeId::VARCHAR);
	input.AddResultColumn("key", varchar);
	input.AddResultColumn("value", varchar);
	input.SetBindData<AvroMetadataBindData>(
	    AvroMetadataBindData {std::string(input.GetConstantArgument(0).Get<cxx::varchar_t>().view())});
}

void AvroMetadataInit(cxx::TableFunction::InitGlobalInput &input) {
	auto &bind_data = input.GetBindData<AvroMetadataBindData>();
	input.SetGlobalState<AvroMetadataGlobalState>(input.GetContext(), bind_data.file_path);
}

void AvroMetadataExec(cxx::TableFunction::ExecInput &input) {
	auto &gstate = input.GetGlobalState<AvroMetadataGlobalState>();
	auto output = input.GetOutputChunk();
	auto key_vector = output.GetVector(0);
	auto value_vector = output.GetVector(1);

	auto count = std::min<idx_t>(gstate.metadata_count - gstate.offset, output.GetCapacity());
	key_vector.SetSize(count);
	value_vector.SetSize(count);
	for (idx_t row = 0; row < count; row++) {
		const char *key = nullptr;
		const char *value = nullptr;
		size_t value_size = 0;
		if (avro_file_reader_get_metadata_by_index(gstate.reader, gstate.offset, &key, &value, &value_size)) {
			throw InvalidInputError("Failed to get metadata at index " + std::to_string(gstate.offset));
		}
		key_vector.AssignString(row, key ? std::string_view(key) : std::string_view());
		value_vector.AssignString(row, value ? std::string_view(value, value_size) : std::string_view());
		gstate.offset++;
	}
}

} // namespace

void AvroMetadata::Register(cxx::Extension &extension, cxx::Context &context) {
	auto function = cxx::TableFunction::Create(extension);
	function.SetName("avro_metadata");
	function.GetSignature().AddParameter("path", context.CreateType(LogicalTypeId::VARCHAR));
	function.SetBindCallback(AvroMetadataBind).SetInitGlobalCallback(AvroMetadataInit).SetExecCallback(AvroMetadataExec);
	function.Register();
}

} // namespace avro

} // namespace duckdb
