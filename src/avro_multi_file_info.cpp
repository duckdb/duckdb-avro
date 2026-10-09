#include "avro_multi_file_info.hpp"
#include "avro_reader.hpp"
#include <avro.h>
#include "duckdb/common/types/value.hpp"

namespace duckdb {

unique_ptr<MultiFileReaderInterface> AvroMultiFileInfo::CreateInterface(ClientContext &context) {
	return make_uniq<AvroMultiFileInfo>();
}

unique_ptr<BaseFileReaderOptions> AvroMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                       optional_ptr<TableFunctionInfo> info) {
	return make_uniq<AvroFileReaderOptions>();
}

bool AvroMultiFileInfo::ParseCopyOption(ClientContext &context, const Identifier &key, const vector<Value> &values,
                                        BaseFileReaderOptions &options_p, vector<Identifier> &expected_names,
                                        vector<LogicalType> &expected_types) {
	// We currently do not have any options for the scanner, so we always return false
	return false;
}

bool AvroMultiFileInfo::ParseOption(ClientContext &context, const Identifier &key, const Value &val,
                                    MultiFileOptions &file_options, BaseFileReaderOptions &options) {
	return false;
}

struct AvroMultiFileData final : public TableFunctionData {
public:
	AvroMultiFileData() = default;
	AvroFileReaderOptions options;
	idx_t initial_file_block_count = 1;
};

unique_ptr<TableFunctionData> AvroMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                    unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<AvroMultiFileData>();
	if (options_p) {
		result->options = options_p->Cast<AvroFileReaderOptions>();
	}
	return std::move(result);
}

void AvroMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<Identifier> &names,
                                   MultiFileBindData &bind_data) {
	if (bind_data.file_options.union_by_name) {
		throw NotImplementedException("'union_by_name' not implemented for Avro reader yet");
	}
	auto &avro_data = bind_data.bind_data->Cast<AvroMultiFileData>();
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
	    context, return_types, names, *bind_data.file_list, bind_data, avro_data.options, bind_data.file_options);
	D_ASSERT(names.size() == return_types.size());
}

void AvroMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	if (!multi_file_data.initial_reader) {
		return;
	}
	auto &avro_data = multi_file_data.bind_data->Cast<AvroMultiFileData>();
	auto &initial_reader = multi_file_data.initial_reader->Cast<AvroReader>();
	avro_data.initial_file_block_count = initial_reader.NumBlocks();
}

optional_idx AvroMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                           const MultiFileGlobalState &global_state, FileExpandResult expand_result) {
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		// always launch max threads if we are reading multiple files
		return {};
	}
	auto &avro_data = bind_data_p.bind_data->Cast<AvroMultiFileData>();
	return MaxValue<idx_t>(avro_data.initial_file_block_count, 1);
}

struct AvroFileGlobalState : public GlobalTableFunctionState {
public:
	AvroFileGlobalState() = default;
	~AvroFileGlobalState() override = default;

public:
	idx_t next_block_index = 0;
};

unique_ptr<GlobalTableFunctionState> AvroMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                              MultiFileBindData &bind_data,
                                                                              MultiFileGlobalState &global_state) {
	return make_uniq<AvroFileGlobalState>();
}

//! The Avro Local File State, basically refers to the Scan of one Avro File
//! This is done by calling the Avro Scan directly on one file.
struct AvroFileLocalState : public LocalTableFunctionState {
public:
	explicit AvroFileLocalState(ClientContext &context) : context(context) {};

public:
	shared_ptr<AvroReader> file_scan;
	unique_ptr<AvroReaderScanState> scan_state;
	idx_t block_index = 0;
	ClientContext &context;
};

unique_ptr<LocalTableFunctionState> AvroMultiFileInfo::InitializeLocalState(ClientContext &context,
                                                                            GlobalTableFunctionState &function_state) {
	return make_uniq<AvroFileLocalState>(context);
}

shared_ptr<BaseFileReader> AvroMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                           BaseUnionData &union_data,
                                                           const MultiFileBindData &bind_data) {
	throw NotImplementedException("'union_by_name' is not implemented for the Avro reader yet");
}

shared_ptr<BaseFileReader> AvroMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                           const OpenFileInfo &file, idx_t file_idx,
                                                           const MultiFileBindData &bind_data) {
	auto &avro_data = bind_data.bind_data->Cast<AvroMultiFileData>();
	return make_shared_ptr<AvroReader>(context, file, avro_data.options);
}

shared_ptr<BaseFileReader> AvroMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                           BaseFileReaderOptions &options,
                                                           const MultiFileOptions &file_options) {
	return make_shared_ptr<AvroReader>(context, file, options.Cast<AvroFileReaderOptions>());
}

bool AvroReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                   LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<AvroFileGlobalState>();
	auto &lstate = lstate_p.Cast<AvroFileLocalState>();
	if (gstate.next_block_index >= NumBlocks()) {
		return false;
	}
	if (!lstate.file_scan || lstate.file_scan.get() != this) {
		lstate.scan_state.reset();
	}
	lstate.file_scan = shared_ptr_cast<BaseFileReader, AvroReader>(shared_from_this());
	lstate.block_index = gstate.next_block_index++;
	return true;
}

void AvroReader::PrepareScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                             LocalTableFunctionState &lstate_p) {
	auto &lstate = lstate_p.Cast<AvroFileLocalState>();
	if (!lstate.scan_state) {
		lstate.scan_state = make_uniq<AvroReaderScanState>(context, *this);
	}
	lstate.scan_state->SelectBlock(lstate.block_index);
}

AsyncResult AvroReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                             LocalTableFunctionState &local_state_p, DataChunk &chunk) {
	auto &lstate = local_state_p.Cast<AvroFileLocalState>();
	D_ASSERT(lstate.scan_state);
	Read(*lstate.scan_state, chunk);
	return chunk.size() ? AsyncResult(SourceResultType::HAVE_MORE_OUTPUT) : AsyncResult(SourceResultType::FINISHED);
}

void AvroReader::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate_p) {
	auto &gstate = gstate_p.Cast<AvroFileGlobalState>();
	gstate.next_block_index = 0;
}

InsertionOrderPreservingMap<Value> AvroReader::GetMetadata() const {
	InsertionOrderPreservingMap<Value> metadata;
	size_t metadata_count = 0;
	if (avro_file_reader_get_metadata_count(reader, &metadata_count)) {
		throw InvalidInputException("Failed to get metadata count");
	}
	for (idx_t i = 0; i < metadata_count; i++) {
		const char *key = nullptr;
		const char *value = nullptr;
		size_t value_size = 0;
		if (avro_file_reader_get_metadata_by_index(reader, i, &key, &value, &value_size)) {
			throw InvalidInputException("Failed to get metadata at index %llu", i);
		}
		if (!key) {
			continue;
		}
		metadata.insert(key, Value(value ? string(value, value_size) : string()));
	}
	return metadata;
}

string AvroReader::GetMetadataValue(const string &key) const {
	auto res = avro_file_reader_get_metadata(reader, key.c_str());
	if (!res) {
		return string();
	}
	return res;
}

unique_ptr<NodeStatistics> AvroMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	//! FIXME: Here is where we might set statistics, for optimizations if we have them
	return make_uniq<NodeStatistics>();
}

} // namespace duckdb
