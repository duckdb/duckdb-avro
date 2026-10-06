//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "avro_common.hpp"
#include "avro_type.hpp"

#include <type_traits>

namespace duckdb {

namespace avro {

struct FileReaderDeleter {
	void operator()(std::remove_pointer_t<avro_file_reader_t> *reader) const {
		avro_file_reader_close(reader);
	}
};

struct ValueInterfaceDeleter {
	void operator()(avro_value_iface_t *iface) const {
		avro_value_iface_decref(iface);
	}
};

//! An Avro object container file, read into memory
class AvroFile {
public:
	AvroFile(cxx::Context &context, const std::string &path);

public:
	idx_t NumBlocks() const {
		return block_count;
	}

public:
	AvroFileBuffer buffer;
	std::unique_ptr<std::remove_pointer_t<avro_file_reader_t>, FileReaderDeleter> reader;
	std::unique_ptr<avro_value_iface_t, ValueInterfaceDeleter> value_iface;
	idx_t block_count = 0;

	AvroType avro_type;
	//! Whether the root is a record, whose fields are pulled up into the columns
	bool root_is_struct = false;
	//! The columns the file is read as
	std::vector<AvroColumn> columns;
	//! The key-value metadata of the file
	std::vector<std::pair<std::string, std::string>> metadata;
};

struct AvroReader {
	//! Registers "read_single_avro_file", which reads a single file, and "read_avro" on top of it
	static void Register(cxx::Extension &extension, cxx::Context &context);
};

} // namespace avro

} // namespace duckdb
