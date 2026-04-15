#pragma once

#include "duckdb/function/copy_function.hpp"
#include <avro.h>
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct AvroCopyFunction {
	static CopyFunction Create();
};

struct WriteAvroBindData : public FunctionData {
public:
	WriteAvroBindData(CopyFunctionBindInput &input, const vector<string> &names, const vector<LogicalType> &types);
	WriteAvroBindData() {
	}
	virtual ~WriteAvroBindData();

public:
	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<WriteAvroBindData>();
		res->names = names;
		res->types = types;

		res->schema = avro_schema_incref(schema);
		res->json_schema = json_schema;

		res->interface = avro_value_iface_incref(interface);
		return std::move(res);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<WriteAvroBindData>();
		if (interface != other.interface) {
			//! FIXME: is comparing pointers too much?
			return false;
		}
		if (other.types.size() != types.size()) {
			return false;
		}
		if (other.names.size() != names.size()) {
			return false;
		}
		if (json_schema != other.json_schema) {
			return false;
		}
		return true;
	}

public:
	vector<string> names;
	vector<LogicalType> types;

	//! The schema of the file to write
	avro_schema_t schema = nullptr;
	string json_schema;

	string json_metadata;
	//! The interface through which new avro values are created
	avro_value_iface_t *interface = nullptr;
};

struct AvroInMemoryBuffer {
public:
	AvroInMemoryBuffer(Allocator &allocator, idx_t initial_capacity = 0)
	    : allocator(allocator), capacity(initial_capacity) {
		if (initial_capacity) {
			Allocate(capacity);
		}
	}
	~AvroInMemoryBuffer() {
		if (data) {
			allocator.FreeData(data, capacity);
		}
	}

public:
	void Resize(idx_t new_capacity) {
		D_ASSERT(this->capacity < new_capacity);
		if (data) {
			//! Free the old buffer directly, we don't need to copy the old contents
			allocator.FreeData(data, this->capacity);
			data = nullptr;
		}
		Allocate(new_capacity);
	}
	void ResizeAndCopy(idx_t new_capacity) {
		data_ptr_t old_data = data;
		auto old_capacity = capacity;

		data = nullptr;
		Allocate(new_capacity);
		if (old_capacity) {
			memcpy(data, old_data, old_capacity);
			allocator.FreeData(old_data, old_capacity);
		}
	}
	data_ptr_t GetData() {
		return data;
	}
	idx_t GetCapacity() const {
		return capacity;
	}

private:
	void Allocate(idx_t new_capacity) {
		D_ASSERT(!data);
		data = allocator.AllocateData(new_capacity);
		capacity = new_capacity;
	}

public:
	Allocator &allocator;
	data_ptr_t data = nullptr;
	idx_t capacity = 0;
};

struct WriteAvroGlobalState : public GlobalFunctionData {
public:
	static constexpr idx_t BUFFER_SIZE = 1024;
	static constexpr idx_t DATUM_BUFFER_SIZE = 16 * 1024;
	//! Size of the bytes written to mark the end of a section (header/datablock)
	static constexpr idx_t SYNC_SIZE = 16;
	//! Avro uses a handrolled varint that they assert can only be 10 bytes
	static constexpr idx_t MAX_ROW_COUNT_BYTES = 10;

public:
	WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs, const string &file_path);
	virtual ~WriteAvroGlobalState();

public:
	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		handle->Write((void *)data, size);
	}

	idx_t FileSize() {
		lock_guard<mutex> flock(lock);
		return handle->GetFileSize();
	}

public:
	Allocator &allocator;
	AvroInMemoryBuffer memory_buffer;
	AvroInMemoryBuffer datum_buffer;
	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;

	//! The writer for the file
	avro_writer_t writer;
	avro_writer_t datum_writer;
	avro_file_writer_t file_writer;
};

struct WriteAvroLocalState : public LocalFunctionData {
public:
	WriteAvroLocalState(FunctionData &bind_data_p);
	virtual ~WriteAvroLocalState();

public:
	//! Avro value representing a row of the schema
	avro_value_t value;
};

} // namespace duckdb
