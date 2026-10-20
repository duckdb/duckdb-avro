#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"

#include <avro.h>
#include <avro/allocation.h>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace duckdb;

namespace {

// Count allocations without retaining pointers, so LeakSanitizer can also detect
// leaked allocations made by codecs outside Avro's allocator.
std::atomic<int64_t> live_allocations {0};
std::atomic<int64_t> fail_allocation {-1};

void Require(bool condition, const std::string &message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

void *TrackingAllocator(void *, void *ptr, size_t, size_t size) {
	if (!size) {
		if (ptr) {
			live_allocations--;
		}
		free(ptr);
		return nullptr;
	}
	if (fail_allocation > 0 && --fail_allocation == 0) {
		fail_allocation = -1;
		return nullptr;
	}
	bool is_new = !ptr;
	auto result = realloc(ptr, size);
	if (result && is_new) {
		live_allocations++;
	}
	return result;
}

struct Faults {
	int fail_write = 0;
	int fail_create_allocation = 0;
	bool fail_value_allocation = false;
	int writes = 0;
};

class FaultFileSystem : public FileSystem {
public:
	struct Handle : FileHandle {
		Handle(FileSystem &fs, const string &path, FileOpenFlags flags) : FileHandle(fs, path, flags) {
		}
		void Close() override {
		}
	};

	explicit FaultFileSystem(Faults &faults) : faults(faults) {
	}
	string GetName() const override {
		return "AvroFaultFileSystem";
	}
	bool CanHandleFile(const string &path) override {
		return path.rfind("avrofault://", 0) == 0;
	}
	bool IsManuallySet() override {
		return true;
	}
	string PathSeparator(const string &) override {
		return "/";
	}
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener>) override {
		// Binding has already finished. The next Avro allocations create the
		// memory writers, file writer and codec, in that order.
		fail_allocation = faults.fail_create_allocation;
		return make_uniq<Handle>(*this, path, flags);
	}
	bool FileExists(const string &, optional_ptr<FileOpener>) override {
		return false;
	}
	bool DirectoryExists(const string &, optional_ptr<FileOpener>) override {
		return false;
	}
	bool IsPipe(const string &, optional_ptr<FileOpener>) override {
		return false;
	}
	void Write(FileHandle &, void *, int64_t, idx_t) override {
		CheckWrite();
	}
	int64_t Write(FileHandle &, void *, int64_t size) override {
		CheckWrite();
		return size;
	}
	void FileSync(FileHandle &) override {
	}
	void AbortFileWrite(FileHandle &) override {
	}
	void RemoveFile(const string &, optional_ptr<FileOpener>) override {
	}

private:
	void CheckWrite() {
		faults.writes++;
		if (faults.writes == faults.fail_write) {
			throw OutOfMemoryException("Injected Avro write failure");
		}
		if (faults.writes == 1 && faults.fail_value_allocation) {
			// The next Avro allocation initializes the generic row value.
			fail_allocation = 1;
		}
	}

	Faults &faults;
};

void TestCopy(const std::string &codec, Faults faults, const std::string &expected_error = "",
              const std::string &options = "") {
	{
		DuckDB db(nullptr);
		Connection con(db);
		Require(!con.Query("LOAD avro")->HasError(), "Could not load avro");
		Require(!con.Query("SET threads=1")->HasError(), "Could not set threads");
		FileSystem::GetFileSystem(*con.context).RegisterSubSystem(make_uniq<FaultFileSystem>(faults));
		for (int attempt = 0; attempt < 3; attempt++) {
			faults.writes = 0;
			auto result = con.Query("COPY (SELECT 42 a) TO 'avrofault://test.avro' "
			                        "(FORMAT AVRO, CODEC '" +
			                        codec + "'" + options + ")");
			fail_allocation = -1;
			if (expected_error.empty()) {
				Require(!result->HasError(), result->HasError() ? result->GetError() : "");
				Require(faults.writes == 2, "Expected header and data writes");
			} else {
				Require(result->HasError(), "Expected COPY to fail");
				Require(result->GetError().find(expected_error) != std::string::npos, result->GetError());
			}
			result.reset();
			Require(live_allocations == 0,
			        "COPY leaked " + std::to_string(live_allocations.load()) + " Avro allocations");
		}
	}
	Require(live_allocations == 0, "Avro allocations survived database destruction");
}

void CheckAvro(int result) {
	Require(result == 0, avro_strerror());
}

void TestDiscard(const char *codec, bool fail_flush) {
	char header[1024];
	char datum[1024];
	avro_schema_t schema;
	const char *json = R"({"type":"record","name":"r","fields":[{"name":"a","type":"long"}]})";
	CheckAvro(avro_schema_from_json_length(json, strlen(json), &schema));
	auto writer = avro_writer_memory(header, sizeof(header));
	auto datum_writer = avro_writer_memory(datum, sizeof(datum));
	avro_file_writer_t file_writer = nullptr;
	CheckAvro(avro_file_writer_create_from_writers_with_metadata_and_codec(writer, datum_writer, schema, &file_writer,
	                                                                       nullptr, codec));
	auto iface = avro_generic_class_from_schema(schema);
	Require(iface != nullptr, avro_strerror());
	avro_value_t value;
	CheckAvro(avro_generic_value_new(iface, &value));
	avro_value_t field;
	CheckAvro(avro_value_get_by_index(&value, 0, &field, nullptr));
	CheckAvro(avro_value_set_long(&field, 42));
	CheckAvro(avro_file_writer_append_value(file_writer, &value));
	avro_value_decref(&value);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);

	if (fail_flush) {
		// Leave room for the block count and compressed length, so the codec
		// allocates its output buffer before the payload write fails.
		avro_writer_memory_set_dest(writer, header, 2);
		Require(avro_file_writer_flush(file_writer) == ENOSPC, "Expected a failed block flush");
	}
	// Cancellation must not touch the output or datum buffers or allocate.
	avro_writer_memory_set_dest(writer, nullptr, 0);
	avro_writer_memory_set_dest(datum_writer, nullptr, 0);
	fail_allocation = 1;
	avro_file_writer_free(file_writer);
	Require(fail_allocation == 1, "Free attempted to allocate");
	fail_allocation = -1;
	Require(live_allocations == 0, "Discard leaked Avro allocations");
	avro_file_writer_free(nullptr);
}

void TestHeaderError() {
	char header[1024];
	avro_schema_t schema;
	CheckAvro(avro_schema_from_json_length("\"long\"", 6, &schema));
	auto writer = avro_writer_memory(header, sizeof(header));
	auto datum_writer = avro_writer_memory(nullptr, 0);
	avro_file_writer_t file_writer = nullptr;
	auto ret = avro_file_writer_create_from_writers_with_metadata_and_codec(writer, datum_writer, schema, &file_writer,
	                                                                        "invalid JSON", "deflate");
	Require(ret == EINVAL && file_writer, "Expected header error after ownership transfer");
	avro_schema_decref(schema);
	avro_file_writer_free(file_writer);
	Require(live_allocations == 0, "Header error leaked Avro allocations");
}

} // namespace

int main() {
	auto previous_allocator = AVRO_CURRENT_ALLOCATOR;
	avro_set_allocator(TrackingAllocator, nullptr);
	try {
		for (const auto &codec : {"null", "deflate", "snappy"}) {
			TestCopy(codec, {});
			TestCopy(codec, {1}, "Injected Avro write failure");
			TestCopy(codec, {2}, "Injected Avro write failure");
			for (int allocation = 1; allocation <= 4; allocation++) {
				TestCopy(codec, {0, allocation}, "Cannot allocate");
			}
			TestCopy(codec, {0, 0, true}, "Cannot allocate memory");
			TestCopy(codec, {}, "", ", METADATA {'large': '" + std::string(16384, 'x') + "'}");
			TestDiscard(codec, false);
			TestDiscard(codec, true);
		}
		TestCopy("unknown-codec", {}, "Unknown codec");
		TestHeaderError();
		avro_set_allocator(previous_allocator.alloc, previous_allocator.user_data);
		std::cout << "Avro writer cleanup tests passed\n";
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << "\n";
		return 1;
	}
	return 0;
}
