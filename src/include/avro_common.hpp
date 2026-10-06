//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_cpp.hpp"

#include <avro.h>
#include <memory>
#include <string>

namespace duckdb {

namespace avro {

using cxx::idx_t;
using cxx::LogicalTypeId;

//! The V2 C API error codes the errors of this extension are reported with, which select the error class DuckDB shows
enum class ErrorCode : int {
	INVALID_INPUT = 2001,
	OUT_OF_RANGE = 2003,
	BINDER = 5003,
	NOT_IMPLEMENTED = 5009,
	INVALID_CONFIGURATION = 7002,
	INTERNAL = 8001,
};

inline cxx::Exception MakeError(ErrorCode code, const std::string &message) {
	return cxx::Exception(static_cast<int>(code), message);
}

inline cxx::Exception InvalidInputError(const std::string &message) {
	return MakeError(ErrorCode::INVALID_INPUT, message);
}

inline cxx::Exception OutOfRangeError(const std::string &message) {
	return MakeError(ErrorCode::OUT_OF_RANGE, message);
}

inline cxx::Exception BinderError(const std::string &message) {
	return MakeError(ErrorCode::BINDER, message);
}

inline cxx::Exception NotImplementedError(const std::string &message) {
	return MakeError(ErrorCode::NOT_IMPLEMENTED, message);
}

inline cxx::Exception InvalidConfigurationError(const std::string &message) {
	return MakeError(ErrorCode::INVALID_CONFIGURATION, message);
}

inline cxx::Exception InternalError(const std::string &message) {
	return MakeError(ErrorCode::INTERNAL, message);
}

//! The last error avro-c reported
inline cxx::Exception AvroError() {
	return InvalidInputError(avro_strerror());
}

std::string StringLower(const std::string &str);
bool StringEqualsCaseInsensitive(const std::string &a, const std::string &b);
//! The SQLNULL type, which the type constructors do not create
cxx::LogicalType CreateNullType(const cxx::Context &context);

//! The contents of a file, read into memory in full
struct AvroFileBuffer {
	std::unique_ptr<char[]> data;
	idx_t size = 0;

	static AvroFileBuffer Read(const cxx::Context &context, const std::string &path);
	//! Reads the file with the given open options, e.g. the ones the multi-file reader knows the file by
	static AvroFileBuffer Read(const cxx::Context &context, const std::string &path, cxx::FileOpenOptions options);
};

} // namespace avro

} // namespace duckdb
