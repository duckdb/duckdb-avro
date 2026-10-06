#include "avro_common.hpp"

#include <cctype>

namespace duckdb {

namespace avro {

std::string StringLower(const std::string &str) {
	std::string result(str);
	for (auto &c : result) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	return result;
}

bool StringEqualsCaseInsensitive(const std::string &a, const std::string &b) {
	return StringLower(a) == StringLower(b);
}

cxx::LogicalType CreateNullType(const cxx::Context &context) {
	return context.ParseType("\"NULL\"");
}

AvroFileBuffer AvroFileBuffer::Read(const cxx::Context &context, const std::string &path) {
	return Read(context, path, context.GetFileSystem().CreateOpenOptions());
}

AvroFileBuffer AvroFileBuffer::Read(const cxx::Context &context, const std::string &path,
                                    cxx::FileOpenOptions options) {
	auto fs = context.GetFileSystem();
	options.SetFlag(cxx::FileFlags::READ).SetFlag(cxx::FileFlags::EXTERNAL_FILE_CACHE);
	auto handle = fs.OpenFile(path, options);

	AvroFileBuffer result;
	result.size = handle.Size();
	result.data = std::unique_ptr<char[]>(new char[result.size ? result.size : 1]);
	if (result.size) {
		handle.ReadAt(result.data.get(), result.size, 0);
	}
	return result;
}

} // namespace avro

} // namespace duckdb
