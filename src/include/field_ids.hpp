#pragma once

#include "avro_common.hpp"

#include <unordered_map>
#include <vector>

namespace duckdb {

namespace avro {

//! NOTE: This is copied (but modified) from 'parquet_extension.cpp', ideally this lives in core DuckDB instead

struct FieldID;

struct ChildFieldIDs {
public:
	//! The field id of a child, looked up by name case-insensitively
	const FieldID *Find(const std::string &name) const;
	std::unordered_map<std::string, FieldID> &Ids();

private:
	//! Keyed by lowercase name
	std::unique_ptr<std::unordered_map<std::string, FieldID>> ids;
};

struct FieldID {
public:
	static constexpr const char *DUCKDB_FIELD_ID = "__duckdb_field_id";
	static constexpr const char *DUCKDB_NULLABLE_ID = "__duckdb_nullable";

public:
	FieldID();
	explicit FieldID(int32_t field_id, bool nullable = true);

public:
	int32_t GetFieldId() const;

public:
	bool set = false;
	int32_t field_id;
	bool nullable = true;
	ChildFieldIDs children;
};

struct FieldIDUtils {
public:
	FieldIDUtils() = delete;

public:
	static ChildFieldIDs ParseFieldIds(cxx::Context &context, const cxx::Value &input, const std::vector<std::string> &names,
	                                   const std::vector<cxx::LogicalType> &types);
};

} // namespace avro

} // namespace duckdb
