#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

namespace avro {

//! NOTE: This is copied (but modified) from 'parquet_extension.cpp', ideally this lives in core DuckDB instead

struct FieldID {
public:
	static constexpr const auto DUCKDB_FIELD_ID = "__duckdb_field_id";
	static constexpr const auto DUCKDB_NULLABLE_ID = "__duckdb_nullable";

public:
	FieldID();
	explicit FieldID(int32_t field_id, bool nullable = true);

public:
	int32_t GetFieldId() const;

public:
	bool set = false;
	int32_t field_id;
	bool nullable = true;
	case_insensitive_map_t<FieldID> children;
};

struct FieldIDUtils {
public:
	FieldIDUtils() = delete;

public:
	static case_insensitive_map_t<FieldID> ParseFieldIds(const Value &input, const vector<string> &names,
	                                                     const vector<LogicalType> &types);
};

} // namespace avro

} // namespace duckdb
