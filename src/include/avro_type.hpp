//===----------------------------------------------------------------------===//
//                         DuckDB
//
// avro_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "avro_common.hpp"

#include <limits>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace duckdb {

namespace avro {

//! An Avro schema node, together with the DuckDB type id it is read as
struct AvroType {
public:
	AvroType() = default;
	AvroType(avro_type_t avro_type_p, LogicalTypeId type_id_p,
	         std::vector<std::pair<std::string, AvroType>> children_p = {},
	         std::unordered_map<idx_t, idx_t> union_child_map_p = {})
	    : avro_type(avro_type_p), type_id(type_id_p), children(std::move(children_p)),
	      union_child_map(std::move(union_child_map_p)) {
	}

public:
	bool HasFieldId() const {
		return field_id != std::numeric_limits<int32_t>::max();
	}
	//! The number of union branches that are not NULL
	idx_t NonNullUnionChildCount() const {
		return union_child_map.size();
	}

public:
	avro_type_t avro_type = AVRO_NULL;
	LogicalTypeId type_id = LogicalTypeId::INVALID;
	std::vector<std::pair<std::string, AvroType>> children;
	//! The DuckDB union member of each union branch that is not NULL
	std::unordered_map<idx_t, idx_t> union_child_map;
	int32_t field_id = std::numeric_limits<int32_t>::max();
	bool is_timestamp_millis = false;
	//! DECIMAL parameters
	uint8_t decimal_width = 0;
	uint8_t decimal_scale = 0;
	//! ENUM symbols
	std::vector<std::string> enum_symbols;
};

//! A column (or nested field) as DuckDB sees it. The children mirror the layout of the vector: the fields of a STRUCT,
//! the element of a LIST, the key/value STRUCT of a MAP and the members of a UNION
struct AvroColumn {
	AvroColumn(std::string name_p, cxx::LogicalType type_p) : name(std::move(name_p)), type(std::move(type_p)) {
	}

	std::string name;
	cxx::LogicalType type;
	std::vector<AvroColumn> children;
	std::optional<int32_t> field_id;
};

//! Converts an Avro schema into the type tree it is read with
AvroType TransformSchema(avro_schema_t avro_schema, std::unordered_set<std::string> parent_schema_names);

//! Converts an Avro type into the DuckDB column it is read into.
//! We use special transformation rules for unions with null:
//! 1) the null does not become a union entry and
//! 2) if there is only one entry the union disappears and is replaced by its child
AvroColumn TransformAvroType(cxx::Context &context, const std::string &name, const AvroType &avro_type);

} // namespace avro

} // namespace duckdb
