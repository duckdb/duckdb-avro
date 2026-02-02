#pragma once

#include "duckdb/common/types.hpp"
#include <avro.h>
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

struct AvroType {
public:
	AvroType() : duckdb_type(LogicalType::INVALID) {
	}
	AvroType(avro_type_t avro_type_p, LogicalType duckdb_type_p, child_list_t<AvroType> children_p = {},
	         unordered_map<idx_t, optional_idx> union_child_map_p = {})
	    : duckdb_type(duckdb_type_p), avro_type(avro_type_p), children(children_p), union_child_map(union_child_map_p) {
	}

public:
	bool operator==(const AvroType &other) const {
		return duckdb_type == other.duckdb_type && avro_type == other.avro_type && children == other.children &&
		       union_child_map == other.union_child_map;
	}
	const bool HasFieldId() const {
		return field_id != NumericLimits<int32_t>::Maximum();
	}
	const int32_t GetFieldId() const {
		D_ASSERT(HasFieldId());
		return field_id;
	}

public:
	// we use special transformation rules for unions with null:
	// 1) the null does not become a union entry and
	// 2) if there is only one entry the union disappears and is repaced by its
	// child
	static MultiFileColumnDefinition TransformAvroType(const string &name, const AvroType &avro_type) {
		vector<MultiFileColumnDefinition> children;

		LogicalType duckdb_type;
		auto id = avro_type.duckdb_type.id();
		switch (id) {
		case LogicalTypeId::STRUCT: {
			child_list_t<LogicalType> type_children;
			for (auto &child : avro_type.children) {
				auto child_col = TransformAvroType(child.first, child.second);
				type_children.emplace_back(child_col.name, child_col.type);
				children.push_back(std::move(child_col));
			}
			D_ASSERT(!type_children.empty());
			duckdb_type = LogicalType::STRUCT(std::move(type_children));
			break;
		}
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST: {
			if (avro_type.avro_type == AVRO_ARRAY) {
				auto element = TransformAvroType("element", avro_type.children[0].second);
				if (id == LogicalTypeId::MAP) {
					auto &key_type = element.children[0].type;
					auto &value_type = element.children[1].type;
					duckdb_type = LogicalType::MAP(key_type, value_type);
					MultiFileColumnDefinition key_value("key_value", element.type);
					key_value.children = std::move(element.children);
					children.push_back(key_value);
				} else {
					duckdb_type = LogicalType::LIST(element.type);
					children.push_back(std::move(element));
				}
			} else {
				child_list_t<LogicalType> type_children;
				auto key = TransformAvroType("key", avro_type.children[0].second);
				auto value = TransformAvroType("value", avro_type.children[1].second);

				type_children.emplace_back(key.name, key.type);
				type_children.emplace_back(value.name, value.type);
				auto key_value_type = LogicalType::STRUCT(std::move(type_children));
				duckdb_type = LogicalType::MAP(key_value_type);

				MultiFileColumnDefinition key_value("key_value", key_value_type);
				key_value.children.push_back(std::move(key));
				key_value.children.push_back(std::move(value));
				children.push_back(key_value);
			}
			break;
		}
		case LogicalTypeId::UNION: {
			for (auto &child : avro_type.children) {
				if (child.second.duckdb_type == LogicalTypeId::SQLNULL) {
					continue;
				}
				auto member = TransformAvroType(child.first, child.second);
				children.push_back(std::move(member));
			}
			if (children.size() == 1) {
				children[0].name = name;

				if (avro_type.HasFieldId()) {
					children[0].identifier = Value::INTEGER(avro_type.GetFieldId());
				}
				return std::move(children[0]);
			}
			if (children.empty()) {
				if (avro_type.children.empty()) {
					throw InvalidInputException("Empty union type");
				}
				auto res = MultiFileColumnDefinition(name, LogicalType::SQLNULL);
				if (avro_type.HasFieldId()) {
					res.identifier = Value::INTEGER(avro_type.GetFieldId());
				}
				return res;
			}

			child_list_t<LogicalType> type_children;
			for (auto &child : children) {
				type_children.emplace_back(child.name, child.type);
			}
			duckdb_type = LogicalType::UNION(std::move(type_children));
			break;
		}
		default:
			duckdb_type = LogicalType(avro_type.duckdb_type);
			break;
		}

		MultiFileColumnDefinition result(name, duckdb_type);
		result.children = std::move(children);
		if (avro_type.HasFieldId()) {
			result.identifier = Value::INTEGER(avro_type.GetFieldId());
		}
		return result;
	}

public:
	LogicalType duckdb_type;
	avro_type_t avro_type;
	child_list_t<AvroType> children;
	unordered_map<idx_t, optional_idx> union_child_map;
	int32_t field_id = NumericLimits<int32_t>::Maximum();
};

} // namespace duckdb
