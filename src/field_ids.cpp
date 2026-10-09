#include "field_ids.hpp"

#include <optional>
#include <unordered_set>

namespace duckdb {

namespace avro {

namespace {

//! The types of the children of a column, by lowercase name, in declaration order
using NameToTypeMap = std::vector<std::pair<std::string, cxx::LogicalType>>;

const cxx::LogicalType *FindChildType(const NameToTypeMap &map, const std::string &name) {
	for (auto &entry : map) {
		if (entry.first == name) {
			return &entry.second;
		}
	}
	return nullptr;
}

NameToTypeMap GetChildNameToTypeMap(const cxx::LogicalType &type) {
	NameToTypeMap name_to_type_map;
	switch (type.GetTypeId()) {
	case LogicalTypeId::LIST:
		name_to_type_map.emplace_back("list", type.GetListChildType());
		break;
	case LogicalTypeId::MAP:
		name_to_type_map.emplace_back("key", type.GetMapKeyType());
		name_to_type_map.emplace_back("value", type.GetMapValueType());
		break;
	case LogicalTypeId::STRUCT:
		for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
			auto child_name = type.GetStructChildName(i);
			if (child_name == FieldID::DUCKDB_FIELD_ID) {
				throw BinderError(std::string("Cannot have column named \"") + FieldID::DUCKDB_FIELD_ID +
				                  "\" with FIELD_IDS");
			}
			name_to_type_map.emplace_back(StringLower(child_name), type.GetStructChildType(i));
		}
		break;
	default: // LCOV_EXCL_START
		throw InternalError("Unexpected type in GetChildNameToTypeMap");
	} // LCOV_EXCL_STOP
	return name_to_type_map;
}

void GetFieldIDs(cxx::Context &context, const cxx::Value &field_ids_value, ChildFieldIDs &field_ids_p,
                 std::unordered_set<uint32_t> &unique_field_ids, const NameToTypeMap &name_to_type_map) {
	auto struct_type = field_ids_value.GetLogicalType();
	if (struct_type.GetTypeId() != LogicalTypeId::STRUCT) {
		throw BinderError(std::string("Expected FIELD_IDS to be a STRUCT, e.g., {col1: 42, col2: {") +
		                  FieldID::DUCKDB_FIELD_ID + ": 43, nested_col: 44}, col3: 44}");
	}
	auto &field_ids = field_ids_p.Ids();
	for (idx_t i = 0; i < struct_type.GetStructChildCount(); i++) {
		const auto col_name = StringLower(struct_type.GetStructChildName(i));
		if (col_name == FieldID::DUCKDB_FIELD_ID || col_name == FieldID::DUCKDB_NULLABLE_ID) {
			continue;
		}

		auto col_type = FindChildType(name_to_type_map, col_name);
		if (!col_type) {
			std::string names;
			for (const auto &name : name_to_type_map) {
				if (!names.empty()) {
					names += ", ";
				}
				names += name.first;
			}
			throw BinderError("Column name \"" + col_name +
			                  "\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS if this "
			                  "column is a partition column. Available column names: [" +
			                  names + "]");
		}

		auto child_value = field_ids_value.GetChild(i);
		auto child_type = child_value.GetLogicalType();
		std::optional<cxx::Value> field_id_value;
		std::optional<cxx::Value> field_id_nullable;
		bool has_child_field_ids = false;

		if (child_type.GetTypeId() == LogicalTypeId::STRUCT) {
			for (idx_t nested_i = 0; nested_i < child_type.GetStructChildCount(); nested_i++) {
				const auto field_id_or_nested_col = child_type.GetStructChildName(nested_i);
				if (field_id_or_nested_col == FieldID::DUCKDB_FIELD_ID) {
					field_id_value = child_value.GetChild(nested_i);
				} else if (field_id_or_nested_col == FieldID::DUCKDB_NULLABLE_ID) {
					field_id_nullable = child_value.GetChild(nested_i);
				} else {
					has_child_field_ids = true;
				}
			}
		} else {
			field_id_value = std::move(child_value);
		}

		FieldID field_id;
		if (field_id_value) {
			const auto field_id_int =
			    field_id_value->Cast(context, context.CreateType(LogicalTypeId::INTEGER)).Get<int32_t>();
			if (!unique_field_ids.insert(static_cast<uint32_t>(field_id_int)).second) {
				throw BinderError("Duplicate field_id " + std::to_string(field_id_int) + " found in FIELD_IDS");
			}
			if (field_id_nullable) {
				auto nullable = field_id_nullable->Cast(context, context.CreateType(LogicalTypeId::BOOLEAN));
				field_id = FieldID(field_id_int, nullable.Get<bool>());
			} else {
				field_id = FieldID(field_id_int);
			}
		}
		auto inserted = field_ids.emplace(col_name, std::move(field_id));

		if (has_child_field_ids) {
			auto type_id = col_type->GetTypeId();
			if (type_id != LogicalTypeId::LIST && type_id != LogicalTypeId::MAP && type_id != LogicalTypeId::STRUCT) {
				throw BinderError("Column \"" + col_name + "\" with type \"" + std::string(col_type->GetName()) +
				                  "\" cannot have a nested FIELD_IDS specification");
			}
			GetFieldIDs(context, field_ids_value.GetChild(i), inserted.first->second.children, unique_field_ids,
			            GetChildNameToTypeMap(*col_type));
		}
	}
}

} // namespace

const FieldID *ChildFieldIDs::Find(const std::string &name) const {
	if (!ids) {
		return nullptr;
	}
	auto entry = ids->find(StringLower(name));
	return entry == ids->end() ? nullptr : &entry->second;
}

std::unordered_map<std::string, FieldID> &ChildFieldIDs::Ids() {
	if (!ids) {
		ids = std::make_unique<std::unordered_map<std::string, FieldID>>();
	}
	return *ids;
}

FieldID::FieldID() : set(false), field_id(0) {
}

FieldID::FieldID(int32_t field_id_p, bool nullable) : set(true), field_id(field_id_p), nullable(nullable) {
}

int32_t FieldID::GetFieldId() const {
	if (!set) {
		throw InternalError("Field id of the field is not set");
	}
	return field_id;
}

ChildFieldIDs FieldIDUtils::ParseFieldIds(cxx::Context &context, const cxx::Value &input,
                                          const std::vector<std::string> &names,
                                          const std::vector<cxx::LogicalType> &types) {
	std::unordered_set<uint32_t> unique_field_ids;
	NameToTypeMap name_to_type_map;
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		if (names[col_idx] == FieldID::DUCKDB_FIELD_ID) {
			throw BinderError(std::string("Cannot have a column named \"") + FieldID::DUCKDB_FIELD_ID +
			                  "\" when writing FIELD_IDS");
		}
		name_to_type_map.emplace_back(StringLower(names[col_idx]), types[col_idx].Copy());
	}

	ChildFieldIDs result;
	GetFieldIDs(context, input, result, unique_field_ids, name_to_type_map);
	return result;
}

} // namespace avro

} // namespace duckdb
