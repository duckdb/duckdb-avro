#include "field_ids.hpp"
#include "include/field_ids.hpp"

#include "duckdb/common/exception/list.hpp"

namespace duckdb {

namespace avro {

constexpr const char *FieldID::DUCKDB_FIELD_ID;

FieldID::FieldID() : set(false) {
}

FieldID::FieldID(int32_t field_id_p, bool nullable) : set(true), field_id(field_id_p), nullable(nullable) {
}

int32_t FieldID::GetFieldId() const {
	D_ASSERT(set);
	return field_id;
}

static case_insensitive_map_t<LogicalType> GetChildNameToTypeMap(const LogicalType &type) {
	case_insensitive_map_t<LogicalType> name_to_type_map;
	switch (type.id()) {
	case LogicalTypeId::LIST:
		name_to_type_map.emplace("list", ListType::GetChildType(type));
		break;
	case LogicalTypeId::MAP:
		name_to_type_map.emplace("key", MapType::KeyType(type));
		name_to_type_map.emplace("value", MapType::ValueType(type));
		break;
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			if (child_type.first == FieldID::DUCKDB_FIELD_ID) {
				throw BinderException("Cannot have column named \"%s\" with FIELD_IDS", FieldID::DUCKDB_FIELD_ID);
			}
			name_to_type_map.emplace(child_type);
		}
		break;
	default: // LCOV_EXCL_START
		throw InternalException("Unexpected type in GetChildNameToTypeMap");
	} // LCOV_EXCL_STOP
	return name_to_type_map;
}

static void GetFieldIDs(const Value &field_ids_value, case_insensitive_map_t<FieldID> &field_ids,
                        unordered_set<uint32_t> &unique_field_ids,
                        const case_insensitive_map_t<LogicalType> &name_to_type_map) {
	const auto &struct_type = field_ids_value.type();
	if (struct_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException(
		    "Expected FIELD_IDS to be a STRUCT, e.g., {col1: 42, col2: {%s: 43, nested_col: 44}, col3: 44}",
		    FieldID::DUCKDB_FIELD_ID);
	}
	const auto &struct_children = StructValue::GetChildren(field_ids_value);
	D_ASSERT(StructType::GetChildTypes(struct_type).size() == struct_children.size());
	for (idx_t i = 0; i < struct_children.size(); i++) {
		const auto &col_name = StringUtil::Lower(StructType::GetChildName(struct_type, i));
		if (col_name == FieldID::DUCKDB_FIELD_ID || col_name == FieldID::DUCKDB_NULLABLE_ID) {
			continue;
		}

		auto it = name_to_type_map.find(col_name);
		if (it == name_to_type_map.end()) {
			string names;
			for (const auto &name : name_to_type_map) {
				if (!names.empty()) {
					names += ", ";
				}
				names += name.first;
			}
			throw BinderException(
			    "Column name \"%s\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS if this "
			    "column is a partition column. Available column names: [%s]",
			    col_name, names);
		}
		D_ASSERT(field_ids.find(col_name) == field_ids.end()); // Caught by STRUCT - deduplicates keys

		const auto &child_value = struct_children[i];
		const auto &child_type = child_value.type();
		optional_ptr<const Value> field_id_value;
		optional_ptr<const Value> field_id_nullable;
		optional_ptr<const Value> child_field_ids_value;

		if (child_type.id() == LogicalTypeId::STRUCT) {
			const auto &nested_children = StructValue::GetChildren(child_value);
			D_ASSERT(StructType::GetChildTypes(child_type).size() == nested_children.size());
			for (idx_t nested_i = 0; nested_i < nested_children.size(); nested_i++) {
				const auto &field_id_or_nested_col = StructType::GetChildName(child_type, nested_i);
				if (field_id_or_nested_col == FieldID::DUCKDB_FIELD_ID) {
					field_id_value = &nested_children[nested_i];
				} else if (field_id_or_nested_col == FieldID::DUCKDB_NULLABLE_ID) {
					field_id_nullable = &nested_children[nested_i];
				} else {
					child_field_ids_value = &child_value;
				}
			}
		} else {
			field_id_value = &child_value;
		}

		FieldID field_id;
		if (field_id_value) {
			Value field_id_integer_value = field_id_value->DefaultCastAs(LogicalType::INTEGER);
			const uint32_t field_id_int = IntegerValue::Get(field_id_integer_value);
			if (!unique_field_ids.insert(field_id_int).second) {
				throw BinderException("Duplicate field_id %s found in FIELD_IDS", field_id_integer_value.ToString());
			}
			if (field_id_nullable) {
				Value field_id_bool_value = field_id_nullable->DefaultCastAs(LogicalType::BOOLEAN);
				const bool field_id_bool = BooleanValue::Get(field_id_bool_value);
				field_id = FieldID(UnsafeNumericCast<int32_t>(field_id_int), field_id_bool);
			} else {
				field_id = FieldID(UnsafeNumericCast<int32_t>(field_id_int));
			}
		}
		auto inserted = field_ids.emplace(col_name, std::move(field_id));
		D_ASSERT(inserted.second);

		if (child_field_ids_value) {
			const auto &col_type = it->second;
			if (col_type.id() != LogicalTypeId::LIST && col_type.id() != LogicalTypeId::MAP &&
			    col_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("Column \"%s\" with type \"%s\" cannot have a nested FIELD_IDS specification",
				                      col_name, LogicalTypeIdToString(col_type.id()));
			}

			GetFieldIDs(*child_field_ids_value, inserted.first->second.children, unique_field_ids,
			            GetChildNameToTypeMap(col_type));
		}
	}
}

case_insensitive_map_t<FieldID> FieldIDUtils::ParseFieldIds(const Value &input, const vector<string> &names,
                                                            const vector<LogicalType> &types) {
	unordered_set<uint32_t> unique_field_ids;
	case_insensitive_map_t<LogicalType> name_to_type_map;
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		if (names[col_idx] == FieldID::DUCKDB_FIELD_ID) {
			throw BinderException("Cannot have a column named \"%s\" when writing FIELD_IDS", FieldID::DUCKDB_FIELD_ID);
		}
		name_to_type_map.emplace(names[col_idx], types[col_idx]);
	}

	case_insensitive_map_t<FieldID> result;
	GetFieldIDs(input, result, unique_field_ids, name_to_type_map);
	return result;
}

} // namespace avro

} // namespace duckdb
