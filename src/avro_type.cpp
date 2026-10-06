#include "avro_type.hpp"

#include <cstring>

namespace duckdb {

namespace avro {

namespace {

struct AvroLogicalType {
	LogicalTypeId id = LogicalTypeId::INVALID;
	uint8_t width = 0;
	uint8_t scale = 0;
};

AvroLogicalType AvroLogicalTypeToLogicalType(avro_schema_t avro_schema) {
	AvroLogicalType result;
	auto logical_type_raw = avro_schema_logical_type(avro_schema);
	if (!logical_type_raw) {
		return result;
	}
	// any nested types are handled switch case in TransformSchema
	switch (avro_typeof(avro_schema)) {
	case AVRO_ARRAY:
	case AVRO_ENUM:
	case AVRO_MAP:
	case AVRO_RECORD:
		return result;
	default:
		break;
	}
	std::string logical_type = logical_type_raw;
	if (logical_type == "date") {
		result.id = LogicalTypeId::DATE;
	} else if (logical_type == "decimal") {
		result.id = LogicalTypeId::DECIMAL;
		result.width = static_cast<uint8_t>(avro_schema_precision(avro_schema));
		result.scale = static_cast<uint8_t>(avro_schema_scale(avro_schema));
	} else if (logical_type == "time-micros" || logical_type == "time-millis") {
		result.id = LogicalTypeId::TIME;
	} else if (logical_type == "timestamp-micros" || logical_type == "timestamp-millis") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		// -1 doesn't exist
		result.id = adjust_to_utc > 0 ? LogicalTypeId::TIMESTAMP_TZ : LogicalTypeId::TIMESTAMP;
	} else if (logical_type == "timestamp-nanos") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		if (adjust_to_utc > 0) {
			throw NotImplementedError("Avro timestamp-nanos with adjust_to_utc not supported");
		}
		result.id = LogicalTypeId::TIMESTAMP_NS;
	} else if (logical_type == "uuid") {
		auto size = avro_schema_fixed_size(avro_schema);
		if (size != 16) {
			throw InvalidConfigurationError("logical type is uuid, but size != 16");
		}
		result.id = LogicalTypeId::UUID;
	} else if (logical_type == "local-timestamp-millis") {
		result.id = LogicalTypeId::TIMESTAMP;
	} else {
		throw NotImplementedError("Unknown Avro logical type " + logical_type);
	}
	return result;
}

AvroType PrimitiveType(avro_type_t avro_type, LogicalTypeId default_type, const AvroLogicalType &logical_type) {
	if (logical_type.id == LogicalTypeId::INVALID) {
		return AvroType(avro_type, default_type);
	}
	AvroType result(avro_type, logical_type.id);
	result.decimal_width = logical_type.width;
	result.decimal_scale = logical_type.scale;
	return result;
}

cxx::LogicalType CreateNamedType(cxx::Context &context, LogicalTypeId id,
                                 const std::vector<AvroColumn> &children) {
	std::vector<cxx::TypeParam> params;
	for (auto &child : children) {
		params.emplace_back(child.name, cxx::Value::Create(context, child.type));
	}
	return context.CreateType(id, params);
}

cxx::LogicalType CreatePositionalType(cxx::Context &context, LogicalTypeId id,
                                      std::vector<const cxx::LogicalType *> children) {
	std::vector<cxx::TypeParam> params;
	for (auto child : children) {
		params.emplace_back(cxx::Value::Create(context, *child));
	}
	return context.CreateType(id, params);
}

cxx::LogicalType CreatePrimitiveType(cxx::Context &context, const AvroType &avro_type) {
	switch (avro_type.type_id) {
	case LogicalTypeId::SQLNULL:
		return CreateNullType(context);
	case LogicalTypeId::DECIMAL: {
		std::vector<cxx::TypeParam> params;
		params.emplace_back(cxx::Value::Create(context, static_cast<int64_t>(avro_type.decimal_width)));
		params.emplace_back(cxx::Value::Create(context, static_cast<int64_t>(avro_type.decimal_scale)));
		return context.CreateType(LogicalTypeId::DECIMAL, params);
	}
	case LogicalTypeId::ENUM: {
		std::vector<cxx::TypeParam> params;
		for (auto &symbol : avro_type.enum_symbols) {
			params.emplace_back(cxx::Value::Create(context, cxx::varchar_t(symbol)));
		}
		return context.CreateType(LogicalTypeId::ENUM, params);
	}
	default:
		return context.CreateType(avro_type.type_id);
	}
}

void SetFieldId(AvroColumn &column, const AvroType &avro_type) {
	if (avro_type.HasFieldId()) {
		column.field_id = avro_type.field_id;
	}
}

} // namespace

AvroType TransformSchema(avro_schema_t avro_schema, std::unordered_set<std::string> parent_schema_names) {
	auto logical_type = AvroLogicalTypeToLogicalType(avro_schema);

	auto raw_lt = avro_schema_logical_type(avro_schema);
	bool is_millis = raw_lt && (std::string(raw_lt) == "timestamp-millis" || std::string(raw_lt) == "time-millis" ||
	                            std::string(raw_lt) == "local-timestamp-millis");

	switch (avro_typeof(avro_schema)) {
	case AVRO_NULL:
		return AvroType(AVRO_NULL, LogicalTypeId::SQLNULL);
	case AVRO_BOOLEAN:
		return AvroType(AVRO_BOOLEAN, LogicalTypeId::BOOLEAN);
	case AVRO_INT32: {
		auto result = PrimitiveType(AVRO_INT32, LogicalTypeId::INTEGER, logical_type);
		result.is_timestamp_millis = is_millis;
		return result;
	}
	case AVRO_INT64: {
		auto result = PrimitiveType(AVRO_INT64, LogicalTypeId::BIGINT, logical_type);
		result.is_timestamp_millis = is_millis;
		return result;
	}
	case AVRO_FLOAT:
		return PrimitiveType(AVRO_FLOAT, LogicalTypeId::FLOAT, logical_type);
	case AVRO_DOUBLE:
		return PrimitiveType(AVRO_DOUBLE, LogicalTypeId::DOUBLE, logical_type);
	case AVRO_BYTES:
		return PrimitiveType(AVRO_BYTES, LogicalTypeId::BLOB, logical_type);
	case AVRO_STRING:
		return PrimitiveType(AVRO_STRING, LogicalTypeId::VARCHAR, logical_type);
	case AVRO_UNION: {
		auto num_children = avro_schema_union_size(avro_schema);
		std::vector<std::pair<std::string, AvroType>> union_children;
		idx_t non_null_child_idx = 0;
		std::unordered_map<idx_t, idx_t> union_child_map;
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_union_branch(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names);
			if (child_type.type_id != LogicalTypeId::SQLNULL) {
				union_child_map[child_idx] = non_null_child_idx++;
			}
			union_children.emplace_back("u" + std::to_string(child_idx), std::move(child_type));
		}
		return AvroType(AVRO_UNION, LogicalTypeId::UNION, std::move(union_children), std::move(union_child_map));
	}
	case AVRO_RECORD: {
		auto schema_name = std::string(avro_schema_name(avro_schema));
		if (parent_schema_names.find(schema_name) != parent_schema_names.end()) {
			throw InvalidInputError("Recursive Avro types not supported: " + schema_name);
		}
		parent_schema_names.insert(schema_name);

		auto num_children = avro_schema_record_size(avro_schema);
		if (num_children == 0) {
			// this we just ignore but we need a marker so we don't get our offsets
			// wrong
			return AvroType(AVRO_RECORD, LogicalTypeId::SQLNULL);
		}
		std::vector<std::pair<std::string, AvroType>> struct_children;
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_record_field_get_by_index(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names);
			child_type.field_id = avro_schema_record_field_id(avro_schema, child_idx);
			auto child_name = avro_schema_record_field_name(avro_schema, child_idx);
			if (!child_name || strlen(child_name) == 0) {
				throw InvalidInputError("Empty avro field name");
			}
			struct_children.emplace_back(child_name, std::move(child_type));
		}
		return AvroType(AVRO_RECORD, LogicalTypeId::STRUCT, std::move(struct_children));
	}
	case AVRO_ENUM: {
		auto size = avro_schema_enum_number_of_symbols(avro_schema);
		AvroType result(AVRO_ENUM, LogicalTypeId::ENUM);
		for (idx_t enum_idx = 0; enum_idx < static_cast<idx_t>(size); enum_idx++) {
			result.enum_symbols.emplace_back(avro_schema_enum_get(avro_schema, static_cast<int>(enum_idx)));
		}
		return result;
	}
	case AVRO_FIXED:
		return PrimitiveType(AVRO_FIXED, LogicalTypeId::BLOB, logical_type);
	case AVRO_ARRAY: {
		auto child_schema = avro_schema_array_items(avro_schema);
		auto element_id = avro_schema_array_element_id(avro_schema);
		auto child_type = TransformSchema(child_schema, parent_schema_names);
		child_type.field_id = element_id;
		std::vector<std::pair<std::string, AvroType>> list_children;
		list_children.emplace_back("list_entry", std::move(child_type));
		bool is_map = avro_schema_array_is_map(avro_schema);
		return AvroType(AVRO_ARRAY, is_map ? LogicalTypeId::MAP : LogicalTypeId::LIST, std::move(list_children));
	}
	case AVRO_MAP: {
		auto child_schema = avro_schema_map_values(avro_schema);
		auto key_id = avro_schema_map_key_id(avro_schema);
		auto value_id = avro_schema_map_value_id(avro_schema);

		AvroType key_type(AVRO_STRING, LogicalTypeId::VARCHAR);
		key_type.field_id = key_id;
		auto value_type = TransformSchema(child_schema, parent_schema_names);
		value_type.field_id = value_id;

		std::vector<std::pair<std::string, AvroType>> map_children;
		map_children.emplace_back("key_entry", std::move(key_type));
		map_children.emplace_back("value_entry", std::move(value_type));
		return AvroType(AVRO_MAP, LogicalTypeId::MAP, std::move(map_children));
	}
	case AVRO_LINK: {
		auto target = avro_schema_link_target(avro_schema);
		return TransformSchema(target, parent_schema_names);
	}
	default:
		throw NotImplementedError(std::string("Unknown Avro Type ") + avro_schema_type_name(avro_schema));
	}
}

AvroColumn TransformAvroType(cxx::Context &context, const std::string &name, const AvroType &avro_type) {
	std::vector<AvroColumn> children;
	auto id = avro_type.type_id;
	switch (id) {
	case LogicalTypeId::STRUCT: {
		for (auto &child : avro_type.children) {
			children.push_back(TransformAvroType(context, child.first, child.second));
		}
		auto type = CreateNamedType(context, LogicalTypeId::STRUCT, children);
		AvroColumn result(name, std::move(type));
		result.children = std::move(children);
		SetFieldId(result, avro_type);
		return result;
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		if (avro_type.avro_type == AVRO_ARRAY) {
			auto element = TransformAvroType(context, "list", avro_type.children[0].second);
			if (id == LogicalTypeId::LIST) {
				auto type = CreatePositionalType(context, LogicalTypeId::LIST, {&element.type});
				AvroColumn result(name, std::move(type));
				result.children.push_back(std::move(element));
				SetFieldId(result, avro_type);
				return result;
			}
			if (element.type.GetTypeId() != LogicalTypeId::STRUCT || element.children.size() != 2) {
				throw InvalidInputError("Avro array with logical type 'map' must contain key/value records");
			}
			children = std::move(element.children);
		} else {
			children.push_back(TransformAvroType(context, "key", avro_type.children[0].second));
			children.push_back(TransformAvroType(context, "value", avro_type.children[1].second));
		}
		children[0].name = "key";
		children[1].name = "value";
		auto type = CreatePositionalType(context, LogicalTypeId::MAP, {&children[0].type, &children[1].type});
		AvroColumn key_value("key_value", CreateNamedType(context, LogicalTypeId::STRUCT, children));
		key_value.children = std::move(children);
		AvroColumn result(name, std::move(type));
		result.children.push_back(std::move(key_value));
		SetFieldId(result, avro_type);
		return result;
	}
	case LogicalTypeId::UNION: {
		for (auto &child : avro_type.children) {
			if (child.second.type_id == LogicalTypeId::SQLNULL) {
				continue;
			}
			children.push_back(TransformAvroType(context, child.first, child.second));
		}
		if (children.size() == 1) {
			auto result = std::move(children[0]);
			result.name = name;
			if (avro_type.HasFieldId()) {
				result.field_id = avro_type.field_id;
			}
			return result;
		}
		if (children.empty()) {
			if (avro_type.children.empty()) {
				throw InvalidInputError("Empty union type");
			}
			AvroColumn result(name, CreateNullType(context));
			SetFieldId(result, avro_type);
			return result;
		}
		auto type = CreateNamedType(context, LogicalTypeId::UNION, children);
		AvroColumn result(name, std::move(type));
		result.children = std::move(children);
		SetFieldId(result, avro_type);
		return result;
	}
	default: {
		AvroColumn result(name, CreatePrimitiveType(context, avro_type));
		SetFieldId(result, avro_type);
		return result;
	}
	}
}

} // namespace avro

} // namespace duckdb
