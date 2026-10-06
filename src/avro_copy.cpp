#include "avro_copy.hpp"

#include "field_ids.hpp"

#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <jansson.h>
#include <unordered_set>

namespace duckdb {

namespace avro {

using cxx::LogicalType;
using cxx::Vector;

namespace {

struct JsonDeleter {
	void operator()(json_t *json) const {
		json_decref(json);
	}
};
using json_ptr_t = std::unique_ptr<json_t, JsonDeleter>;

//! Renders JSON the way the schema and metadata of the file are stored: compact, with keys in insertion order
std::string WriteJSON(json_t *json, const char *what) {
	auto data = json_dumps(json, JSON_COMPACT | JSON_PRESERVE_ORDER);
	if (!data) {
		throw InvalidInputError(std::string("Could not create a JSON representation of the ") + what);
	}
	std::string result(data);
	free(data);
	return result;
}

idx_t NextPowerOfTwo(idx_t v) {
	idx_t result = 1;
	while (result < v) {
		result <<= 1;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Options
//===--------------------------------------------------------------------===//
//! The options of a COPY statement
class CopyOptions {
public:
	explicit CopyOptions(cxx::CopyFunction::CopyToBindInput &input) {
		for (idx_t i = 0; i < input.GetOptionCount(); i++) {
			options.emplace_back(input.GetOptionName(i), input.GetOptionValue(i));
		}
		recognized.resize(options.size(), false);
	}

public:
	//! The value of an option, which is marked as recognized - nullptr if the statement does not set it
	const cxx::Value *Find(const std::string &name) {
		for (idx_t i = 0; i < options.size(); i++) {
			if (StringEqualsCaseInsensitive(options[i].first, name)) {
				recognized[i] = true;
				return &options[i].second;
			}
		}
		return nullptr;
	}
	//! The value of an option that requires one - an option given without a value reads as true
	const cxx::Value *FindWithValue(const std::string &name) {
		auto value = Find(name);
		if (value && IsBare(*value)) {
			throw InvalidInputError(name + " can not be provided without a value");
		}
		return value;
	}
	void VerifyAllRecognized() const {
		std::string unrecognized_options;
		for (idx_t i = 0; i < options.size(); i++) {
			if (recognized[i]) {
				continue;
			}
			if (!unrecognized_options.empty()) {
				unrecognized_options += ", ";
			}
			unrecognized_options += "key: \"" + options[i].first + "\"";
			if (!IsBare(options[i].second)) {
				unrecognized_options += " with value: '" + options[i].second.ToText() + "'";
			}
		}
		if (!unrecognized_options.empty()) {
			throw InvalidConfigurationError("The following option(s) are not recognized: " + unrecognized_options);
		}
	}

private:
	static bool IsBare(const cxx::Value &value) {
		return !value.IsNull() && value.GetLogicalType().GetTypeId() == LogicalTypeId::BOOLEAN && value.Get<bool>();
	}

private:
	std::vector<std::pair<std::string, cxx::Value>> options;
	std::vector<bool> recognized;
};

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
std::string ConvertTypeToAvro(const LogicalType &type) {
	switch (type.GetTypeId()) {
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "bytes";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::SQLNULL:
		return "null";
	case LogicalTypeId::STRUCT:
		return "record";
	case LogicalTypeId::DATE:
		return "int";
	case LogicalTypeId::TIME:
		return "long";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
		// captures
		// timestamp-micros
		return "long";
	case LogicalTypeId::TIMESTAMP_TZ:
		// timestamp tz will capture
		// local-timestamp-micros
		return "long";
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
		return "fixed";
	case LogicalTypeId::LIST:
		return "array";
	case LogicalTypeId::MAP:
		//! This uses a 'logicalType': map, and a struct as 'items'
		return "array";
	case LogicalTypeId::ENUM:
		//! FIXME: this should be implemented at some point
	default:
		throw NotImplementedError("Can't convert logical type '" + type.ToText() + "' to Avro type");
	}
}

bool IsTemporal(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		return false;
	}
}

std::string GetTemporalLogicalType(const LogicalType &type) {
	switch (type.GetTypeId()) {
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time-micros";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamp-micros";
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp-nanos";
	default:
		throw NotImplementedError("Can't convert logical type '" + type.ToText() + "' to Avro temporal type");
	}
}

uint32_t MinBytesRequiredForDecimal(int32_t precision) {
	// Number of bits needed: ceil(precision * log2(10)) + 1 (sign bit)
	// log2(10) ~ 10/3, but more precisely we use the fact that
	// 10^P requires ceil(P * log2(10)) bits.
	// Exact bit counts per precision bracket:
	static constexpr int32_t BITS_REQUIRED[] = {
	    0,   // precision 0 (unused)
	    4,   // 1  -> max 9
	    7,   // 2  -> max 99
	    10,  // 3  -> max 999
	    14,  // 4  -> max 9999
	    17,  // 5
	    20,  // 6
	    24,  // 7
	    27,  // 8
	    30,  // 9
	    34,  // 10
	    37,  // 11
	    40,  // 12
	    44,  // 13
	    47,  // 14
	    50,  // 15
	    54,  // 16
	    57,  // 17
	    60,  // 18
	    64,  // 19
	    67,  // 20
	    70,  // 21
	    74,  // 22
	    77,  // 23
	    80,  // 24
	    84,  // 25
	    87,  // 26
	    90,  // 27
	    94,  // 28
	    97,  // 29
	    100, // 30
	    103, // 31
	    107, // 32
	    110, // 33
	    113, // 34
	    117, // 35
	    120, // 36
	    123, // 37
	    127, // 38
	};
	return (BITS_REQUIRED[precision] + 7) / 8; // ceil(bits / 8)
}

bool IsNamedSchema(const LogicalType &type) {
	switch (type.GetTypeId()) {
	//! NOTE: 'fixed' is also part of this, but we don't have that type in DuckDB
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

constexpr const char *EMPTY_STRUCT_MARKER = "__duckdb_empty_struct_marker";

class JSONSchemaGenerator {
private:
	struct MapKeyValueIds {
		int64_t key_id;
		int64_t value_id;
	};

public:
	JSONSchemaGenerator(cxx::Context &context, const std::vector<std::string> &names,
	                    const std::vector<LogicalType> &types)
	    : context(context), names(names), types(types) {
	}

public:
	void ParseFieldIds(CopyOptions &options) {
		auto value = options.FindWithValue("FIELD_IDS");
		if (value) {
			field_ids = FieldIDUtils::ParseFieldIds(context, *value, names, types);
		}
	}
	void ParseRootName(CopyOptions &options) {
		auto value = options.FindWithValue("ROOT_NAME");
		if (!value) {
			return;
		}
		if (value->GetLogicalType().GetTypeId() != LogicalTypeId::VARCHAR) {
			throw InvalidInputError("'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name "
			                        "of the top level 'record'");
		}
		root_name = std::string(value->Get<cxx::varchar_t>().view());
	}
	void ParseSanitizeFieldNames(CopyOptions &options) {
		auto value = options.Find("SANITIZE_FIELD_NAMES");
		if (!value) {
			return;
		}
		if (value->IsNull() || value->GetLogicalType().GetTypeId() != LogicalTypeId::BOOLEAN) {
			throw InvalidInputError("SANITIZE_FIELD_NAMES requires a non-NULL BOOLEAN value");
		}
		sanitize_field_names = value->Get<bool>();
	}

	std::string GenerateJSON() {
		VerifyAvroName(root_name);
		VerifyNamedSchemaUniqueness(root_name);
		json_ptr_t root_object(json_object());
		json_object_set_new(root_object.get(), "type", json_string("record"));
		json_object_set_new(root_object.get(), "name", json_string(root_name.c_str()));
		auto fields = json_array();
		json_object_set_new(root_object.get(), "fields", fields);

		std::unordered_map<std::string, std::string> field_names;
		//! Add all the fields
		for (idx_t i = 0; i < names.size(); i++) {
			json_array_append_new(
			    fields, CreateStructField(names[i], types[i], field_ids.Find(names[i]), field_names).release());
		}
		return WriteJSON(root_object.get(), "table schema");
	}

private:
	void VerifyAvroName(const std::string &name) {
		for (idx_t i = 0; i < name.size(); i++) {
			auto c = static_cast<unsigned char>(name[i]);
			if (!(isalpha(c) || c == '_' || (i && isdigit(c)))) {
				throw InvalidInputError("'" + name +
				                        "' is not a valid Avro identifier\nThe identifier has to match the "
				                        "regex: [A-Za-z_][A-Za-z0-9_]*");
			}
		}
	}

	void VerifyNamedSchemaUniqueness(const std::string &name) {
		auto res = named_schemas.insert(name);
		if (!res.second) {
			throw BinderError("Avro schema by the name of '" + name +
			                  "' already exists, names of 'record', 'enum' and 'fixed' types have to be distinct");
		}
	}

	std::string GenerateSchemaName(const std::string &base) {
		auto res = base + std::to_string(generated_name_id++);
		VerifyAvroName(res);
		VerifyNamedSchemaUniqueness(res);
		return res;
	}

	static json_ptr_t WrapTypeInObject(json_ptr_t type_val) {
		if (json_is_object(type_val.get())) {
			return type_val;
		}
		json_ptr_t object(json_object());
		json_object_set_new(object.get(), "type", type_val.release());
		return object;
	}

	//! Wraps a type in a union with null
	static json_ptr_t MakeNullable(json_ptr_t type_val) {
		json_ptr_t union_array(json_array());
		json_array_append_new(union_array.get(), json_string("null"));
		json_array_append_new(union_array.get(), type_val.release());
		return union_array;
	}

	json_ptr_t CreateJSONType(const LogicalType &type, const FieldID *field_id,
	                          const char *preset_schema_name = nullptr) {
		auto avro_type_str = ConvertTypeToAvro(type);
		json_ptr_t type_val(json_string(avro_type_str.c_str()));
		auto type_id = type.GetTypeId();

		if (type_id == LogicalTypeId::STRUCT) {
			type_val = WrapTypeInObject(std::move(type_val));
			auto fields = json_array();
			json_object_set_new(type_val.get(), "fields", fields);
			std::unordered_map<std::string, std::string> field_names;
			for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
				auto child_name = type.GetStructChildName(i);
				if (child_name == EMPTY_STRUCT_MARKER) {
					continue;
				}
				auto child_field_id = GetChildFieldIdByName(field_id, child_name);
				json_array_append_new(
				    fields,
				    CreateStructField(child_name, type.GetStructChildType(i), child_field_id, field_names).release());
			}
		} else if (type_id == LogicalTypeId::LIST) {
			type_val = WrapTypeInObject(std::move(type_val));

			auto list_child = type.GetListChildType();
			auto element_field_id = GetChildFieldIdByName(field_id, "list");
			json_ptr_t items_type_val;
			if (list_child.GetTypeId() == LogicalTypeId::STRUCT && element_field_id) {
				auto element_schema_name = "r" + std::to_string(element_field_id->GetFieldId());
				items_type_val = CreateJSONType(list_child, element_field_id, element_schema_name.c_str());
			} else {
				items_type_val = CreateJSONType(list_child, element_field_id);
			}

			if (!element_field_id || element_field_id->nullable) {
				items_type_val = MakeNullable(std::move(items_type_val));
			}
			json_object_set_new(type_val.get(), "items", items_type_val.release());

			if (element_field_id) {
				json_object_set_new(type_val.get(), "element-id", json_integer(element_field_id->GetFieldId()));
			}
		} else if (type_id == LogicalTypeId::MAP) {
			type_val = WrapTypeInObject(std::move(type_val));

			json_object_set_new(type_val.get(), "logicalType", json_string("map"));

			json_ptr_t map_items_val;
			MapKeyValueIds key_value_ids;
			auto key_value_type = MapEntriesType(type);
			if (!GetMapKeyValueIds(field_id, key_value_ids)) {
				map_items_val = CreateJSONType(key_value_type, field_id);
			} else {
				auto map_schema_name =
				    "k" + std::to_string(key_value_ids.key_id) + "_v" + std::to_string(key_value_ids.value_id);
				map_items_val = CreateJSONType(key_value_type, field_id, map_schema_name.c_str());
			}
			json_object_set_new(type_val.get(), "items", map_items_val.release());
		} else if (IsTemporal(type_id)) {
			type_val = WrapTypeInObject(std::move(type_val));
			json_object_set_new(type_val.get(), "logicalType", json_string(GetTemporalLogicalType(type).c_str()));
			if (type_id == LogicalTypeId::TIMESTAMP_TZ) {
				json_object_set_new(type_val.get(), "adjust-to-utc", json_true());
			}
		} else if (type_id == LogicalTypeId::DECIMAL) {
			type_val = WrapTypeInObject(std::move(type_val));
			auto scale = type.GetDecimalScale();
			auto width = type.GetDecimalWidth();
			json_object_set_new(type_val.get(), "logicalType", json_string("decimal"));
			json_object_set_new(type_val.get(), "scale", json_integer(scale));
			json_object_set_new(type_val.get(), "precision", json_integer(width));
			json_object_set_new(type_val.get(), "size", json_integer(MinBytesRequiredForDecimal(width)));
		} else if (type_id == LogicalTypeId::UUID) {
			type_val = WrapTypeInObject(std::move(type_val));
			json_object_set_new(type_val.get(), "logicalType", json_string("uuid"));
			json_object_set_new(type_val.get(), "size", json_integer(16));
		}

		if (IsNamedSchema(type)) {
			if (preset_schema_name) {
				VerifyAvroName(preset_schema_name);
				VerifyNamedSchemaUniqueness(preset_schema_name);
				json_object_set_new(type_val.get(), "name", json_string(preset_schema_name));
			} else {
				auto named_schema = GenerateSchemaName(avro_type_str);
				json_object_set_new(type_val.get(), "name", json_string(named_schema.c_str()));
			}
		}
		return type_val;
	}

	std::string SanitizeFieldName(const std::string &name, std::unordered_map<std::string, std::string> &field_names) {
		if (!sanitize_field_names) {
			return name;
		}
		if (name.empty()) {
			throw InvalidInputError("Cannot sanitize an empty Avro field name");
		}
		// Follow Iceberg's AvroSchemaUtil escaping for ASCII. Escape UTF-8 bytes as well,
		// since the Avro identifier grammar only permits ASCII letters and digits.
		std::string result;
		result.reserve(name.size());
		for (idx_t i = 0; i < name.size(); i++) {
			auto c = static_cast<unsigned char>(name[i]);
			auto digit = c >= '0' && c <= '9';
			if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (i && digit)) {
				result += static_cast<char>(c);
			} else if (digit) {
				result += '_';
				result += static_cast<char>(c);
			} else {
				char escaped[8];
				snprintf(escaped, sizeof(escaped), "_x%X", static_cast<unsigned int>(c));
				result += escaped;
			}
		}
		auto entry = field_names.emplace(result, name);
		if (!entry.second) {
			throw BinderError("SANITIZE_FIELD_NAMES maps both '" + entry.first->second + "' and '" + name + "' to '" +
			                  result + "' in the same Avro record");
		}
		return result;
	}

	json_ptr_t CreateStructField(const std::string &name, const LogicalType &type, const FieldID *field_id,
	                             std::unordered_map<std::string, std::string> &field_names) {
		auto output_name = SanitizeFieldName(name, field_names);
		auto schema_name = output_name;
		if (type.GetTypeId() == LogicalTypeId::STRUCT && field_id) {
			schema_name = "r" + std::to_string(field_id->GetFieldId());
		}
		auto struct_field_type = CreateJSONType(type, field_id, schema_name.c_str());
		if (!field_id || field_id->nullable) {
			struct_field_type = MakeNullable(std::move(struct_field_type));
		}
		json_ptr_t struct_field(json_object());
		json_object_set_new(struct_field.get(), "type", struct_field_type.release());
		if (field_id) {
			json_object_set_new(struct_field.get(), "field-id", json_integer(field_id->GetFieldId()));
		}
		json_object_set_new(struct_field.get(), "name", json_string(output_name.c_str()));
		return struct_field;
	}

	static const FieldID *GetChildFieldIdByName(const FieldID *parent, const std::string &name) {
		if (!parent) {
			return nullptr;
		}
		return parent->children.Find(name);
	}

	static bool GetMapKeyValueIds(const FieldID *field_id, MapKeyValueIds &ids) {
		if (!field_id) {
			return false;
		}
		auto key = GetChildFieldIdByName(field_id, "key");
		auto value = GetChildFieldIdByName(field_id, "value");
		if (key && value) {
			ids.key_id = key->GetFieldId();
			ids.value_id = value->GetFieldId();
			return true;
		}
		return false;
	}

	//! The STRUCT(key, value) the entries of a MAP are made of
	LogicalType MapEntriesType(const LogicalType &type) {
		std::vector<cxx::TypeParam> params;
		params.emplace_back("key", cxx::Value::Create(context, type.GetMapKeyType()));
		params.emplace_back("value", cxx::Value::Create(context, type.GetMapValueType()));
		return context.CreateType(LogicalTypeId::STRUCT, params);
	}

private:
	cxx::Context &context;
	const std::vector<std::string> &names;
	const std::vector<LogicalType> &types;

	bool sanitize_field_names = false;
	std::string root_name = "root";
	ChildFieldIDs field_ids;
	idx_t generated_name_id = 0;
	std::unordered_set<std::string> named_schemas;
};

std::string CreateJSONMetadata(CopyOptions &options) {
	auto value = options.FindWithValue("METADATA");
	if (!value) {
		return "";
	}
	auto type = value->GetLogicalType();
	if (type.GetTypeId() != LogicalTypeId::STRUCT) {
		throw InvalidInputError("'METADATA' is expected to be provided as a STRUCT of key-value string metadata");
	}

	json_ptr_t root_object(json_object());
	for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
		auto key = type.GetStructChildName(i);
		auto child_value = value->GetChild(i).ToText();
		json_object_set_new(root_object.get(), key.c_str(), json_string(child_value.c_str()));
	}
	return WriteJSON(root_object.get(), "metadata");
}

//! Parse the CODEC option (Avro object-container compression codec). Validates the value is a
//! non-empty VARCHAR and returns the lowercased codec name; empty string when unset (writer
//! defaults to "null"). The codec name itself is validated by avro-c when the writer is created
//! (it reports "Unknown codec X" for anything the library was not built with), so this stays in
//! lock-step with avro-c's actual capabilities instead of duplicating a list that could drift.
std::string ParseCodec(CopyOptions &options) {
	auto value = options.FindWithValue("CODEC");
	if (!value) {
		return "";
	}
	if (value->GetLogicalType().GetTypeId() != LogicalTypeId::VARCHAR) {
		throw InvalidInputError("'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')");
	}
	return StringLower(std::string(value->Get<cxx::varchar_t>().view()));
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//
struct WriteAvroBindData {
public:
	WriteAvroBindData(cxx::CopyFunction::CopyToBindInput &input, cxx::Context &context) {
		for (idx_t i = 0; i < input.GetColumnCount(); i++) {
			names.push_back(input.GetColumnName(i));
			types.push_back(input.GetColumnType(i));
		}

		CopyOptions options(input);
		json_metadata = CreateJSONMetadata(options);

		JSONSchemaGenerator generator(context, names, types);
		generator.ParseFieldIds(options);
		generator.ParseRootName(options);
		generator.ParseSanitizeFieldNames(options);
		json_schema = generator.GenerateJSON();

		codec = ParseCodec(options);
		options.VerifyAllRecognized();

		if (avro_schema_from_json_length(json_schema.c_str(), json_schema.size(), &schema)) {
			throw AvroError();
		}
		interface = avro_generic_class_from_schema(schema);
	}
	~WriteAvroBindData() {
		avro_schema_decref(schema);
		avro_value_iface_decref(interface);
	}

public:
	std::vector<std::string> names;
	std::vector<LogicalType> types;

	//! The schema of the file to write
	avro_schema_t schema = nullptr;
	std::string json_schema;

	std::string json_metadata;
	//! Avro object-container compression codec ("null", "deflate", "snappy", "zstandard", ...).
	//! Empty means unset -> the writer defaults to "null" (uncompressed). Passed through to
	//! avro-c's codec so the COPY writer compresses natively (no post-processing).
	std::string codec;
	//! The interface through which new avro values are created
	avro_value_iface_t *interface = nullptr;
};

//===--------------------------------------------------------------------===//
// Column Writers
//===--------------------------------------------------------------------===//
class AvroColumnWriter {
public:
	explicit AvroColumnWriter(std::string type_name) : type_name(std::move(type_name)) {
	}
	virtual ~AvroColumnWriter() = default;

	virtual void Prepare(const Vector &vector) = 0;
	virtual idx_t Write(avro_value_t *target, idx_t row) = 0;

protected:
	idx_t WriteNull(avro_value_t *target) const {
		auto union_value = *target;
		avro_value_set_branch(&union_value, 0, target);
		auto schema_type = avro_value_get_type(target);
		if (schema_type != AVRO_NULL) {
			throw InvalidInputError("Cannot insert NULL to non-nullable field of type " + type_name);
		}
		avro_value_set_null(target);
		return 1;
	}

	static avro_value_t *GetNonNullTarget(avro_value_t *target) {
		auto union_value = *target;
		avro_value_set_branch(&union_value, 1, target);
		return target;
	}

protected:
	//! The name of the type of the column, for error messages
	std::string type_name;
};

using writer_ptr_t = std::unique_ptr<AvroColumnWriter>;

//! Writes a DECIMAL as the big-endian two's complement bytes its precision requires
template <class T>
idx_t WriteDecimalAsFixedBytes(const T &value, uint8_t *bytes, uint8_t width) {
	uint64_t upper;
	uint64_t lower;
	if constexpr (std::is_same_v<T, cxx::int128_t>) {
		upper = static_cast<uint64_t>(value.upper);
		lower = value.lower;
	} else {
		lower = static_cast<uint64_t>(static_cast<int64_t>(value));
		upper = value < 0 ? ~0ULL : 0;
	}
	uint8_t full[16];
	for (idx_t i = 0; i < 8; i++) {
		full[i] = static_cast<uint8_t>(upper >> (56 - 8 * i));
		full[8 + i] = static_cast<uint8_t>(lower >> (56 - 8 * i));
	}
	auto bytes_needed = MinBytesRequiredForDecimal(width);
	memcpy(bytes, full + sizeof(full) - bytes_needed, bytes_needed);
	return bytes_needed;
}

template <class T>
class PrimitiveAvroColumnWriter : public AvroColumnWriter {
public:
	using WriteFunction = idx_t (*)(avro_value_t *, const T &, uint8_t);

public:
	PrimitiveAvroColumnWriter(std::string type_name, WriteFunction write_function, uint8_t decimal_width = 0)
	    : AvroColumnWriter(std::move(type_name)), write_function(write_function), decimal_width(decimal_width) {
	}

	void Prepare(const Vector &vector) override {
		view = vector.GetView();
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		auto idx = view.SelAt(row);
		if (!view.RowIsValid(idx)) {
			return WriteNull(target);
		}
		return write_function(GetNonNullTarget(target), view.Data<T>()[idx], decimal_width);
	}

private:
	WriteFunction write_function;
	uint8_t decimal_width;
	cxx::VectorView view {};
};

idx_t WriteBooleanValue(avro_value_t *target, const bool &value, uint8_t) {
	avro_value_set_boolean(target, value);
	return sizeof(bool);
}

idx_t WriteBlobValue(avro_value_t *target, const cxx::blob_t &value, uint8_t) {
	avro_value_set_bytes(target, (void *)value.data(), value.size());
	return value.size();
}

idx_t WriteDoubleValue(avro_value_t *target, const double &value, uint8_t) {
	avro_value_set_double(target, value);
	return sizeof(double);
}

idx_t WriteFloatValue(avro_value_t *target, const float &value, uint8_t) {
	avro_value_set_float(target, value);
	return sizeof(float);
}

idx_t WriteIntegerValue(avro_value_t *target, const int32_t &value, uint8_t) {
	avro_value_set_int(target, value);
	return sizeof(int32_t);
}

idx_t WriteBigIntValue(avro_value_t *target, const int64_t &value, uint8_t) {
	avro_value_set_long(target, value);
	return sizeof(int64_t);
}

idx_t WriteStringValue(avro_value_t *target, const cxx::varchar_t &value, uint8_t) {
	avro_value_set_string_len(target, value.data(), value.size());
	return value.size();
}

idx_t WriteDateValue(avro_value_t *target, const cxx::date_t &value, uint8_t) {
	avro_value_set_int(target, value.days);
	return sizeof(int32_t);
}

idx_t WriteTimeValue(avro_value_t *target, const cxx::dtime_t &value, uint8_t) {
	avro_value_set_long(target, value.micros);
	return sizeof(int64_t);
}

idx_t WriteTimestampValue(avro_value_t *target, const cxx::timestamp_t &value, uint8_t) {
	avro_value_set_long(target, value.micros);
	return sizeof(int64_t);
}

idx_t WriteTimestampTZValue(avro_value_t *target, const cxx::timestamp_tz_t &value, uint8_t) {
	avro_value_set_long(target, value.micros);
	return sizeof(int64_t);
}

idx_t WriteUUIDValue(avro_value_t *target, const cxx::uuid_t &value, uint8_t) {
	auto bytes = value.Decode();
	avro_value_set_fixed(target, bytes.bytes, sizeof(bytes.bytes));
	return sizeof(bytes.bytes);
}

template <class T>
idx_t WriteDecimalValue(avro_value_t *target, const T &value, uint8_t width) {
	uint8_t bytes[16];
	auto byte_count = WriteDecimalAsFixedBytes<T>(value, bytes, width);
	avro_value_set_fixed(target, bytes, byte_count);
	return byte_count;
}

class NullAvroColumnWriter : public AvroColumnWriter {
public:
	using AvroColumnWriter::AvroColumnWriter;

	void Prepare(const Vector &) override {
	}

	idx_t Write(avro_value_t *target, idx_t) override {
		return WriteNull(target);
	}
};

class StructAvroColumnWriter : public AvroColumnWriter {
public:
	StructAvroColumnWriter(std::string type_name, std::vector<idx_t> child_indexes, std::vector<writer_ptr_t> children)
	    : AvroColumnWriter(std::move(type_name)), child_indexes(std::move(child_indexes)),
	      children(std::move(children)) {
	}

	void Prepare(const Vector &vector) override {
		if (vector.GetVectorType() == cxx::VectorType::DICTIONARY) {
			vector.Flatten();
		}
		view = vector.GetView();
		for (idx_t i = 0; i < children.size(); i++) {
			children[i]->Prepare(vector.GetChild(child_indexes[i]));
		}
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		if (!view.IsValid(row)) {
			return WriteNull(target);
		}
		auto *non_null_target = GetNonNullTarget(target);
		idx_t struct_value_size = 0;
		for (idx_t i = 0; i < children.size(); i++) {
			const char *unused_name;
			avro_value_t field;
			if (avro_value_get_by_index(non_null_target, i, &field, &unused_name)) {
				throw AvroError();
			}
			struct_value_size += children[i]->Write(&field, row);
		}
		return struct_value_size + 1;
	}

private:
	std::vector<idx_t> child_indexes;
	std::vector<writer_ptr_t> children;
	cxx::VectorView view {};
};

class ListAvroColumnWriter : public AvroColumnWriter {
public:
	ListAvroColumnWriter(std::string type_name, writer_ptr_t child_writer)
	    : AvroColumnWriter(std::move(type_name)), child_writer(std::move(child_writer)) {
	}

	void Prepare(const Vector &vector) override {
		if (vector.GetVectorType() == cxx::VectorType::DICTIONARY) {
			vector.Flatten();
		}
		view = vector.GetView();
		child_writer->Prepare(vector.GetChild(0));
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		auto sel_idx = view.SelAt(row);
		if (!view.RowIsValid(sel_idx)) {
			return WriteNull(target);
		}

		auto *non_null_target = GetNonNullTarget(target);
		const auto &entry = view.Data<cxx::list_entry_t>()[sel_idx];
		idx_t list_value_size = 0;
		for (idx_t i = 0; i < entry.length; i++) {
			avro_value_t item;
			size_t unused_new_index;
			if (avro_value_append(non_null_target, &item, &unused_new_index)) {
				throw AvroError();
			}
			list_value_size += child_writer->Write(&item, entry.offset + i);
		}
		return list_value_size + 1;
	}

private:
	writer_ptr_t child_writer;
	cxx::VectorView view {};
};

writer_ptr_t CreateAvroColumnWriter(const LogicalType &type) {
	auto type_name = std::string(type.GetName());
	switch (type.GetTypeId()) {
	case LogicalTypeId::BOOLEAN:
		return std::make_unique<PrimitiveAvroColumnWriter<bool>>(type_name, WriteBooleanValue);
	case LogicalTypeId::BLOB:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::blob_t>>(type_name, WriteBlobValue);
	case LogicalTypeId::DOUBLE:
		return std::make_unique<PrimitiveAvroColumnWriter<double>>(type_name, WriteDoubleValue);
	case LogicalTypeId::FLOAT:
		return std::make_unique<PrimitiveAvroColumnWriter<float>>(type_name, WriteFloatValue);
	case LogicalTypeId::INTEGER:
		return std::make_unique<PrimitiveAvroColumnWriter<int32_t>>(type_name, WriteIntegerValue);
	case LogicalTypeId::BIGINT:
		return std::make_unique<PrimitiveAvroColumnWriter<int64_t>>(type_name, WriteBigIntValue);
	case LogicalTypeId::VARCHAR:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::varchar_t>>(type_name, WriteStringValue);
	case LogicalTypeId::DATE:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::date_t>>(type_name, WriteDateValue);
	case LogicalTypeId::TIME:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::dtime_t>>(type_name, WriteTimeValue);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::timestamp_t>>(type_name, WriteTimestampValue);
	case LogicalTypeId::TIMESTAMP_TZ:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::timestamp_tz_t>>(type_name, WriteTimestampTZValue);
	case LogicalTypeId::UUID:
		return std::make_unique<PrimitiveAvroColumnWriter<cxx::uuid_t>>(type_name, WriteUUIDValue);
	case LogicalTypeId::DECIMAL: {
		auto width = type.GetDecimalWidth();
		switch (type.GetDecimalInternalTypeId()) {
		case LogicalTypeId::SMALLINT:
			return std::make_unique<PrimitiveAvroColumnWriter<int16_t>>(type_name, WriteDecimalValue<int16_t>, width);
		case LogicalTypeId::INTEGER:
			return std::make_unique<PrimitiveAvroColumnWriter<int32_t>>(type_name, WriteDecimalValue<int32_t>, width);
		case LogicalTypeId::BIGINT:
			return std::make_unique<PrimitiveAvroColumnWriter<int64_t>>(type_name, WriteDecimalValue<int64_t>, width);
		case LogicalTypeId::HUGEINT:
			return std::make_unique<PrimitiveAvroColumnWriter<cxx::int128_t>>(type_name,
			                                                                  WriteDecimalValue<cxx::int128_t>, width);
		default:
			throw NotImplementedError("Unsupported decimal physical type");
		}
	}
	case LogicalTypeId::SQLNULL:
		return std::make_unique<NullAvroColumnWriter>(type_name);
	case LogicalTypeId::STRUCT: {
		std::vector<idx_t> child_indexes;
		std::vector<writer_ptr_t> child_writers;
		for (idx_t i = 0; i < type.GetStructChildCount(); i++) {
			if (type.GetStructChildName(i) == EMPTY_STRUCT_MARKER) {
				continue;
			}
			child_indexes.push_back(i);
			child_writers.push_back(CreateAvroColumnWriter(type.GetStructChildType(i)));
		}
		return std::make_unique<StructAvroColumnWriter>(type_name, std::move(child_indexes), std::move(child_writers));
	}
	case LogicalTypeId::MAP: {
		// the entries of a MAP are written as records of their key and value
		std::vector<writer_ptr_t> child_writers;
		child_writers.push_back(CreateAvroColumnWriter(type.GetMapKeyType()));
		child_writers.push_back(CreateAvroColumnWriter(type.GetMapValueType()));
		auto entries_writer =
		    std::make_unique<StructAvroColumnWriter>("STRUCT", std::vector<idx_t> {0, 1}, std::move(child_writers));
		return std::make_unique<ListAvroColumnWriter>(type_name, std::move(entries_writer));
	}
	case LogicalTypeId::LIST:
		return std::make_unique<ListAvroColumnWriter>(type_name, CreateAvroColumnWriter(type.GetListChildType()));
	case LogicalTypeId::ENUM:
		throw NotImplementedError("Can't convert ENUM Value to Avro yet");
	default:
		throw NotImplementedError("PopulateValue not implemented for type " + type.ToText());
	}
}

//===--------------------------------------------------------------------===//
// Writing a file
//===--------------------------------------------------------------------===//
//! A growable buffer that avro-c writes into
struct AvroInMemoryBuffer {
public:
	void Resize(idx_t new_capacity) {
		//! The old contents do not need to be kept
		data.reset(new char[new_capacity]);
		capacity = new_capacity;
	}
	void ResizeAndCopy(idx_t new_capacity) {
		std::unique_ptr<char[]> new_data(new char[new_capacity]);
		if (capacity) {
			memcpy(new_data.get(), data.get(), capacity);
		}
		data = std::move(new_data);
		capacity = new_capacity;
	}
	char *GetData() {
		return data.get();
	}
	idx_t GetCapacity() const {
		return capacity;
	}

private:
	std::unique_ptr<char[]> data;
	idx_t capacity = 0;
};

//! The state of one file being written
struct WriteAvroGlobalState {
public:
	static constexpr idx_t BUFFER_SIZE = 1024;
	//! Size of the bytes written to mark the end of a section (header/datablock)
	static constexpr idx_t SYNC_SIZE = 16;
	//! Avro uses a handrolled varint that they assert can only be 10 bytes
	static constexpr idx_t MAX_ROW_COUNT_BYTES = 10;

public:
	WriteAvroGlobalState(cxx::Context &context, const WriteAvroBindData &bind_data, const std::string &file_path)
	    : types(CopyTypes(bind_data.types)), handle(context.GetFileSystem().OpenFile(
	                                             file_path, {cxx::FileFlags::WRITE, cxx::FileFlags::FILE_CREATE_NEW})) {
		//! Guess how big the "header" of the Avro file needs to be
		idx_t capacity = std::max<idx_t>(
		    BUFFER_SIZE, NextPowerOfTwo(bind_data.json_schema.size() + SYNC_SIZE + MAX_ROW_COUNT_BYTES));
		memory_buffer.Resize(capacity);

		writer = avro_writer_memory(memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));
		datum_writer = avro_writer_memory(datum_buffer.GetData(), static_cast<int64_t>(datum_buffer.GetCapacity()));

		const char *json_metadata = bind_data.json_metadata.empty() ? nullptr : bind_data.json_metadata.c_str();
		//! Pass the compression codec straight to avro-c so the object container is written compressed
		//! natively (no post-processing). Empty -> nullptr -> avro-c default ("null"/uncompressed).
		const char *codec = bind_data.codec.empty() ? nullptr : bind_data.codec.c_str();

		int ret;
		while ((ret = avro_file_writer_create_from_writers_with_metadata_and_codec(
		            writer, datum_writer, bind_data.schema, &file_writer, json_metadata, codec)) == ENOSPC) {
			memory_buffer.Resize(NextPowerOfTwo(memory_buffer.GetCapacity() * 2));
			// re-initialize writer to use correct data location
			avro_file_writer_close(file_writer);
			file_writer = nullptr;
			writer = avro_writer_memory(memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));
			datum_writer = avro_writer_memory(datum_buffer.GetData(), static_cast<int64_t>(datum_buffer.GetCapacity()));
		}
		if (ret) {
			auto error = AvroError();
			if (!file_writer) {
				//! the writers were not handed over to a file writer (e.g. an unknown codec)
				avro_writer_free(writer);
				avro_writer_free(datum_writer);
			}
			throw error;
		}

		WriteData(memory_buffer.GetData(), static_cast<idx_t>(avro_writer_tell(writer)));
		avro_writer_memory_set_dest(writer, memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));

		if (avro_generic_value_new(bind_data.interface, &value)) {
			throw AvroError();
		}
		value_initialized = true;
		for (auto &type : bind_data.types) {
			column_writers.push_back(CreateAvroColumnWriter(type));
		}
	}
	~WriteAvroGlobalState() {
		if (value_initialized) {
			avro_value_decref(&value);
		}
		//! NOTE: the 'writer' and 'datum_writer' do not need to be closed, they are owned by the file_writer
		if (file_writer) {
			avro_file_writer_close(file_writer);
		}
	}

public:
	//! Writes the rows of a chunk to the file as one block
	void WriteChunk(const cxx::DataChunk &input) {
		auto column_count = input.GetVectorCount();
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			column_writers[col_idx]->Prepare(input.GetVector(col_idx));
		}

		idx_t count = input.GetRowCount();
		idx_t offset_in_datum_buffer = 0;

		for (idx_t i = 0; i < count; i++) {
			//! Populate our avro value, estimating the size of the value as we go
			idx_t value_size = 0;
			for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
				const char *unused_name;
				avro_value_t column;
				if (avro_value_get_by_index(&value, col_idx, &column, &unused_name)) {
					throw AvroError();
				}
				value_size += column_writers[col_idx]->Write(&column, i);
			}

			//! Prepare the datum buffer for this row
			idx_t length = datum_buffer.GetCapacity() - offset_in_datum_buffer;
			if (value_size > length) {
				//! This value is too big to fit into the remaining portion of the buffer
				datum_buffer.ResizeAndCopy(NextPowerOfTwo(datum_buffer.GetCapacity() + value_size));
			}
			SetDatumDestination(offset_in_datum_buffer);

			int ret;
			while ((ret = avro_file_writer_append_value(file_writer, &value)) == ENOSPC) {
				datum_buffer.ResizeAndCopy(NextPowerOfTwo(datum_buffer.GetCapacity() * 2));
				SetDatumDestination(offset_in_datum_buffer);
			}
			if (ret) {
				throw AvroError();
			}

			offset_in_datum_buffer = static_cast<idx_t>(avro_writer_tell(datum_writer));
			avro_value_reset(&value);
		}

		auto expected_size = static_cast<idx_t>(avro_writer_tell(datum_writer)) + SYNC_SIZE + MAX_ROW_COUNT_BYTES;
		if (expected_size > memory_buffer.GetCapacity()) {
			//! Resize the buffer in advance, to prevent any need for resizing below
			memory_buffer.Resize(NextPowerOfTwo(expected_size));
			SetDestination();
		}

		//! Flush the contents to the buffer, if it fails, resize the buffer and try again
		int ret;
		while ((ret = avro_file_writer_flush(file_writer)) == ENOSPC) {
			memory_buffer.Resize(NextPowerOfTwo(memory_buffer.GetCapacity() * 2));
			SetDestination();
		}
		if (ret) {
			throw AvroError();
		}

		WriteData(memory_buffer.GetData(), static_cast<idx_t>(avro_writer_tell(writer)));
		SetDestination();
		row_count += count;
	}

	void Close() {
		handle.Close();
	}

public:
	//! Running total of bytes written to the file, which is the size of the file once it is closed
	idx_t bytes_written = 0;
	//! Number of rows written
	idx_t row_count = 0;
	//! The types of the columns being written
	std::vector<LogicalType> types;

private:
	static std::vector<LogicalType> CopyTypes(const std::vector<LogicalType> &types) {
		std::vector<LogicalType> result;
		for (auto &type : types) {
			result.push_back(type.Copy());
		}
		return result;
	}

	void WriteData(const char *data, idx_t size) {
		while (size > 0) {
			auto written = handle.Write(data, size);
			if (written == 0) {
				throw InvalidInputError("Could not write to the Avro file");
			}
			data += written;
			size -= written;
			bytes_written += written;
		}
	}

	void SetDestination() {
		avro_writer_memory_set_dest(writer, memory_buffer.GetData(), static_cast<int64_t>(memory_buffer.GetCapacity()));
	}

	void SetDatumDestination(idx_t offset) {
		avro_writer_memory_set_dest_with_offset(datum_writer, datum_buffer.GetData(),
		                                        static_cast<int64_t>(datum_buffer.GetCapacity()),
		                                        static_cast<int64_t>(offset));
	}

private:
	//! The file handle to write to
	cxx::FileHandle handle;
	AvroInMemoryBuffer memory_buffer;
	AvroInMemoryBuffer datum_buffer;

	//! The writer for the file
	avro_writer_t writer = nullptr;
	avro_writer_t datum_writer = nullptr;
	avro_file_writer_t file_writer = nullptr;

	//! Avro value representing a row of the schema
	avro_value_t value;
	bool value_initialized = false;
	std::vector<writer_ptr_t> column_writers;
};

struct WriteAvroBatch {
	explicit WriteAvroBatch(cxx::ColumnDataCollection collection) : collection(std::move(collection)) {
	}

	cxx::ColumnDataCollection collection;
};

//===--------------------------------------------------------------------===//
// Callbacks
//===--------------------------------------------------------------------===//
void WriteAvroBind(cxx::CopyFunction::CopyToBindInput &input) {
	auto context = input.GetContext();
	input.SetBindData<WriteAvroBindData>(input, context);
}

void WriteAvroInit(cxx::CopyFunction::CopyToInitInput &input) {
	auto context = input.GetContext();
	input.SetInitData<WriteAvroGlobalState>(context, input.GetBindData<WriteAvroBindData>(), input.GetFilePath());
}

void WriteAvroPrepareBatch(cxx::CopyFunction::CopyToBatchInput &input) {
	//! The rows are encoded when the batch is flushed, as the blocks of a file are written one at a time
	input.SetBatchData<WriteAvroBatch>(input.TakeBatch());
}

void WriteAvroFlushBatch(cxx::CopyFunction::CopyToFlushInput &input) {
	auto &global_state = input.GetInitData<WriteAvroGlobalState>();
	auto &batch = input.GetBatchData<WriteAvroBatch>();
	auto shared_state = batch.collection.CreateSharedScanState();
	auto worker_state = batch.collection.CreateWorkerScanState();
	cxx::DataChunk chunk(input.GetContext(), global_state.types);
	while (batch.collection.Scan(shared_state, worker_state, chunk)) {
		global_state.WriteChunk(chunk);
	}
}

void WriteAvroFinalize(cxx::CopyFunction::CopyToFinalizeInput &input) {
	//! Every block has already been written to the file handle, so there is no more data to write here.
	//! We only need to close the handle, which is required for stores that commit on Close() (e.g.
	//! Azure DFS / OneLake, where Write() Appends staged bytes that only become visible after
	//! Flush(Close=true)). Local FS, S3, and Azure Blob are unaffected because their writes commit
	//! incrementally.
	input.GetInitData<WriteAvroGlobalState>().Close();
}

void WriteAvroStatistics(cxx::CopyFunction::CopyToStatisticsInput &input) {
	//! bytes_written is the exact, incrementally-tracked file size (no HEAD probe). The other stat
	//! columns (footer_size_bytes, column_statistics) are not applicable to the Avro object container
	//! and are left at their defaults.
	auto &global_state = input.GetInitData<WriteAvroGlobalState>();
	input.SetFileSize(global_state.bytes_written);
	input.SetRowCount(global_state.row_count);
}

} // namespace

void AvroCopyFunction::Register(cxx::Extension &extension) {
	auto function = cxx::CopyFunction::Create(extension);
	function.SetName("avro");
	function.SetCopyToBindCallback(WriteAvroBind)
	    .SetCopyToInitCallback(WriteAvroInit)
	    .SetCopyToBatchCallback(WriteAvroPrepareBatch)
	    .SetCopyToFlushCallback(WriteAvroFlushBatch)
	    .SetCopyToFinalizeCallback(WriteAvroFinalize)
	    .SetCopyToStatisticsCallback(WriteAvroStatistics);
	function.Register();
}

} // namespace avro

} // namespace duckdb
