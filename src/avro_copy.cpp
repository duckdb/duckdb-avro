#include "avro_copy.hpp"

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/function/function.hpp"
#include "yyjson.hpp"
#include "duckdb/common/printer.hpp"
#include "field_ids.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "errno.h"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) {
		yyjson_doc_free(doc);
	}
	void operator()(yyjson_mut_doc *doc) {
		yyjson_mut_doc_free(doc);
	}
};

} // namespace

static string ConvertTypeToAvro(const LogicalType &type) {
	switch (type.id()) {
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
	case LogicalTypeId::TIME: {
		return "long";
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS: {
		// captures
		// timestamp-micros
		return "long";
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		// timestamp tz will capture
		// local-timestamp-micros
		return "long";
	}
	case LogicalTypeId::UUID:
	case LogicalTypeId::DECIMAL: {
		return "fixed";
	}
	case LogicalTypeId::ENUM:
		//! FIXME: this should be implemented at some point
		throw NotImplementedException("Can't convert logical type '%s' to Avro type", type.ToString());
	case LogicalTypeId::LIST:
		return "array";
	case LogicalTypeId::MAP:
		//! This uses a 'logicalType': map, and a struct as 'items'
		return "array";
	default:
		throw NotImplementedException("Can't convert logical type '%s' to Avro type", type.ToString());
	};

	//! FIXME: we don't have support for 'FIXED' currently (a fixed size blob)
}

static string GetTemporalLogicalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME: {
		return "time-micros";
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ: {
		return "timestamp-micros";
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		return "timestamp-nanos";
	}
	default:
		throw NotImplementedException("Can't convert logical type '%s' to Avro temporal type", type.ToString());
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
	auto ret = (BITS_REQUIRED[precision] + 7) / 8; // ceil(bits / 8)
	return ret;
}

static bool IsNamedSchema(const LogicalType &type) {
	switch (type.id()) {
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

struct JSONSchemaGenerator {
private:
	struct MapKeyValueIds {
		int64_t key_id;
		int64_t value_id;
	};

public:
	JSONSchemaGenerator(const vector<string> &names, const vector<LogicalType> &types) : names(names), types(types) {
		doc = yyjson_mut_doc_new(nullptr);
		root_object = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root_object);
	}
	~JSONSchemaGenerator() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}

public:
	void ParseFieldIds(const case_insensitive_map_t<vector<Value>> &options, case_insensitive_set_t &recognized) {
		auto it = options.find("FIELD_IDS");
		if (it == options.end()) {
			return;
		}
		if (it->second.empty()) {
			throw InvalidInputException("FIELD_IDS can not be provided without a value");
		}
		field_ids = avro::FieldIDUtils::ParseFieldIds(it->second[0], names, types);
		recognized.insert(it->first);
	}
	void ParseRootName(const case_insensitive_map_t<vector<Value>> &options, case_insensitive_set_t &recognized) {
		auto it = options.find("ROOT_NAME");
		if (it == options.end()) {
			return;
		}
		if (it->second.empty()) {
			throw InvalidInputException("ROOT_NAME can not be provided without a value");
		}
		auto &value = it->second[0];
		if (value.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name "
			                            "of the top level 'record'");
		}
		root_name = value.GetValue<string>();
		recognized.insert(it->first);
	}

public:
	void VerifyAvroName(const string &name) {
		D_ASSERT(!name.empty());
		for (idx_t i = 0; i < name.size(); i++) {
			if (!(isalpha(name[i]) || name[i] == '_' || (i && isdigit(name[i])))) {
				throw InvalidInputException("'%s' is not a valid Avro identifier\nThe identifier has to match the "
				                            "regex: [A-Za-z_][A-Za-z0-9_]*",
				                            name);
			}
		}
	}

	string GenerateSchemaName(const string &base) {
		auto res = StringUtil::Format("%s%d", base, generated_name_id++);
		VerifyAvroName(res);
		VerifyNamedSchemaUniqueness(res);
		return res;
	}

	bool IsJSONObject(yyjson_mut_val *val) {
		auto type = yyjson_mut_get_type(val);
		return type == YYJSON_TYPE_OBJ;
	}

	yyjson_mut_val *WrapTypeInObject(yyjson_mut_doc *doc, yyjson_mut_val *type_val) {
		if (IsJSONObject(type_val)) {
			return type_val;
		}
		auto object = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_val(doc, object, "type", type_val);
		return object;
	}

	yyjson_mut_val *CreateJSONType(const LogicalType &type, optional_ptr<avro::FieldID> field_id,
	                               const char *preset_schema_name = nullptr) {
		auto avro_type_str = ConvertTypeToAvro(type);
		auto type_val = yyjson_mut_strcpy(doc, avro_type_str.c_str());
		auto type_id = type.id();

		if (type_id == LogicalTypeId::STRUCT) {
			type_val = WrapTypeInObject(doc, type_val);
			auto &struct_children = StructType::GetChildTypes(type);
			auto fields = yyjson_mut_obj_add_arr(doc, type_val, "fields");
			for (auto &it : struct_children) {
				auto &child_name = it.first;
				if (child_name == "__duckdb_empty_struct_marker") {
					continue;
				}
				auto &child_type = it.second;
				auto child_field_id = GetChildFieldIdByName(field_id, child_name.GetIdentifierName());

				auto struct_field = CreateStructField(child_name.GetIdentifierName(), child_type, child_field_id);
				yyjson_mut_arr_add_val(fields, struct_field);
			}
		} else if (type_id == LogicalTypeId::LIST) {
			type_val = WrapTypeInObject(doc, type_val);

			auto &list_child = ListType::GetChildType(type);
			auto element_field_id = GetChildFieldIdByName(field_id, "list");
			yyjson_mut_val *items_type_val;
			if (list_child.id() == LogicalTypeId::STRUCT && element_field_id) {
				auto preset_schema_name = StringUtil::Format("r%d", element_field_id->GetFieldId());
				items_type_val = CreateJSONType(list_child, element_field_id, preset_schema_name.c_str());
			} else {
				items_type_val = CreateJSONType(list_child, element_field_id);
			}

			if (!element_field_id || element_field_id->nullable) {
				auto union_array = yyjson_mut_arr(doc);
				yyjson_mut_arr_add_strcpy(doc, union_array, "null");
				yyjson_mut_arr_add_val(union_array, items_type_val);
				items_type_val = union_array;
			}
			yyjson_mut_obj_add_val(doc, type_val, "items", items_type_val);

			if (element_field_id) {
				yyjson_mut_obj_add_int(doc, type_val, "element-id", element_field_id->GetFieldId());
			}
		} else if (type_id == LogicalTypeId::MAP) {
			type_val = WrapTypeInObject(doc, type_val);

			yyjson_mut_obj_add_strcpy(doc, type_val, "logicalType", "map");

			yyjson_mut_val *map_items_val;
			MapKeyValueIds key_value_ids;
			auto &list_child = ListType::GetChildType(type);
			D_ASSERT(list_child.id() == LogicalTypeId::STRUCT);

			if (!GetMapKeyValueIds(field_id, key_value_ids)) {
				map_items_val = CreateJSONType(list_child, field_id);
			} else {
				auto preset_schema_name = StringUtil::Format("k%d_v%d", key_value_ids.key_id, key_value_ids.value_id);
				map_items_val = CreateJSONType(list_child, field_id, preset_schema_name.c_str());
			}
			yyjson_mut_obj_add_val(doc, type_val, "items", map_items_val);
		} else if (type.IsTemporal()) {
			type_val = WrapTypeInObject(doc, type_val);
			yyjson_mut_obj_add_strcpy(doc, type_val, "logicalType", GetTemporalLogicalType(type).c_str());
			if (type == LogicalTypeId::TIMESTAMP_TZ) {
				yyjson_mut_obj_add_bool(doc, type_val, "adjust-to-utc", true);
			}
		} else if (type_id == LogicalTypeId::DECIMAL) {
			type_val = WrapTypeInObject(doc, type_val);
			auto scale = DecimalType::GetScale(type);
			auto width = DecimalType::GetWidth(type);
			yyjson_mut_obj_add_strcpy(doc, type_val, "logicalType", "decimal");
			yyjson_mut_obj_add_uint(doc, type_val, "scale", scale);
			yyjson_mut_obj_add_uint(doc, type_val, "precision", width);
			yyjson_mut_obj_add_uint(doc, type_val, "size", MinBytesRequiredForDecimal(width));
		} else if (type_id == LogicalTypeId::UUID) {
			type_val = WrapTypeInObject(doc, type_val);
			yyjson_mut_obj_add_strcpy(doc, type_val, "logicalType", "uuid");
			yyjson_mut_obj_add_uint(doc, type_val, "size", 16);
		} else if (type.IsNested()) {
			throw NotImplementedException("Can't convert nested type '%s' to Avro", type.ToString());
		}

		if (IsNamedSchema(type)) {
			D_ASSERT(IsJSONObject(type_val));
			if (preset_schema_name) {
				yyjson_mut_obj_add_strcpy(doc, type_val, "name", preset_schema_name);
			} else {
				auto named_schema = GenerateSchemaName(avro_type_str);
				yyjson_mut_obj_add_strcpy(doc, type_val, "name", named_schema.c_str());
			}
		}
		return type_val;
	}

	string GenerateJSON() {
		yyjson_mut_obj_add_str(doc, root_object, "type", "record");
		yyjson_mut_obj_add_strcpy(doc, root_object, "name", root_name.c_str());
		auto array = yyjson_mut_obj_add_arr(doc, root_object, "fields");

		//! Add all the fields
		D_ASSERT(names.size() == types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			auto &type = types[i];
			optional_ptr<avro::FieldID> field_id;
			auto &children = field_ids.Ids();
			auto it = children.find(name);
			if (it != children.end()) {
				field_id = it->second;
			}
			yyjson_mut_arr_add_val(array, CreateStructField(name, type, field_id));
		}

		//! Write the result to a string
		auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
		if (!data) {
			yyjson_mut_doc_free(doc);
			throw InvalidInputException("Could not create a JSON representation of the table schema, yyjson failed");
		}
		auto res = string(data);
		free(data);
		return res;
	}

private:
	void VerifyNamedSchemaUniqueness(const string &name) {
		auto res = named_schemas.insert(name);
		if (!res.second) {
			throw BinderException("Avro schema by the name of '%s' already exists, names of 'record', 'enum' and "
			                      "'fixed' types have to be distinct",
			                      name);
		}
	}

	yyjson_mut_val *CreateStructField(const string &name, const LogicalType &type,
	                                  optional_ptr<avro::FieldID> field_id) {
		auto struct_field = yyjson_mut_obj(doc);
		const char *struct_name = name.c_str();
		auto struct_field_type = CreateJSONType(type, field_id, struct_name);
		if (!field_id || field_id->nullable) {
			auto union_array = yyjson_mut_arr(doc);
			yyjson_mut_arr_add_strcpy(doc, union_array, "null");
			yyjson_mut_arr_add_val(union_array, struct_field_type);
			struct_field_type = union_array;
		}
		yyjson_mut_obj_add_val(doc, struct_field, "type", struct_field_type);
		if (field_id) {
			yyjson_mut_obj_add_uint(doc, struct_field, "field-id", field_id->GetFieldId());
		}
		yyjson_mut_obj_add_strcpy(doc, struct_field, "name", name.c_str());
		return struct_field;
	}

	optional_ptr<avro::FieldID> GetChildFieldIdByName(optional_ptr<avro::FieldID> parent, const string &name) {
		if (!parent) {
			return nullptr;
		}
		auto &children = parent->children.Ids();
		auto it = children.find(name);
		if (it != children.end()) {
			return it->second;
		}
		return nullptr;
	}

	bool GetMapKeyValueIds(optional_ptr<avro::FieldID> field_id, MapKeyValueIds &ids) {
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

public:
	const vector<string> &names;
	const vector<LogicalType> &types;

	string root_name = "root";
	avro::ChildFieldIDs field_ids;
	idx_t generated_name_id = 0;
	yyjson_mut_doc *doc = nullptr;
	yyjson_mut_val *root_object;
	unordered_set<string> named_schemas;
};

static string CreateJSONSchema(const case_insensitive_map_t<vector<Value>> &options, const vector<string> &names,
                               const vector<LogicalType> &types, case_insensitive_set_t &recognized) {
	JSONSchemaGenerator state(names, types);

	state.ParseFieldIds(options, recognized);
	state.ParseRootName(options, recognized);
	return state.GenerateJSON();
}

static string CreateJSONMetadata(const case_insensitive_map_t<vector<Value>> &options,
                                 case_insensitive_set_t &recognized) {
	auto it = options.find("METADATA");
	if (it == options.end()) {
		return "";
	}

	if (it->second.empty()) {
		throw InvalidInputException("METADATA can not be provided without a value");
	}
	auto &value = it->second[0];
	if (value.type().id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("'METADATA' is expected to be provided as a STRUCT of key-value string metadata");
	}
	recognized.insert(it->first);

	unordered_map<string, string> metadata;
	auto &children = StructValue::GetChildren(value);
	auto &child_types = StructType::GetChildTypes(value.type());
	for (idx_t i = 0; i < children.size(); i++) {
		auto &child = children[i];
		auto &child_name = child_types[i].first;
		metadata[child_name.GetIdentifierName()] = child.ToString();
	}

	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	yyjson_mut_val *root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);

	for (auto &item : metadata) {
		auto &key = item.first;
		auto &value = item.second;
		yyjson_mut_obj_add_strcpy(doc, root_object, key.c_str(), value.c_str());
	}

	//! Write the result to a string
	auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
	if (!data) {
		throw InvalidInputException("Could not create a JSON representation of the metadata, yyjson failed");
	}
	auto res = string(data);
	free(data);
	return res;
}

//! Parse the CODEC option (Avro object-container compression codec). Validates the value is a
//! non-empty VARCHAR and returns the lowercased codec name; empty string when unset (writer
//! defaults to "null"). The codec name itself is validated by avro-c when the writer is created
//! (it reports "Unknown codec X" for anything the library was not built with), so this stays in
//! lock-step with avro-c's actual capabilities instead of duplicating a list that could drift.
static string ParseCodec(const case_insensitive_map_t<vector<Value>> &options,
                         case_insensitive_set_t &recognized) {
	auto it = options.find("CODEC");
	if (it == options.end()) {
		return "";
	}
	if (it->second.empty()) {
		throw InvalidInputException("CODEC can not be provided without a value");
	}
	auto &value = it->second[0];
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')");
	}
	recognized.insert(it->first);
	return StringUtil::Lower(value.GetValue<string>());
}

WriteAvroBindData::WriteAvroBindData(CopyFunctionBindInput &input, const vector<Identifier> &names,
                                     const vector<LogicalType> &types)
    : names(IdentifiersToStrings(names)), types(types) {

	case_insensitive_set_t recognized;

	json_metadata = CreateJSONMetadata(input.info.options, recognized);
	json_schema = CreateJSONSchema(input.info.options, this->names, types, recognized);
	codec = ParseCodec(input.info.options, recognized);

	vector<string> unrecognized_options;
	for (auto &option : input.info.options) {
		if (recognized.count(option.first)) {
			continue;
		}
		auto key = option.first;
		if (option.second.empty()) {
			unrecognized_options.push_back(StringUtil::Format("key: '%s'", key));
		} else {
			unrecognized_options.push_back(
			    StringUtil::Format("key: '%s' with value: '%s'", key, option.second[0].ToString()));
		}
	}
	if (!unrecognized_options.empty()) {
		throw InvalidConfigurationException("The following option(s) are not recognized: %s",
		                                    StringUtil::Join(unrecognized_options, ", "));
	}
	if (avro_schema_from_json_length(json_schema.c_str(), json_schema.size(), &schema)) {
		throw InvalidInputException(avro_strerror());
	}
	interface = avro_generic_class_from_schema(schema);
}

WriteAvroBindData::~WriteAvroBindData() {
	avro_schema_decref(schema);
	avro_value_iface_decref(interface);
}

WriteAvroGlobalState::~WriteAvroGlobalState() {
	//! NOTE: the 'writer' and 'datum_writer' do not need to be closed, they are owned by the file_writer
	avro_file_writer_close(file_writer);
}

WriteAvroGlobalState::WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs,
                                           const string &file_path)
    : allocator(Allocator::Get(context)), memory_buffer(allocator), datum_buffer(allocator), fs(fs) {
	handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
	                                    FileLockType::WRITE_LOCK | FileCompressionType::AUTO_DETECT);
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	//! Guess how big the "header" of the Avro file needs to be
	idx_t capacity =
	    MaxValue<idx_t>(BUFFER_SIZE, NextPowerOfTwo(bind_data.json_schema.size() + SYNC_SIZE + MAX_ROW_COUNT_BYTES));
	memory_buffer.Resize(capacity);

	int ret;
	writer = avro_writer_memory(const_char_ptr_cast(memory_buffer.GetData()), memory_buffer.GetCapacity());
	datum_writer = avro_writer_memory(const_char_ptr_cast(datum_buffer.GetData()), datum_buffer.GetCapacity());

	const char *json_metadata = nullptr;
	if (!bind_data.json_metadata.empty()) {
		json_metadata = bind_data.json_metadata.c_str();
	}

	//! Pass the compression codec straight to avro-c so the object container is written compressed
	//! natively (no post-processing). Empty -> nullptr -> avro-c default ("null"/uncompressed).
	const char *codec = bind_data.codec.empty() ? nullptr : bind_data.codec.c_str();

	while ((ret = avro_file_writer_create_from_writers_with_metadata_and_codec(
	            writer, datum_writer, bind_data.schema, &file_writer, json_metadata, codec)) == ENOSPC) {
		auto current_capacity = memory_buffer.GetCapacity();
		memory_buffer.Resize(NextPowerOfTwo(current_capacity * 2));
		// re-initialize writer to use correct data location
		avro_file_writer_close(file_writer);
		writer = avro_writer_memory(const_char_ptr_cast(memory_buffer.GetData()), memory_buffer.GetCapacity());
		datum_writer = avro_writer_memory(const_char_ptr_cast(datum_buffer.GetData()), datum_buffer.GetCapacity());
	}
	if (ret) {
		throw InvalidInputException(avro_strerror());
	}

	auto written_bytes = avro_writer_tell(writer);
	WriteData(memory_buffer.GetData(), written_bytes);
	avro_writer_memory_set_dest(writer, (const char *)memory_buffer.GetData(), memory_buffer.GetCapacity());
}

static unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input,
                                              const vector<Identifier> &names, const vector<LogicalType> &sql_types) {
	auto res = make_uniq<WriteAvroBindData>(input, names, sql_types);
	return std::move(res);
}

static unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto res = make_uniq<WriteAvroLocalState>(bind_data_p);
	return std::move(res);
}

static unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                                const string &file_path) {
	auto res = make_uniq<WriteAvroGlobalState>(context, bind_data_p, FileSystem::GetFileSystem(context), file_path);
	return std::move(res);
}

class AvroColumnWriter {
public:
	virtual ~AvroColumnWriter() = default;

	virtual void Prepare(Vector &vector) = 0;
	virtual idx_t Write(avro_value_t *target, idx_t row) = 0;

protected:
	static idx_t WriteNull(avro_value_t *target, const LogicalType &type) {
		auto union_value = *target;
		avro_value_set_branch(&union_value, 0, target);
		auto schema_type = avro_value_get_type(target);
		if (schema_type != AVRO_NULL) {
			throw InvalidInputException("Cannot insert NULL to non-nullable field of type %s",
			                            LogicalTypeIdToString(type.id()));
		}
		avro_value_set_null(target);
		return 1;
	}

	static avro_value_t *GetNonNullTarget(avro_value_t *target) {
		auto union_value = *target;
		avro_value_set_branch(&union_value, 1, target);
		return target;
	}
};

template <typename T, typename ShiftT = T>
static idx_t WriteDecimalAsFixedBytes(T value, uint8_t *bytes, const LogicalType &type) {
	auto bytes_needed = MinBytesRequiredForDecimal(DecimalType::GetWidth(type));
	auto type_bytes = static_cast<int>(sizeof(T));
	auto start = type_bytes - static_cast<int>(bytes_needed);
	for (int i = start; i < type_bytes; i++) {
		bytes[i - start] = static_cast<uint8_t>(static_cast<ShiftT>(value >> ((type_bytes - i - 1) * 8)));
	}
	return bytes_needed;
}

static idx_t WriteBooleanValue(avro_value_t *target, const bool &value, const LogicalType &) {
	avro_value_set_boolean(target, value);
	return sizeof(bool);
}

static idx_t WriteBlobValue(avro_value_t *target, const string_t &value, const LogicalType &) {
	avro_value_set_bytes(target, (void *)value.GetData(), value.GetSize());
	return value.GetSize();
}

static idx_t WriteDoubleValue(avro_value_t *target, const double &value, const LogicalType &) {
	avro_value_set_double(target, value);
	return sizeof(double);
}

static idx_t WriteFloatValue(avro_value_t *target, const float &value, const LogicalType &) {
	avro_value_set_float(target, value);
	return sizeof(float);
}

static idx_t WriteIntegerValue(avro_value_t *target, const int32_t &value, const LogicalType &) {
	avro_value_set_int(target, value);
	return sizeof(int32_t);
}

static idx_t WriteBigIntValue(avro_value_t *target, const int64_t &value, const LogicalType &) {
	avro_value_set_long(target, value);
	return sizeof(int64_t);
}

static idx_t WriteStringValue(avro_value_t *target, const string_t &value, const LogicalType &) {
	avro_value_set_string_len(target, value.GetData(), value.GetSize() + 1);
	return value.GetSize();
}

static idx_t WriteDateValue(avro_value_t *target, const date_t &value, const LogicalType &) {
	avro_value_set_int(target, Date::EpochDays(value));
	return sizeof(int32_t);
}

static idx_t WriteTimeValue(avro_value_t *target, const dtime_t &value, const LogicalType &) {
	avro_value_set_long(target, value.value);
	return sizeof(int64_t);
}

static idx_t WriteTimestampValue(avro_value_t *target, const timestamp_t &value, const LogicalType &) {
	avro_value_set_long(target, value.value);
	return sizeof(int64_t);
}

static idx_t WriteTimestampTZValue(avro_value_t *target, const timestamp_tz_t &value, const LogicalType &) {
	avro_value_set_long(target, value.value);
	return sizeof(int64_t);
}

static idx_t WriteUUIDValue(avro_value_t *target, const hugeint_t &value, const LogicalType &) {
	uint8_t bytes[16];
	BaseUUID::ToBlob(value, data_ptr_cast(bytes));
	avro_value_set_fixed(target, bytes, 16);
	return 16;
}

template <typename T, typename ShiftT = T>
static idx_t WriteDecimalValue(avro_value_t *target, const T &value, const LogicalType &type) {
	uint8_t bytes[16];
	auto byte_count = WriteDecimalAsFixedBytes<T, ShiftT>(value, bytes, type);
	avro_value_set_fixed(target, bytes, byte_count);
	return byte_count;
}

template <class T>
class PrimitiveAvroColumnWriter : public AvroColumnWriter {
public:
	using WriteFunction = idx_t (*)(avro_value_t *, const T &, const LogicalType &);

public:
	PrimitiveAvroColumnWriter(LogicalType type, WriteFunction write_function)
	    : type(std::move(type)), write_function(write_function) {
	}

	void Prepare(Vector &vector) override {
		iterator = make_uniq<VectorIterator<T>>(vector);
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		auto entry = (*iterator)[row];
		if (!entry.IsValid()) {
			return WriteNull(target, type);
		}
		return write_function(GetNonNullTarget(target), entry.GetValueUnsafe(), type);
	}

private:
	LogicalType type;
	WriteFunction write_function;
	unique_ptr<VectorIterator<T>> iterator;
};

class NullAvroColumnWriter : public AvroColumnWriter {
public:
	explicit NullAvroColumnWriter(LogicalType type) : type(std::move(type)) {
	}

	void Prepare(Vector &) override {
	}

	idx_t Write(avro_value_t *target, idx_t) override {
		return WriteNull(target, type);
	}

private:
	LogicalType type;
};

class StructAvroColumnWriter : public AvroColumnWriter {
public:
	StructAvroColumnWriter(LogicalType type, vector<idx_t> child_indexes, vector<unique_ptr<AvroColumnWriter>> children)
	    : type(std::move(type)), child_indexes(std::move(child_indexes)), children(std::move(children)) {
	}

	void Prepare(Vector &vector) override {
		if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			vector.Flatten();
		}
		validity = make_uniq<VectorValidityIterator>(vector);
		auto &entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < children.size(); i++) {
			children[i]->Prepare(entries[child_indexes[i]]);
		}
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		if (!validity->IsValid(row)) {
			return WriteNull(target, type);
		}
		auto *non_null_target = GetNonNullTarget(target);
		idx_t struct_value_size = 0;
		for (idx_t i = 0; i < children.size(); i++) {
			const char *unused_name;
			avro_value_t field;
			if (avro_value_get_by_index(non_null_target, i, &field, &unused_name)) {
				throw InvalidInputException(avro_strerror());
			}
			struct_value_size += children[i]->Write(&field, row);
		}
		return struct_value_size + 1;
	}

private:
	LogicalType type;
	vector<idx_t> child_indexes;
	vector<unique_ptr<AvroColumnWriter>> children;
	unique_ptr<VectorValidityIterator> validity;
};

class ListAvroColumnWriter : public AvroColumnWriter {
public:
	ListAvroColumnWriter(LogicalType type, unique_ptr<AvroColumnWriter> child_writer)
	    : type(std::move(type)), child_writer(std::move(child_writer)) {
	}

	void Prepare(Vector &vector) override {
		vector.ToUnifiedFormat(format);
		list_data = UnifiedVectorFormat::GetData<list_entry_t>(format);
		auto &child = ListVector::GetChildMutable(vector);
		child_writer->Prepare(child);
	}

	idx_t Write(avro_value_t *target, idx_t row) override {
		auto sel_idx = format.sel->get_index(row);
		if (!format.validity.RowIsValid(sel_idx)) {
			return WriteNull(target, type);
		}

		auto *non_null_target = GetNonNullTarget(target);
		const auto &entry = list_data[sel_idx];
		idx_t list_value_size = 0;
		for (idx_t i = 0; i < entry.length; i++) {
			avro_value_t item;
			size_t unused_new_index;
			if (avro_value_append(non_null_target, &item, &unused_new_index)) {
				throw InvalidInputException(avro_strerror());
			}
			list_value_size += child_writer->Write(&item, entry.offset + i);
		}
		return list_value_size + 1;
	}

private:
	LogicalType type;
	unique_ptr<AvroColumnWriter> child_writer;
	UnifiedVectorFormat format;
	const list_entry_t *list_data = nullptr;
};

static unique_ptr<AvroColumnWriter> CreateAvroColumnWriter(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<PrimitiveAvroColumnWriter<bool>>(type, WriteBooleanValue);
	case LogicalTypeId::BLOB:
		return make_uniq<PrimitiveAvroColumnWriter<string_t>>(type, WriteBlobValue);
	case LogicalTypeId::DOUBLE:
		return make_uniq<PrimitiveAvroColumnWriter<double>>(type, WriteDoubleValue);
	case LogicalTypeId::FLOAT:
		return make_uniq<PrimitiveAvroColumnWriter<float>>(type, WriteFloatValue);
	case LogicalTypeId::INTEGER:
		return make_uniq<PrimitiveAvroColumnWriter<int32_t>>(type, WriteIntegerValue);
	case LogicalTypeId::BIGINT:
		return make_uniq<PrimitiveAvroColumnWriter<int64_t>>(type, WriteBigIntValue);
	case LogicalTypeId::VARCHAR:
		return make_uniq<PrimitiveAvroColumnWriter<string_t>>(type, WriteStringValue);
	case LogicalTypeId::DATE:
		return make_uniq<PrimitiveAvroColumnWriter<date_t>>(type, WriteDateValue);
	case LogicalTypeId::TIME:
		return make_uniq<PrimitiveAvroColumnWriter<dtime_t>>(type, WriteTimeValue);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return make_uniq<PrimitiveAvroColumnWriter<timestamp_t>>(type, WriteTimestampValue);
	case LogicalTypeId::TIMESTAMP_TZ:
		return make_uniq<PrimitiveAvroColumnWriter<timestamp_tz_t>>(type, WriteTimestampTZValue);
	case LogicalTypeId::UUID:
		return make_uniq<PrimitiveAvroColumnWriter<hugeint_t>>(type, WriteUUIDValue);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return make_uniq<PrimitiveAvroColumnWriter<int16_t>>(type, WriteDecimalValue<int16_t>);
		case PhysicalType::INT32:
			return make_uniq<PrimitiveAvroColumnWriter<int32_t>>(type, WriteDecimalValue<int32_t>);
		case PhysicalType::INT64:
			return make_uniq<PrimitiveAvroColumnWriter<int64_t>>(type, WriteDecimalValue<int64_t>);
		case PhysicalType::INT128:
			return make_uniq<PrimitiveAvroColumnWriter<hugeint_t>>(type, WriteDecimalValue<hugeint_t, uhugeint_t>);
		default:
			throw NotImplementedException("Unsupported decimal physical type");
		}
	case LogicalTypeId::SQLNULL:
		return make_uniq<NullAvroColumnWriter>(type);
	case LogicalTypeId::STRUCT: {
		vector<idx_t> child_indexes;
		vector<unique_ptr<AvroColumnWriter>> child_writers;
		auto &child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (child_types[i].first == "__duckdb_empty_struct_marker") {
				continue;
			}
			child_indexes.push_back(i);
			child_writers.push_back(CreateAvroColumnWriter(child_types[i].second));
		}
		return make_uniq<StructAvroColumnWriter>(type, std::move(child_indexes), std::move(child_writers));
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
		return make_uniq<ListAvroColumnWriter>(type, CreateAvroColumnWriter(ListType::GetChildType(type)));
	case LogicalTypeId::ENUM:
		throw NotImplementedException("Can't convert ENUM Value to Avro yet");
	default:
		throw NotImplementedException("PopulateValue not implemented for type %s", type.ToString());
	}
}

WriteAvroLocalState::WriteAvroLocalState(FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();
	avro_generic_value_new(bind_data.interface, &value);
	column_writers.reserve(bind_data.types.size());
	for (auto &type : bind_data.types) {
		column_writers.push_back(CreateAvroColumnWriter(type));
	}
}

WriteAvroLocalState::~WriteAvroLocalState() {
	avro_value_decref(&value);
}

static void WriteAvroSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                          LocalFunctionData &lstate_p, DataChunk &input) {
	auto &global_state = gstate_p.Cast<WriteAvroGlobalState>();
	auto &local_state = lstate_p.Cast<WriteAvroLocalState>();

	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		local_state.column_writers[col_idx]->Prepare(input.data[col_idx]);
	}

	auto &datum_buffer = global_state.datum_buffer;
	idx_t count = input.size();
	idx_t offset_in_datum_buffer = 0;

	for (idx_t i = 0; i < count; i++) {

		//! Populate our avro value, estimating the size of the value as we go
		idx_t value_size = 0;
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			const char *unused_name;
			avro_value_t column;
			if (avro_value_get_by_index(&local_state.value, col_idx, &column, &unused_name)) {
				throw InvalidInputException(avro_strerror());
			}
			value_size += local_state.column_writers[col_idx]->Write(&column, i);
		}

		//! Prepare the datum buffer for this row
		idx_t length = datum_buffer.GetCapacity() - offset_in_datum_buffer;
		if (value_size > length) {
			//! This value is too big to fit into the remaining portion of the buffer
			idx_t new_capacity = datum_buffer.GetCapacity();
			new_capacity += value_size;
			datum_buffer.ResizeAndCopy(NextPowerOfTwo(new_capacity));
		}
		avro_writer_memory_set_dest_with_offset(global_state.datum_writer, (const char *)datum_buffer.GetData(),
		                                        datum_buffer.GetCapacity(), offset_in_datum_buffer);

		int ret;
		while ((ret = avro_file_writer_append_value(global_state.file_writer, &local_state.value)) == ENOSPC) {
			auto current_capacity = datum_buffer.GetCapacity();
			datum_buffer.ResizeAndCopy(NextPowerOfTwo(current_capacity * 2));
			avro_writer_memory_set_dest_with_offset(global_state.datum_writer, (const char *)datum_buffer.GetData(),
			                                        datum_buffer.GetCapacity(), offset_in_datum_buffer);
		}
		if (ret) {
			throw InvalidInputException(avro_strerror());
		}

		offset_in_datum_buffer = avro_writer_tell(global_state.datum_writer);
		avro_value_reset(&local_state.value);
	}

	auto &buffer = global_state.memory_buffer;
	auto expected_size = avro_writer_tell(global_state.datum_writer);
	expected_size += WriteAvroGlobalState::SYNC_SIZE + WriteAvroGlobalState::MAX_ROW_COUNT_BYTES;
	if (static_cast<idx_t>(expected_size) > buffer.GetCapacity()) {
		//! Resize the buffer in advance, to prevent any need for resizing below
		buffer.Resize(NextPowerOfTwo(expected_size));
		avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
	}

	//! Flush the contents to the buffer, if it fails, resize the buffer and try again
	int ret;
	while ((ret = avro_file_writer_flush(global_state.file_writer)) == ENOSPC) {
		auto current_capacity = buffer.GetCapacity();
		buffer.Resize(NextPowerOfTwo(current_capacity * 2));
		avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
	}
	if (ret) {
		throw InvalidInputException(avro_strerror());
	}

	auto written_bytes = avro_writer_tell(global_state.writer);
	global_state.WriteData(buffer.GetData(), written_bytes);
	avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());

	//! Track rows written for RETURN_STATS `count`. Avro COPY is single-threaded
	//! (REGULAR_COPY_TO_FILE), but guard with the same lock for consistency.
	{
		lock_guard<mutex> flock(global_state.lock);
		global_state.row_count += count;
	}
}

static void WriteAvroCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                             LocalFunctionData &lstate) {
	return;
}

static void WriteAvroFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = gstate.Cast<WriteAvroGlobalState>();
	//! WriteAvroSink has already flushed every block to the file handle, so there is no
	//! more data to write here. We only need to close the handle, which is required for
	//! stores that commit on Close() (e.g. Azure DFS / OneLake, where Write() Appends
	//! staged bytes that only become visible after Flush(Close=true)). Local FS, S3, and
	//! Azure Blob are unaffected because their writes commit incrementally.
	if (global_state.handle) {
		global_state.handle->Close();
	}

	//! Populate RETURN_STATS now that the file is fully written. bytes_written is the exact,
	//! incrementally-tracked file size (no HEAD probe); row_count is the number of rows sunk.
	//! The other stat columns (footer_size_bytes, column_statistics) are not applicable to the
	//! Avro object container and are left at their defaults.
	if (global_state.written_stats) {
		global_state.written_stats->file_size_bytes = global_state.BytesWritten();
		global_state.written_stats->row_count = global_state.row_count;
	}
}

CopyFunctionExecutionMode WriteAvroExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	//! For now we only support single-threaded writes to Avro
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

static void WriteAvroGetWrittenStatistics(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                          CopyFunctionFileStatistics &statistics) {
	//! DuckDB calls this once, right after copy_to_initialize_global and BEFORE any rows are
	//! written, so the file only contains its header at this point. Store the destination and
	//! fill the real values in copy_to_finalize (after the file is fully written and closed),
	//! mirroring the Parquet writer. Reporting BytesWritten() here would yield only the header
	//! size. No file-size probe (HEAD) is needed: bytes_written is tracked incrementally.
	auto &global_state = gstate.Cast<WriteAvroGlobalState>();
	global_state.written_stats = &statistics;
}

CopyFunction AvroCopyFunction::Create() {
	CopyFunction function("avro");
	function.extension = "avro";

	function.copy_to_bind = WriteAvroBind;
	function.copy_to_initialize_local = WriteAvroInitializeLocal;
	function.copy_to_initialize_global = WriteAvroInitializeGlobal;
	function.copy_to_sink = WriteAvroSink;
	function.copy_to_combine = WriteAvroCombine;
	function.copy_to_finalize = WriteAvroFinalize;
	function.copy_to_get_written_statistics = WriteAvroGetWrittenStatistics;
	function.execution_mode = WriteAvroExecutionMode;
	return function;
}

} // namespace duckdb
