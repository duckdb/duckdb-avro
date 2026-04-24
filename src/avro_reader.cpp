#include "avro_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/operator/multiply.hpp"

namespace duckdb {

static LogicalType AvroLogicalTypeToLogicalType(avro_schema_t &avro_schema, bool convert_millis_to_micro) {
	auto logical_type_raw = avro_schema_logical_type(avro_schema);
	if (!logical_type_raw) {
		return LogicalType::INVALID;
	}
	// any nested types are handled switch case in TransformSchema
	switch (avro_typeof(avro_schema)) {
	case AVRO_ARRAY:
	case AVRO_ENUM:
	case AVRO_MAP:
	case AVRO_RECORD:
		return LogicalType::INVALID;
	default:
		break;
	}
	string logical_type = logical_type_raw;
	if (logical_type == "date") {
		return LogicalType::DATE;
	}
	if (logical_type == "decimal") {
		auto scale = avro_schema_scale(avro_schema);
		auto precision = avro_schema_precision(avro_schema);
		return LogicalType::DECIMAL(precision, scale);
	}
	if (logical_type == "time-micros") {
		return LogicalType::TIME;
	}
	if (logical_type == "timestamp-micros") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		// -1 doesn't exist
		if (adjust_to_utc > 0) {
			return LogicalType::TIMESTAMP_TZ;
		}
		return LogicalType::TIMESTAMP;
	}
	if (logical_type == "timestamp-nanos") {
		auto adjust_to_utc = avro_schema_adjust_to_utc(avro_schema);
		if (adjust_to_utc > 0) {
			throw NotImplementedException("Avro timestamp-nanos with adjust_to_utc not supported");
		}
		return LogicalType::TIMESTAMP_NS;
	}
	if (logical_type == "uuid") {
		auto size = avro_schema_fixed_size(avro_schema);
		if (size != 16) {
			throw InvalidConfigurationException("logical type is uuid, but size != 16");
		}
		return LogicalType::UUID;
	}
	if (logical_type == "time-millis") {
		if (!convert_millis_to_micro) {
			throw NotImplementedException("Avro logical type time-millis not supported. Use "
			                              "convert_millis_to_micro=true to automatically convert to microseconds");
		}
		return LogicalType::TIME;
	}
	if (logical_type == "timestamp-millis") {
		if (!convert_millis_to_micro) {
			throw NotImplementedException("Avro logical type timestamp-millis not supported. Use "
			                              "convert_millis_to_micro=true to automatically convert to microseconds");
		}
		return LogicalType::TIMESTAMP;
	}
	if (logical_type == "local-timestamp-millis") {
		if (!convert_millis_to_micro) {
			throw NotImplementedException("Avro logical type local-timestamp-millis not supported. Use "
			                              "convert_millis_to_micro=true to automatically convert to microseconds");
		}
		return LogicalType::TIMESTAMP;
	}
	throw NotImplementedException("Unknown Avro logical type %s", logical_type);
}

static AvroType TransformSchema(avro_schema_t &avro_schema, unordered_set<string> parent_schema_names,
                                bool convert_millis_to_micro) {
	auto duckdb_logical_type = AvroLogicalTypeToLogicalType(avro_schema, convert_millis_to_micro);
	bool has_logical_type = duckdb_logical_type != LogicalType::INVALID;

	auto raw_lt = avro_schema_logical_type(avro_schema);
	bool is_millis = raw_lt && (string(raw_lt) == "timestamp-millis" || string(raw_lt) == "time-millis" ||
	                            string(raw_lt) == "local-timestamp-millis");

	switch (avro_typeof(avro_schema)) {
	case AVRO_NULL:
		return AvroType(AVRO_NULL, LogicalType::SQLNULL);
	case AVRO_BOOLEAN:
		return AvroType(AVRO_BOOLEAN, LogicalType::BOOLEAN);
	case AVRO_INT32: {
		AvroType result(AVRO_INT32, has_logical_type ? duckdb_logical_type : LogicalType::INTEGER);
		result.is_timestamp_millis = is_millis;
		return result;
	}
	case AVRO_INT64: {
		AvroType result(AVRO_INT64, has_logical_type ? duckdb_logical_type : LogicalType::BIGINT);
		result.is_timestamp_millis = is_millis;
		return result;
	}
	case AVRO_FLOAT:
		return AvroType(AVRO_FLOAT, has_logical_type ? duckdb_logical_type : LogicalType::FLOAT);
	case AVRO_DOUBLE:
		return AvroType(AVRO_DOUBLE, has_logical_type ? duckdb_logical_type : LogicalType::DOUBLE);
	case AVRO_BYTES:
		return AvroType(AVRO_BYTES, has_logical_type ? duckdb_logical_type : LogicalType::BLOB);
	case AVRO_STRING:
		return AvroType(AVRO_STRING, has_logical_type ? duckdb_logical_type : LogicalType::VARCHAR);
	case AVRO_UNION: {
		auto num_children = avro_schema_union_size(avro_schema);
		child_list_t<AvroType> union_children;
		idx_t non_null_child_idx = 0;
		unordered_map<idx_t, optional_idx> union_child_map;
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_union_branch(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names, convert_millis_to_micro);
			union_children.push_back(
			    std::pair<std::string, AvroType>(StringUtil::Format("u%llu", child_idx), std::move(child_type)));
			if (child_type.duckdb_type.id() != LogicalTypeId::SQLNULL) {
				union_child_map[child_idx] = non_null_child_idx++;
			}
		}
		return AvroType(AVRO_UNION, LogicalTypeId::UNION, std::move(union_children), union_child_map);
	}
	case AVRO_RECORD: {
		auto schema_name = string(avro_schema_name(avro_schema));
		if (parent_schema_names.find(schema_name) != parent_schema_names.end()) {
			throw InvalidInputException("Recursive Avro types not supported: %s", schema_name);
		}
		parent_schema_names.insert(schema_name);

		auto num_children = avro_schema_record_size(avro_schema);
		if (num_children == 0) {
			// this we just ignore but we need a marker so we don't get our offsets
			// wrong
			return AvroType(AVRO_RECORD, LogicalTypeId::SQLNULL);
		}
		child_list_t<AvroType> struct_children;
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_record_field_get_by_index(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names, convert_millis_to_micro);
			child_type.field_id = avro_schema_record_field_id(avro_schema, child_idx);
			auto child_name = avro_schema_record_field_name(avro_schema, child_idx);
			if (!child_name || strlen(child_name) == 0) {
				throw InvalidInputException("Empty avro field name");
			}

			struct_children.push_back(std::pair<std::string, AvroType>(child_name, std::move(child_type)));
		}

		return AvroType(AVRO_RECORD, LogicalTypeId::STRUCT, std::move(struct_children));
	}
	case AVRO_ENUM: {
		auto size = avro_schema_enum_number_of_symbols(avro_schema);
		Vector levels(LogicalType::VARCHAR, size);
		auto levels_data = FlatVector::GetData<string_t>(levels);
		for (idx_t enum_idx = 0; enum_idx < static_cast<idx_t>(size); enum_idx++) {
			levels_data[enum_idx] = StringVector::AddString(levels, avro_schema_enum_get(avro_schema, enum_idx));
		}
		levels.Verify(size);
		return AvroType(AVRO_ENUM, LogicalType::ENUM(levels, size));
	}
	case AVRO_FIXED: {
		return AvroType(AVRO_FIXED, has_logical_type ? duckdb_logical_type : LogicalType::BLOB);
	}
	case AVRO_ARRAY: {
		auto child_schema = avro_schema_array_items(avro_schema);
		auto element_id = avro_schema_array_element_id(avro_schema);
		auto child_type = TransformSchema(child_schema, parent_schema_names, convert_millis_to_micro);
		child_type.field_id = element_id;
		child_list_t<AvroType> list_children;
		list_children.push_back(std::pair<std::string, AvroType>("list_entry", std::move(child_type)));
		bool is_map = avro_schema_array_is_map(avro_schema);
		if (is_map) {
			return AvroType(AVRO_ARRAY, LogicalTypeId::MAP, std::move(list_children));
		}
		return AvroType(AVRO_ARRAY, LogicalTypeId::LIST, std::move(list_children));
	}
	case AVRO_MAP: {
		auto child_schema = avro_schema_map_values(avro_schema);
		auto key_id = avro_schema_map_key_id(avro_schema);
		auto value_id = avro_schema_map_value_id(avro_schema);

		AvroType key_type(AVRO_STRING, LogicalTypeId::VARCHAR);
		key_type.field_id = key_id;
		auto value_type = TransformSchema(child_schema, parent_schema_names, convert_millis_to_micro);
		value_type.field_id = value_id;

		child_list_t<AvroType> map_children;
		map_children.push_back(std::pair<std::string, AvroType>("key_entry", std::move(key_type)));
		map_children.push_back(std::pair<std::string, AvroType>("value_entry", std::move(value_type)));
		return AvroType(AVRO_MAP, LogicalTypeId::MAP, std::move(map_children));
	}
	case AVRO_LINK: {
		auto target = avro_schema_link_target(avro_schema);
		return TransformSchema(target, parent_schema_names, convert_millis_to_micro);
	}
	default:
		throw NotImplementedException("Unknown Avro Type %s", avro_schema_type_name(avro_schema));
	}
}

AvroReader::AvroReader(ClientContext &context, OpenFileInfo file, const AvroFileReaderOptions &options)
    : BaseFileReader(file) {
	auto caching_file_system = CachingFileSystem::Get(context);

	auto caching_file_handle = caching_file_system.OpenFile(this->file, FileOpenFlags::FILE_FLAGS_READ);
	auto total_size = caching_file_handle->GetFileSize();
	data_ptr_t data = nullptr;

	buf_handle = caching_file_handle->Read(data, total_size);
	auto buffer_data = buf_handle.Ptr();

	D_ASSERT(buf_handle.IsValid());
	D_ASSERT(buffer_data == data);
	auto avro_reader = avro_reader_memory(const_char_ptr_cast(buffer_data), total_size);

	if (avro_reader_reader(avro_reader, &reader)) {
		throw InvalidInputException(avro_strerror());
	}

	auto avro_schema = avro_file_reader_get_writer_schema(reader);
	auto schema_name = avro_schema_name(avro_schema);
	string root_name = schema_name ? schema_name : "avro_schema";

	avro_type = TransformSchema(avro_schema, {}, options.convert_millis_to_micro);
	auto root = AvroType::TransformAvroType(root_name, avro_type);
	duckdb_type = root.type;
	read_vec = make_uniq<Vector>(duckdb_type);

	auto interface = avro_generic_class_from_schema(avro_schema);
	avro_generic_value_new(interface, &value);
	avro_value_iface_decref(interface);

	// special handling for root structs, we pull up the entries
	if (duckdb_type.id() == LogicalTypeId::STRUCT) {
		columns = std::move(root.children);
	} else {
		columns.clear();
		columns.push_back(std::move(root));
	}
	avro_schema_decref(avro_schema);
}

static void TransformValue(avro_value *avro_val, const AvroType &avro_type, Vector &target, idx_t out_idx) {

	switch (avro_type.duckdb_type.id()) {
	case LogicalTypeId::SQLNULL: {
		FlatVector::SetNull(target, out_idx, true);
		break;
	}
	case LogicalTypeId::BOOLEAN: {
		int bool_val;
		if (avro_value_get_boolean(avro_val, &bool_val)) {
			throw InvalidInputException(avro_strerror());
		}
		FlatVector::GetData<uint8_t>(target)[out_idx] = bool_val != 0;
		break;
	}
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER: {
		if (avro_value_get_int(avro_val, &FlatVector::GetData<int32_t>(target)[out_idx])) {
			throw InvalidInputException(avro_strerror());
		}
		break;
	}
	case LogicalTypeId::TIME: {
		int64_t result;
		if (avro_type.is_timestamp_millis) {
			// time-millis: stored as int32 (ms since midnight), scale to µs
			int32_t raw_val;
			if (avro_value_get_int(avro_val, &raw_val)) {
				throw InvalidInputException(avro_strerror());
			}
			// no thread of overflow since raw value is int32_t
			result = static_cast<int64_t>(raw_val) * Interval::MICROS_PER_MSEC;
		} else {
			if (avro_value_get_long(avro_val, &result)) {
				throw InvalidInputException(avro_strerror());
			}
		}
		FlatVector::GetData<int64_t>(target)[out_idx] = result;
		break;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::BIGINT: {
		int64_t raw_val;
		if (avro_value_get_long(avro_val, &raw_val)) {
			throw InvalidInputException(avro_strerror());
		}
		FlatVector::GetData<int64_t>(target)[out_idx] =
		    avro_type.is_timestamp_millis ? MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(
		                                        raw_val, Interval::MICROS_PER_MSEC)
		                                  : raw_val;
		break;
	}
	case LogicalTypeId::FLOAT: {
		if (avro_value_get_float(avro_val, &FlatVector::GetData<float>(target)[out_idx])) {
			throw InvalidInputException(avro_strerror());
		}
		break;
	}
	case LogicalTypeId::DOUBLE: {
		if (avro_value_get_double(avro_val, &FlatVector::GetData<double>(target)[out_idx])) {
			throw InvalidInputException(avro_strerror());
		}
		break;
	}
	case LogicalTypeId::UUID: {
		size_t fixed_size = 16;
		const void *fixed_data;
		if (avro_value_get_fixed(avro_val, &fixed_data, &fixed_size)) {
			throw InvalidInputException(avro_strerror());
		}
		FlatVector::GetData<hugeint_t>(target)[out_idx] = BaseUUID::FromBlob(const_data_ptr_cast(fixed_data));
		break;
	}
	case LogicalTypeId::DECIMAL: {
		// decimals should never be more than 16
		const uint8_t bytes_data[16] = {};
		const void *ptr = bytes_data;
		size_t bytes_size;
		avro_wrapped_buffer bytes_buf = AVRO_WRAPPED_BUFFER_EMPTY;

		if (avro_type.avro_type == AVRO_BYTES) {
			if (avro_value_grab_bytes(avro_val, &bytes_buf)) {
				throw InvalidInputException(avro_strerror());
			}
			ptr = bytes_buf.buf;
			bytes_size = bytes_buf.size;
		} else { // AVRO_FIXED
			if (avro_value_get_fixed(avro_val, &ptr, &bytes_size)) {
				throw InvalidInputException(avro_strerror());
			}
		}

		auto raw = const_data_ptr_cast(ptr);
		// Sign bit is in the MSB of the first byte
		bool negative = bytes_size > 0 && (raw[0] & 0x80);

		switch (avro_type.duckdb_type.InternalType()) {
		case PhysicalType::INT16: {
			uint16_t result = negative ? 0xFFFF : 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				result = (result << 8) | raw[i];
			}
			FlatVector::GetData<int16_t>(target)[out_idx] = (int16_t)result;
			break;
		}
		case PhysicalType::INT32: {
			uint32_t result = negative ? 0xFFFFFFFF : 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				result = (result << 8) | raw[i];
			}
			FlatVector::GetData<int32_t>(target)[out_idx] = (int32_t)result;
			break;
		}
		case PhysicalType::INT64: {
			uint64_t result = negative ? ~0ULL : 0;
			for (idx_t i = 0; i < bytes_size; i++) {
				result = (result << 8) | raw[i];
			}
			FlatVector::GetData<int64_t>(target)[out_idx] = (int64_t)result;
			break;
		}
		case PhysicalType::INT128: {
			int64_t upper_val = 0;
			uint64_t lower_val = 0;

			// Calculate how many bytes go into upper and lower parts
			idx_t upper_bytes;
			if (bytes_size == 8) {
				int64_t lower_val_signed = 0;
				for (idx_t i = 0; i < bytes_size; i++) {
					int64_t raw_byte = raw[i];
					lower_val_signed |= (raw_byte << ((7 - i) * 8));
				}
				upper_bytes = 0;
				auto ret = hugeint_t(lower_val_signed);
				FlatVector::GetData<hugeint_t>(target)[out_idx] = ret;
				break;
			} else {
				upper_bytes = (bytes_size <= sizeof(uint64_t)) ? bytes_size : (bytes_size - sizeof(uint64_t));
			}

			// Read upper part (big-endian)
			// TODO: upper part might be sign extended bti.
			for (idx_t i = 0; i < upper_bytes; i++) {
				upper_val = (upper_val << 8) | raw[i];
			}

			// Handle sign extension for negative numbers
			if (bytes_size > 0 && (raw[0] & 0x80)) {
				// Fill remaining bytes with 1s for negative numbers
				if (upper_bytes < sizeof(int64_t)) {
					// Create a mask with 1s in the upper bits that need to be filled
					int64_t mask = ((int64_t)1 << ((sizeof(int64_t) - upper_bytes) * 8)) - 1;
					mask = mask << (upper_bytes * 8);
					upper_val |= mask;
				}
			}

			// Read lower part if there are remaining bytes
			if (bytes_size > sizeof(int64_t)) {
				for (idx_t i = upper_bytes; i < bytes_size; i++) {
					lower_val = (lower_val << 8) | raw[i];
				}
			}

			auto ret = hugeint_t(upper_val, lower_val);
			FlatVector::GetData<hugeint_t>(target)[out_idx] = ret;
			break;
		}
		default:
			throw NotImplementedException("Unsupported decimal physical type");
		}

		if (avro_type.avro_type == AVRO_BYTES) {
			bytes_buf.free(&bytes_buf);
		}
		break;
	}
	case LogicalTypeId::BLOB:
		switch (avro_type.avro_type) {
		case AVRO_FIXED: {
			size_t fixed_size;
			const void *fixed_data;
			if (avro_value_get_fixed(avro_val, &fixed_data, &fixed_size)) {
				throw InvalidInputException(avro_strerror());
			}
			FlatVector::GetData<string_t>(target)[out_idx] =
			    StringVector::AddStringOrBlob(target, const_char_ptr_cast(fixed_data), fixed_size);
			break;
		}
		case AVRO_BYTES: {
			avro_wrapped_buffer blob_buf = AVRO_WRAPPED_BUFFER_EMPTY;
			if (avro_value_grab_bytes(avro_val, &blob_buf)) {
				throw InvalidInputException(avro_strerror());
			}
			FlatVector::GetData<string_t>(target)[out_idx] =
			    StringVector::AddStringOrBlob(target, const_char_ptr_cast(blob_buf.buf), blob_buf.size);
			blob_buf.free(&blob_buf);
			break;
		}
		default:
			throw NotImplementedException("Unknown Avro blob type %s");
		}
		break;

	case LogicalTypeId::VARCHAR: {
		avro_wrapped_buffer str_buf = AVRO_WRAPPED_BUFFER_EMPTY;
		if (avro_value_grab_string(avro_val, &str_buf)) {
			throw InvalidInputException(avro_strerror());
		}
		// avro strings are null-terminated
		D_ASSERT(const_char_ptr_cast(str_buf.buf)[str_buf.size - 1] == '\0');
		if (Utf8Proc::Analyze(const_char_ptr_cast(str_buf.buf), str_buf.size - 1) == UnicodeType::INVALID) {
			throw InvalidInputException("Avro file contains invalid unicode string");
		}
		FlatVector::GetData<string_t>(target)[out_idx] =
		    StringVector::AddString(target, const_char_ptr_cast(str_buf.buf), str_buf.size - 1);
		str_buf.free(&str_buf);
		break;
	}
	case LogicalTypeId::STRUCT: {
		size_t child_count;
		if (avro_value_get_size(avro_val, &child_count)) {
			throw InvalidInputException(avro_strerror());
		}
		D_ASSERT(child_count == StructType::GetChildCount(target.GetType()));
		D_ASSERT(child_count == avro_type.children.size());

		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			avro_value child_value;
			if (avro_value_get_by_index(avro_val, child_idx, &child_value, nullptr)) {
				throw InvalidInputException(avro_strerror());
			}
			TransformValue(&child_value, avro_type.children[child_idx].second,
			               *StructVector::GetEntries(target)[child_idx], out_idx);
		}
		break;
	}

	case LogicalTypeId::UNION: {
		int discriminant = 0;
		avro_value union_value;
		if (avro_value_get_discriminant(avro_val, &discriminant) ||
		    avro_value_get_current_branch(avro_val, &union_value)) {
			throw InvalidInputException(avro_strerror());
		}
		if (static_cast<size_t>(discriminant) >= avro_type.children.size()) {
			throw InvalidInputException("Invalid union tag");
		}

		if (avro_type.children[discriminant].second.duckdb_type == LogicalTypeId::SQLNULL) {
			FlatVector::SetNull(target, out_idx, true);
			break;
		}

		if (target.GetType().id() == LogicalTypeId::UNION) {
			auto duckdb_child_index = avro_type.union_child_map.at(discriminant).GetIndex();
			auto &tags = UnionVector::GetTags(target);
			FlatVector::GetData<union_tag_t>(tags)[out_idx] = duckdb_child_index;
			auto &union_vector = UnionVector::GetMember(target, duckdb_child_index);

			// orrrrrrrrrrrrr
			for (idx_t child_idx = 1; child_idx < StructVector::GetEntries(target).size(); child_idx++) {
				if (child_idx != duckdb_child_index + 1) { // duckdb child index is bigger because of the tag
					FlatVector::SetNull(*StructVector::GetEntries(target)[child_idx], out_idx, true);
				}
			}

			TransformValue(&union_value, avro_type.children[discriminant].second, union_vector, out_idx);
		} else { // directly recurse, we have dissolved the union
			TransformValue(&union_value, avro_type.children[discriminant].second, target, out_idx);
		}

		break;
	}
	case LogicalTypeId::ENUM: {
		auto enum_type = EnumType::GetPhysicalType(target.GetType());
		int enum_val;

		if (avro_value_get_enum(avro_val, &enum_val)) {
			throw InvalidInputException(avro_strerror());
		}
		if (enum_val < 0 || static_cast<idx_t>(enum_val) >= EnumType::GetSize(target.GetType())) {
			throw InvalidInputException("Enum value out of range");
		}

		switch (enum_type) {
		case PhysicalType::UINT8:
			FlatVector::GetData<uint8_t>(target)[out_idx] = enum_val;
			break;
		case PhysicalType::UINT16:
			FlatVector::GetData<uint16_t>(target)[out_idx] = enum_val;
			break;
		case PhysicalType::UINT32:
			FlatVector::GetData<uint32_t>(target)[out_idx] = enum_val;
			break;
		default:
			throw InternalException("Unsupported Enum Internal Type");
		}
		break;
	}

	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		size_t list_len;
		if (avro_value_get_size(avro_val, &list_len)) {
			throw InvalidInputException(avro_strerror());
		}

		auto child_offset = ListVector::GetListSize(target);
		ListVector::Reserve(target, child_offset + list_len);

		if (avro_type.avro_type == AVRO_ARRAY) {
			auto &child_vector = ListVector::GetEntry(target);

			for (idx_t child_idx = 0; child_idx < list_len; child_idx++) {
				avro_value_t child_value;
				if (avro_value_get_by_index(avro_val, child_idx, &child_value, nullptr)) {
					throw InvalidInputException(avro_strerror());
				}
				TransformValue(&child_value, avro_type.children[0].second, child_vector, child_offset + child_idx);
			}
		} else {
			auto &key_vector = MapVector::GetKeys(target);
			auto &value_vector = MapVector::GetValues(target);

			auto &key_type = avro_type.children[0].second;
			//! Unused, always VARCHAR
			(void)key_type;
			auto &value_type = avro_type.children[1].second;
			D_ASSERT(key_vector.GetType().id() == LogicalTypeId::VARCHAR);
			auto string_ptr = FlatVector::GetData<string_t>(key_vector);
			for (idx_t entry_idx = 0; entry_idx < list_len; entry_idx++) {
				avro_value child_value;
				const char *map_key;
				if (avro_value_get_by_index(avro_val, entry_idx, &child_value, &map_key)) {
					throw InvalidInputException(avro_strerror());
				}
				D_ASSERT(map_key);
				string_ptr[child_offset + entry_idx] = StringVector::AddString(key_vector, map_key);
				TransformValue(&child_value, value_type, value_vector, child_offset + entry_idx);
			}
		}
		auto list_vector_data = ListVector::GetData(target);
		list_vector_data[out_idx].length = list_len;
		list_vector_data[out_idx].offset = child_offset;
		ListVector::SetListSize(target, child_offset + list_len);
		break;
	}

	default:
		throw NotImplementedException(avro_type.duckdb_type.ToString());
	}
}

void AvroReader::Read(DataChunk &output) {
	idx_t out_idx = 0;

	while (avro_file_reader_read_value(reader, &value) == 0) {
		TransformValue(&value, avro_type, *read_vec, out_idx++);
		if (out_idx == STANDARD_VECTOR_SIZE) {
			break;
		}
	}
	// pull up root struct into output chunk
	if (duckdb_type.id() == LogicalTypeId::STRUCT) {
		for (idx_t col_idx = 0; col_idx < column_indexes.size(); col_idx++) {
			if (column_indexes[col_idx].GetPrimaryIndex() >= columns.size()) {
				continue; // to be filled in later
			}
			output.data[col_idx].Reference(
			    *StructVector::GetEntries(*read_vec)[column_indexes[col_idx].GetPrimaryIndex()]);
		}
	} else {
		output.data[column_indexes[0].GetPrimaryIndex()].Reference(*read_vec);
	}
	output.SetCardinality(out_idx);
}

} // namespace duckdb
