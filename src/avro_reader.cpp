#include "avro_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

static AvroType TransformSchema(avro_schema_t &avro_schema, unordered_set<string> parent_schema_names) {
	switch (avro_typeof(avro_schema)) {
	case AVRO_NULL:
		return AvroType(AVRO_NULL, LogicalType::SQLNULL);
	case AVRO_BOOLEAN:
		return AvroType(AVRO_BOOLEAN, LogicalType::BOOLEAN);
	case AVRO_INT32:
		return AvroType(AVRO_INT32, LogicalType::INTEGER);
	case AVRO_INT64:
		return AvroType(AVRO_INT64, LogicalType::BIGINT);
	case AVRO_FLOAT:
		return AvroType(AVRO_FLOAT, LogicalType::FLOAT);
	case AVRO_DOUBLE:
		return AvroType(AVRO_DOUBLE, LogicalType::DOUBLE);
	case AVRO_BYTES:
		return AvroType(AVRO_BYTES, LogicalType::BLOB);
	case AVRO_STRING:
		return AvroType(AVRO_STRING, LogicalType::VARCHAR);
	case AVRO_UNION: {
		auto num_children = avro_schema_union_size(avro_schema);
		child_list_t<AvroType> union_children;
		idx_t non_null_child_idx = 0;
		unordered_map<idx_t, optional_idx> union_child_map;
		for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
			auto child_schema = avro_schema_union_branch(avro_schema, child_idx);
			auto child_type = TransformSchema(child_schema, parent_schema_names);
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
			auto child_type = TransformSchema(child_schema, parent_schema_names);
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
		return AvroType(AVRO_FIXED, LogicalType::BLOB);
	}
	case AVRO_ARRAY: {
		auto child_schema = avro_schema_array_items(avro_schema);
		auto element_id = avro_schema_array_element_id(avro_schema);
		auto child_type = TransformSchema(child_schema, parent_schema_names);
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
		auto value_type = TransformSchema(child_schema, parent_schema_names);
		value_type.field_id = value_id;

		child_list_t<AvroType> map_children;
		map_children.push_back(std::pair<std::string, AvroType>("key_entry", std::move(key_type)));
		map_children.push_back(std::pair<std::string, AvroType>("value_entry", std::move(value_type)));
		return AvroType(AVRO_MAP, LogicalTypeId::MAP, std::move(map_children));
	}
	case AVRO_LINK: {
		auto target = avro_schema_link_target(avro_schema);
		return TransformSchema(target, parent_schema_names);
	}
	default:
		throw NotImplementedException("Unknown Avro Type %s", avro_schema_type_name(avro_schema));
	}
}

AvroReader::AvroReader(ClientContext &context, OpenFileInfo file) : BaseFileReader(file) {
	auto caching_file_system = CachingFileSystem::Get(context);
	auto caching_file_handle = caching_file_system.OpenFile(this->file, FileOpenFlags::FILE_FLAGS_READ);

	Allocator &allocator = Allocator::Get(context);

	constexpr idx_t INITIAL_BUFFER_SIZE = 32 * 1024; // 32KB initial buffer
	file_buffer = make_uniq<AvroInMemoryBuffer>(allocator, INITIAL_BUFFER_SIZE);
	file_size = 0;

	// Read until EOF using CachingFileSystem (avoids HEAD request to get size, enables caching)
	while (true) {
		if (file_size >= file_buffer->GetCapacity()) {
			file_buffer->ResizeAndCopy(NextPowerOfTwo(file_buffer->GetCapacity() * 2));
		}

		data_ptr_t chunk_data = nullptr;
		idx_t chunk_size = file_buffer->GetCapacity() - file_size;
		auto chunk_handle = caching_file_handle->Read(chunk_data, chunk_size);

		if (chunk_size == 0) {
			break; // EOF
		}

		memcpy(file_buffer->GetData() + file_size, chunk_data, chunk_size);
		file_size += chunk_size;
	}

	if (file_size == 0) {
		throw InvalidInputException("Empty Avro file: %s", this->file.path);
	}

	auto avro_reader = avro_reader_memory(const_char_ptr_cast(file_buffer->GetData()), file_size);

	if (avro_reader_reader(avro_reader, &reader)) {
		throw InvalidInputException(avro_strerror());
	}

	auto avro_schema = avro_file_reader_get_writer_schema(reader);
	auto schema_name = avro_schema_name(avro_schema);
	string root_name = schema_name ? schema_name : "avro_schema";

	avro_type = TransformSchema(avro_schema, {});
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
	case LogicalTypeId::INTEGER: {
		if (avro_value_get_int(avro_val, &FlatVector::GetData<int32_t>(target)[out_idx])) {
			throw InvalidInputException(avro_strerror());
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		if (avro_value_get_long(avro_val, &FlatVector::GetData<int64_t>(target)[out_idx])) {
			throw InvalidInputException(avro_strerror());
		}
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
