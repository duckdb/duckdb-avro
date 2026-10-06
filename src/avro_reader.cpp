#include "avro_reader.hpp"

#include <atomic>
#include <cstring>

namespace duckdb {

namespace avro {

using cxx::Vector;

namespace {

constexpr int64_t MICROS_PER_MSEC = 1000;

struct SchemaDeleter {
	void operator()(std::remove_pointer_t<avro_schema_t> *schema) const {
		avro_schema_decref(schema);
	}
};

int64_t MillisToMicros(int64_t millis) {
	if (millis > std::numeric_limits<int64_t>::max() / MICROS_PER_MSEC ||
	    millis < std::numeric_limits<int64_t>::min() / MICROS_PER_MSEC) {
		throw OutOfRangeError("Overflow in multiplication of INT64 (" + std::to_string(millis) + " * " +
		                      std::to_string(MICROS_PER_MSEC) + ")!");
	}
	return millis * MICROS_PER_MSEC;
}

std::string_view ValidateString(std::string_view str) {
	try {
		cxx::ValidateUTF8(str);
	} catch (const cxx::Exception &) {
		throw InvalidInputError("Avro file contains invalid unicode string");
	}
	return str;
}

//===--------------------------------------------------------------------===//
// Value Readers
//===--------------------------------------------------------------------===//
//! Reads Avro values into one vector of an output chunk
class ValueReader {
public:
	virtual ~ValueReader() = default;

public:
	//! Starts filling the vector of a new output chunk. The vector must outlive the chunk being filled
	void Reset(Vector &vector_p) {
		vector = &vector_p;
		ResetInternal();
	}
	//! Re-fetches the buffers of the vector, after it was resized
	virtual void Refresh() {
	}
	virtual void Read(avro_value_t *value, idx_t row) = 0;
	virtual void SetNull(idx_t row) {
		vector->SetNull(row);
	}
	//! Called once every row of the chunk has been read
	virtual void Finalize() {
	}

protected:
	virtual void ResetInternal() {
		Refresh();
	}

protected:
	Vector *vector = nullptr;
};

using reader_ptr_t = std::unique_ptr<ValueReader>;

class NullReader : public ValueReader {
public:
	void Read(avro_value_t *, idx_t row) override {
		SetNull(row);
	}
};

template <class T>
class PrimitiveReader : public ValueReader {
public:
	void Refresh() override {
		data = vector->GetDataMutable<T>();
		validity = vector->GetValidityMutable();
	}
	void SetNull(idx_t row) override {
		validity.SetInvalid(row);
	}

protected:
	T *data = nullptr;
	cxx::ValidityMask validity {nullptr};
};

class BooleanReader : public PrimitiveReader<bool> {
public:
	void Read(avro_value_t *value, idx_t row) override {
		int bool_val;
		if (avro_value_get_boolean(value, &bool_val)) {
			throw AvroError();
		}
		data[row] = bool_val != 0;
	}
};

class IntegerReader : public PrimitiveReader<int32_t> {
public:
	void Read(avro_value_t *value, idx_t row) override {
		if (avro_value_get_int(value, &data[row])) {
			throw AvroError();
		}
	}
};

class TimeReader : public PrimitiveReader<int64_t> {
public:
	explicit TimeReader(bool is_millis) : is_millis(is_millis) {
	}

	void Read(avro_value_t *value, idx_t row) override {
		if (is_millis) {
			// time-millis: stored as int32 (ms since midnight), scale to µs
			int32_t raw_val;
			if (avro_value_get_int(value, &raw_val)) {
				throw AvroError();
			}
			// no threat of overflow since raw value is int32_t
			data[row] = static_cast<int64_t>(raw_val) * MICROS_PER_MSEC;
			return;
		}
		if (avro_value_get_long(value, &data[row])) {
			throw AvroError();
		}
	}

private:
	bool is_millis;
};

class BigIntReader : public PrimitiveReader<int64_t> {
public:
	explicit BigIntReader(bool is_millis) : is_millis(is_millis) {
	}

	void Read(avro_value_t *value, idx_t row) override {
		int64_t raw_val;
		if (avro_value_get_long(value, &raw_val)) {
			throw AvroError();
		}
		data[row] = is_millis ? MillisToMicros(raw_val) : raw_val;
	}

private:
	bool is_millis;
};

class FloatReader : public PrimitiveReader<float> {
public:
	void Read(avro_value_t *value, idx_t row) override {
		if (avro_value_get_float(value, &data[row])) {
			throw AvroError();
		}
	}
};

class DoubleReader : public PrimitiveReader<double> {
public:
	void Read(avro_value_t *value, idx_t row) override {
		if (avro_value_get_double(value, &data[row])) {
			throw AvroError();
		}
	}
};

class UUIDReader : public PrimitiveReader<cxx::uuid_t> {
public:
	void Read(avro_value_t *value, idx_t row) override {
		size_t fixed_size = 16;
		const void *fixed_data;
		if (avro_value_get_fixed(value, &fixed_data, &fixed_size)) {
			throw AvroError();
		}
		cxx::uuid_t::Decoded bytes;
		memcpy(bytes.bytes, fixed_data, sizeof(bytes.bytes));
		data[row] = cxx::uuid_t::Encode(bytes);
	}
};

//! Reads a big-endian two's complement integer of up to 8 bytes
template <class UNSIGNED>
UNSIGNED ReadBigEndian(const uint8_t *raw, idx_t size) {
	bool negative = size > 0 && (raw[0] & 0x80);
	UNSIGNED result = negative ? static_cast<UNSIGNED>(~UNSIGNED(0)) : 0;
	for (idx_t i = 0; i < size; i++) {
		result = static_cast<UNSIGNED>((result << 8) | raw[i]);
	}
	return result;
}

template <class T>
T ReadDecimal(const uint8_t *raw, idx_t size) {
	if constexpr (std::is_same_v<T, cxx::int128_t>) {
		bool negative = size > 0 && (raw[0] & 0x80);
		uint64_t upper = negative ? ~0ULL : 0;
		uint64_t lower = negative ? ~0ULL : 0;
		for (idx_t i = 0; i < size; i++) {
			upper = (upper << 8) | (lower >> 56);
			lower = (lower << 8) | raw[i];
		}
		return cxx::int128_t {lower, static_cast<int64_t>(upper)};
	} else {
		return static_cast<T>(ReadBigEndian<std::make_unsigned_t<T>>(raw, size));
	}
}

template <class T>
class DecimalReader : public PrimitiveReader<T> {
public:
	explicit DecimalReader(avro_type_t avro_type) : avro_type(avro_type) {
	}

	void Read(avro_value_t *value, idx_t row) override {
		if (avro_type == AVRO_BYTES) {
			avro_wrapped_buffer bytes_buf = AVRO_WRAPPED_BUFFER_EMPTY;
			if (avro_value_grab_bytes(value, &bytes_buf)) {
				throw AvroError();
			}
			this->data[row] = ReadDecimal<T>(static_cast<const uint8_t *>(bytes_buf.buf), bytes_buf.size);
			bytes_buf.free(&bytes_buf);
			return;
		}
		const void *ptr;
		size_t bytes_size;
		if (avro_value_get_fixed(value, &ptr, &bytes_size)) {
			throw AvroError();
		}
		this->data[row] = ReadDecimal<T>(static_cast<const uint8_t *>(ptr), bytes_size);
	}

private:
	avro_type_t avro_type;
};

class BlobReader : public PrimitiveReader<cxx::blob_t> {
public:
	explicit BlobReader(avro_type_t avro_type) : avro_type(avro_type) {
	}

	void Refresh() override {
		PrimitiveReader::Refresh();
		heap.emplace(vector->GetHeap());
	}

	void Read(avro_value_t *value, idx_t row) override {
		if (avro_type == AVRO_FIXED) {
			size_t fixed_size;
			const void *fixed_data;
			if (avro_value_get_fixed(value, &fixed_data, &fixed_size)) {
				throw AvroError();
			}
			data[row] = heap->AddBlob(std::string_view(static_cast<const char *>(fixed_data), fixed_size));
			return;
		}
		avro_wrapped_buffer blob_buf = AVRO_WRAPPED_BUFFER_EMPTY;
		if (avro_value_grab_bytes(value, &blob_buf)) {
			throw AvroError();
		}
		data[row] = heap->AddBlob(std::string_view(static_cast<const char *>(blob_buf.buf), blob_buf.size));
		blob_buf.free(&blob_buf);
	}

private:
	avro_type_t avro_type;
	std::optional<cxx::Arena> heap;
};

class VarcharReader : public PrimitiveReader<cxx::varchar_t> {
public:
	void Refresh() override {
		PrimitiveReader::Refresh();
		heap.emplace(vector->GetHeap());
	}

	void Read(avro_value_t *value, idx_t row) override {
		avro_wrapped_buffer str_buf = AVRO_WRAPPED_BUFFER_EMPTY;
		if (avro_value_grab_string(value, &str_buf)) {
			throw AvroError();
		}
		// avro strings are null-terminated
		auto size = str_buf.size ? str_buf.size - 1 : 0;
		auto str = ValidateString(std::string_view(static_cast<const char *>(str_buf.buf), size));
		data[row] = heap->AddStringUnsafe(str);
		str_buf.free(&str_buf);
	}

private:
	std::optional<cxx::Arena> heap;
};

template <class T>
class EnumReader : public PrimitiveReader<T> {
public:
	explicit EnumReader(idx_t enum_size) : enum_size(enum_size) {
	}

	void Read(avro_value_t *value, idx_t row) override {
		int enum_val;
		if (avro_value_get_enum(value, &enum_val)) {
			throw AvroError();
		}
		if (enum_val < 0 || static_cast<idx_t>(enum_val) >= enum_size) {
			throw InvalidInputError("Enum value out of range");
		}
		this->data[row] = static_cast<T>(enum_val);
	}

private:
	idx_t enum_size;
};

class StructReader : public ValueReader {
public:
	explicit StructReader(std::vector<reader_ptr_t> children) : children(std::move(children)) {
	}

	void Refresh() override {
		for (auto &child : children) {
			child->Refresh();
		}
	}

	void Read(avro_value_t *value, idx_t row) override {
		size_t child_count;
		if (avro_value_get_size(value, &child_count)) {
			throw AvroError();
		}
		if (child_count != children.size()) {
			throw InvalidInputError("Avro record has an unexpected number of fields");
		}
		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			avro_value_t child_value;
			if (avro_value_get_by_index(value, child_idx, &child_value, nullptr)) {
				throw AvroError();
			}
			children[child_idx]->Read(&child_value, row);
		}
	}

	void Finalize() override {
		for (auto &child : children) {
			child->Finalize();
		}
	}

protected:
	void ResetInternal() override {
		child_vectors.clear();
		child_vectors.reserve(children.size());
		for (idx_t i = 0; i < children.size(); i++) {
			child_vectors.push_back(vector->GetChild(i));
		}
		for (idx_t i = 0; i < children.size(); i++) {
			children[i]->Reset(child_vectors[i]);
		}
	}

private:
	std::vector<reader_ptr_t> children;
	std::vector<Vector> child_vectors;
};

//! Reads an Avro union into the DuckDB union of its branches that are not NULL
class UnionReader : public ValueReader {
public:
	UnionReader(const AvroType &avro_type, std::vector<reader_ptr_t> members)
	    : avro_type(avro_type), members(std::move(members)) {
	}

	void Refresh() override {
		tags = tag_vector->GetDataMutable<uint8_t>();
		for (auto &member : members) {
			member->Refresh();
		}
	}

	void Read(avro_value_t *value, idx_t row) override {
		int discriminant = 0;
		avro_value_t union_value;
		if (avro_value_get_discriminant(value, &discriminant) ||
		    avro_value_get_current_branch(value, &union_value)) {
			throw AvroError();
		}
		if (discriminant < 0 || static_cast<idx_t>(discriminant) >= avro_type.children.size()) {
			throw InvalidInputError("Invalid union tag");
		}
		auto entry = avro_type.union_child_map.find(static_cast<idx_t>(discriminant));
		if (entry == avro_type.union_child_map.end()) {
			SetNull(row);
			return;
		}
		auto member_idx = entry->second;
		tags[row] = static_cast<uint8_t>(member_idx);
		for (idx_t i = 0; i < members.size(); i++) {
			if (i != member_idx) {
				members[i]->SetNull(row);
			}
		}
		members[member_idx]->Read(&union_value, row);
	}

	void Finalize() override {
		for (auto &member : members) {
			member->Finalize();
		}
	}

protected:
	void ResetInternal() override {
		tag_vector.emplace(vector->GetChild(0));
		member_vectors.clear();
		member_vectors.reserve(members.size());
		for (idx_t i = 0; i < members.size(); i++) {
			member_vectors.push_back(vector->GetChild(i + 1));
		}
		for (idx_t i = 0; i < members.size(); i++) {
			members[i]->Reset(member_vectors[i]);
		}
		tags = tag_vector->GetDataMutable<uint8_t>();
	}

private:
	const AvroType &avro_type;
	std::vector<reader_ptr_t> members;
	std::optional<Vector> tag_vector;
	std::vector<Vector> member_vectors;
	uint8_t *tags = nullptr;
};

//! Reads an Avro union of NULL and a single other type straight into the vector of that type
class CollapsedUnionReader : public ValueReader {
public:
	CollapsedUnionReader(const AvroType &avro_type, reader_ptr_t child, idx_t child_branch)
	    : avro_type(avro_type), child(std::move(child)), child_branch(child_branch) {
	}

	void Refresh() override {
		child->Refresh();
	}

	void Read(avro_value_t *value, idx_t row) override {
		int discriminant = 0;
		avro_value_t union_value;
		if (avro_value_get_discriminant(value, &discriminant) ||
		    avro_value_get_current_branch(value, &union_value)) {
			throw AvroError();
		}
		if (discriminant < 0 || static_cast<idx_t>(discriminant) >= avro_type.children.size()) {
			throw InvalidInputError("Invalid union tag");
		}
		if (static_cast<idx_t>(discriminant) != child_branch) {
			child->SetNull(row);
			return;
		}
		child->Read(&union_value, row);
	}

	void SetNull(idx_t row) override {
		child->SetNull(row);
	}

	void Finalize() override {
		child->Finalize();
	}

protected:
	void ResetInternal() override {
		child->Reset(*vector);
	}

private:
	const AvroType &avro_type;
	reader_ptr_t child;
	idx_t child_branch;
};

//! The child vector of a LIST or MAP, grown as elements are appended to it
class ListChild {
public:
	void Reset(Vector child_vector) {
		vector.emplace(std::move(child_vector));
		count = 0;
		capacity = 0;
	}
	//! Makes room for "additional" more elements - returns true if the vector was resized
	bool Reserve(idx_t additional) {
		auto required = count + additional;
		if (required <= capacity) {
			return false;
		}
		capacity = std::max<idx_t>(required, std::max<idx_t>(capacity * 2, MINIMUM_CAPACITY));
		vector->SetSize(capacity);
		return true;
	}
	void Finalize() {
		vector->SetSize(count);
	}

public:
	static constexpr idx_t MINIMUM_CAPACITY = 64;

	std::optional<Vector> vector;
	idx_t count = 0;
	idx_t capacity = 0;
};

class ListReader : public ValueReader {
public:
	explicit ListReader(reader_ptr_t child) : child(std::move(child)) {
	}

	void Refresh() override {
		entries = vector->GetDataMutable<cxx::list_entry_t>();
	}

	void Read(avro_value_t *value, idx_t row) override {
		size_t list_len;
		if (avro_value_get_size(value, &list_len)) {
			throw AvroError();
		}
		if (elements.Reserve(list_len)) {
			child->Refresh();
		}
		auto offset = elements.count;
		for (idx_t child_idx = 0; child_idx < list_len; child_idx++) {
			avro_value_t child_value;
			if (avro_value_get_by_index(value, child_idx, &child_value, nullptr)) {
				throw AvroError();
			}
			child->Read(&child_value, offset + child_idx);
		}
		entries[row] = cxx::list_entry_t {offset, list_len};
		elements.count += list_len;
	}

	void Finalize() override {
		elements.Finalize();
		child->Finalize();
	}

protected:
	void ResetInternal() override {
		Refresh();
		elements.Reset(vector->GetChild(0));
		child->Reset(*elements.vector);
	}

private:
	reader_ptr_t child;
	ListChild elements;
	cxx::list_entry_t *entries = nullptr;
};

//! Reads an Avro map, whose keys are always strings
class MapReader : public ValueReader {
public:
	explicit MapReader(reader_ptr_t value_reader) : value_reader(std::move(value_reader)) {
	}

	void Refresh() override {
		map_entries = vector->GetDataMutable<cxx::list_entry_t>();
	}

	void Read(avro_value_t *value, idx_t row) override {
		size_t map_len;
		if (avro_value_get_size(value, &map_len)) {
			throw AvroError();
		}
		if (entries.Reserve(map_len)) {
			RefreshEntries();
		}
		auto offset = entries.count;
		for (idx_t entry_idx = 0; entry_idx < map_len; entry_idx++) {
			avro_value_t child_value;
			const char *map_key;
			if (avro_value_get_by_index(value, entry_idx, &child_value, &map_key)) {
				throw AvroError();
			}
			key_data[offset + entry_idx] = key_heap->AddStringUnsafe(ValidateString(map_key));
			value_reader->Read(&child_value, offset + entry_idx);
		}
		map_entries[row] = cxx::list_entry_t {offset, map_len};
		entries.count += map_len;
	}

	void Finalize() override {
		entries.Finalize();
		value_reader->Finalize();
	}

protected:
	void ResetInternal() override {
		Refresh();
		entries.Reset(vector->GetChild(0));
		keys.emplace(entries.vector->GetChild(0));
		values.emplace(entries.vector->GetChild(1));
		value_reader->Reset(*values);
		RefreshKeys();
	}

private:
	void RefreshKeys() {
		key_data = keys->GetDataMutable<cxx::varchar_t>();
		key_heap.emplace(keys->GetHeap());
	}
	void RefreshEntries() {
		RefreshKeys();
		value_reader->Refresh();
	}

private:
	reader_ptr_t value_reader;
	ListChild entries;
	std::optional<Vector> keys;
	std::optional<Vector> values;
	cxx::list_entry_t *map_entries = nullptr;
	cxx::varchar_t *key_data = nullptr;
	std::optional<cxx::Arena> key_heap;
};

reader_ptr_t CreateReader(const AvroType &avro_type, const AvroColumn &column) {
	switch (avro_type.type_id) {
	case LogicalTypeId::SQLNULL:
		return std::make_unique<NullReader>();
	case LogicalTypeId::BOOLEAN:
		return std::make_unique<BooleanReader>();
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return std::make_unique<IntegerReader>();
	case LogicalTypeId::TIME:
		return std::make_unique<TimeReader>(avro_type.is_timestamp_millis);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::BIGINT:
		return std::make_unique<BigIntReader>(avro_type.is_timestamp_millis);
	case LogicalTypeId::FLOAT:
		return std::make_unique<FloatReader>();
	case LogicalTypeId::DOUBLE:
		return std::make_unique<DoubleReader>();
	case LogicalTypeId::UUID:
		return std::make_unique<UUIDReader>();
	case LogicalTypeId::DECIMAL:
		switch (column.type.GetDecimalInternalTypeId()) {
		case LogicalTypeId::SMALLINT:
			return std::make_unique<DecimalReader<int16_t>>(avro_type.avro_type);
		case LogicalTypeId::INTEGER:
			return std::make_unique<DecimalReader<int32_t>>(avro_type.avro_type);
		case LogicalTypeId::BIGINT:
			return std::make_unique<DecimalReader<int64_t>>(avro_type.avro_type);
		case LogicalTypeId::HUGEINT:
			return std::make_unique<DecimalReader<cxx::int128_t>>(avro_type.avro_type);
		default:
			throw NotImplementedError("Unsupported decimal physical type");
		}
	case LogicalTypeId::BLOB:
		if (avro_type.avro_type != AVRO_FIXED && avro_type.avro_type != AVRO_BYTES) {
			throw NotImplementedError("Unknown Avro blob type");
		}
		return std::make_unique<BlobReader>(avro_type.avro_type);
	case LogicalTypeId::VARCHAR:
		return std::make_unique<VarcharReader>();
	case LogicalTypeId::ENUM: {
		auto enum_size = avro_type.enum_symbols.size();
		switch (column.type.GetEnumInternalTypeId()) {
		case LogicalTypeId::UTINYINT:
			return std::make_unique<EnumReader<uint8_t>>(enum_size);
		case LogicalTypeId::USMALLINT:
			return std::make_unique<EnumReader<uint16_t>>(enum_size);
		case LogicalTypeId::UINTEGER:
			return std::make_unique<EnumReader<uint32_t>>(enum_size);
		default:
			throw InternalError("Unsupported Enum Internal Type");
		}
	}
	case LogicalTypeId::STRUCT: {
		std::vector<reader_ptr_t> children;
		for (idx_t i = 0; i < avro_type.children.size(); i++) {
			children.push_back(CreateReader(avro_type.children[i].second, column.children[i]));
		}
		return std::make_unique<StructReader>(std::move(children));
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
		if (avro_type.avro_type == AVRO_ARRAY) {
			// the element of a LIST, or the key/value STRUCT of a MAP
			return std::make_unique<ListReader>(CreateReader(avro_type.children[0].second, column.children[0]));
		}
		return std::make_unique<MapReader>(CreateReader(avro_type.children[1].second, column.children[0].children[1]));
	case LogicalTypeId::UNION: {
		if (avro_type.NonNullUnionChildCount() == 0) {
			return std::make_unique<NullReader>();
		}
		if (avro_type.NonNullUnionChildCount() == 1) {
			auto &child = *avro_type.union_child_map.begin();
			return std::make_unique<CollapsedUnionReader>(
			    avro_type, CreateReader(avro_type.children[child.first].second, column), child.first);
		}
		std::vector<reader_ptr_t> members(avro_type.NonNullUnionChildCount());
		for (auto &entry : avro_type.union_child_map) {
			members[entry.second] = CreateReader(avro_type.children[entry.first].second, column.children[entry.second]);
		}
		return std::make_unique<UnionReader>(avro_type, std::move(members));
	}
	default:
		throw NotImplementedError("Reading Avro values as " + column.type.ToText() + " is not supported");
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct AvroBindData {
	explicit AvroBindData(std::unique_ptr<AvroFile> file_p) : file(std::move(file_p)) {
	}

	std::unique_ptr<AvroFile> file;
};

struct AvroGlobalState {
	std::atomic<idx_t> next_block_index {0};
};

//! The scan of one thread: it reads the blocks it claims, one at a time
class AvroScanState {
public:
	AvroScanState(const AvroFile &file, std::vector<idx_t> column_ids_p)
	    : file(file), column_ids(std::move(column_ids_p)) {
		if (file.root_is_struct) {
			// a record root may be wrapped in a union with null
			record_type = &file.avro_type;
			while (record_type->type_id == LogicalTypeId::UNION) {
				record_type = &record_type->children[record_type->union_child_map.begin()->first].second;
			}
			for (auto column_id : column_ids) {
				readers.push_back(CreateReader(record_type->children[column_id].second, file.columns[column_id]));
			}
		} else {
			readers.push_back(CreateReader(file.avro_type, file.columns[0]));
		}

		if (avro_file_block_reader_create(file.reader.get(), &block_reader)) {
			throw AvroError();
		}
		if (avro_generic_value_new(file.value_iface.get(), &value)) {
			avro_file_block_reader_close(block_reader);
			throw AvroError();
		}
	}
	~AvroScanState() {
		avro_value_decref(&value);
		avro_file_block_reader_close(block_reader);
	}

public:
	void Scan(cxx::DataChunk output) {
		if (!block_selected) {
			avro_value_reset(&value);
			if (avro_file_block_reader_select_block(block_reader, block_index)) {
				throw AvroError();
			}
			block_selected = true;
		}

		auto capacity = output.GetCapacity();
		std::vector<Vector> vectors;
		vectors.reserve(readers.size());
		for (idx_t i = 0; i < readers.size(); i++) {
			vectors.push_back(output.GetVector(i));
			vectors.back().SetSize(capacity);
		}
		for (idx_t i = 0; i < readers.size(); i++) {
			readers[i]->Reset(vectors[i]);
		}

		idx_t count = 0;
		int ret = 0;
		while (count < capacity && (ret = avro_file_block_reader_read_value(block_reader, &value)) == 0) {
			ReadRow(count++);
		}
		if (ret != 0 && ret != EOF) {
			throw AvroError();
		}

		for (idx_t i = 0; i < readers.size(); i++) {
			vectors[i].SetSize(count);
			readers[i]->Finalize();
		}
	}

private:
	void ReadRow(idx_t row) {
		if (!record_type) {
			readers[0]->Read(&value, row);
			return;
		}
		// pull up root struct into output chunk
		avro_value_t record = value;
		const AvroType *type = &file.avro_type;
		while (type->type_id == LogicalTypeId::UNION) {
			int discriminant = 0;
			avro_value_t branch;
			if (avro_value_get_discriminant(&record, &discriminant) ||
			    avro_value_get_current_branch(&record, &branch)) {
				throw AvroError();
			}
			if (discriminant < 0 || static_cast<idx_t>(discriminant) >= type->children.size()) {
				throw InvalidInputError("Invalid union tag");
			}
			type = &type->children[discriminant].second;
			if (type->type_id == LogicalTypeId::SQLNULL) {
				for (auto &reader : readers) {
					reader->SetNull(row);
				}
				return;
			}
			record = branch;
		}
		for (idx_t i = 0; i < readers.size(); i++) {
			avro_value_t field;
			if (avro_value_get_by_index(&record, column_ids[i], &field, nullptr)) {
				throw AvroError();
			}
			readers[i]->Read(&field, row);
		}
	}

public:
	const AvroFile &file;
	idx_t block_index = 0;
	bool block_selected = false;

private:
	std::vector<idx_t> column_ids;
	//! The root record whose fields are the columns, if the root is a record
	const AvroType *record_type = nullptr;
	std::vector<reader_ptr_t> readers;
	avro_file_block_reader_t block_reader = nullptr;
	avro_value_t value;
};

void AddColumnIdentifiers(cxx::TableFunction::BindInput &input, cxx::Context &context, const AvroColumn &column,
                          idx_t column_index, std::vector<idx_t> &child_path) {
	if (column.field_id) {
		input.SetColumnIdentifier(column_index, child_path, cxx::Value::Create(context, *column.field_id));
	}
	// a MAP addresses its keys and values directly, without the key/value STRUCT in between
	auto &children = column.type.GetTypeId() == LogicalTypeId::MAP ? column.children[0].children : column.children;
	for (idx_t i = 0; i < children.size(); i++) {
		child_path.push_back(i);
		AddColumnIdentifiers(input, context, children[i], column_index, child_path);
		child_path.pop_back();
	}
}

void AvroScanBind(cxx::TableFunction::BindInput &input) {
	auto context = input.GetContext();
	auto path = std::string(input.GetConstantArgument(0).Get<cxx::varchar_t>().view());
	auto file = std::make_unique<AvroFile>(context, path);

	for (idx_t col_idx = 0; col_idx < file->columns.size(); col_idx++) {
		auto &column = file->columns[col_idx];
		input.AddResultColumn(column.name, column.type);
		std::vector<idx_t> child_path;
		AddColumnIdentifiers(input, context, column, col_idx, child_path);
	}
	for (auto &entry : file->metadata) {
		input.AddFileMetadata(entry.first, cxx::Value::Create(context, cxx::varchar_t(entry.second)));
	}
	input.SetBindData<AvroBindData>(std::move(file));
}

void AvroScanInitGlobal(cxx::TableFunction::InitGlobalInput &input) {
	auto &file = *input.GetBindData<AvroBindData>().file;
	input.SetGlobalState<AvroGlobalState>();
	input.SetMaxThreads(std::max<idx_t>(file.NumBlocks(), 1));
}

void AvroScanInitLocal(cxx::TableFunction::InitLocalInput &input) {
	auto &file = *input.GetBindData<AvroBindData>().file;
	std::vector<idx_t> column_ids;
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		column_ids.push_back(input.GetColumnIndex(i));
	}
	input.SetLocalState<AvroScanState>(file, std::move(column_ids));
}

void AvroScanClaimBatch(cxx::TableFunction::ClaimBatchInput &input) {
	auto &file = *input.GetBindData<AvroBindData>().file;
	auto &gstate = input.GetGlobalState<AvroGlobalState>();
	auto &lstate = input.GetLocalState<AvroScanState>();
	auto block_index = gstate.next_block_index++;
	if (block_index >= file.NumBlocks()) {
		input.SetClaimed(false);
		return;
	}
	lstate.block_index = block_index;
	lstate.block_selected = false;
	input.SetClaimed(true);
}

void AvroScanExec(cxx::TableFunction::ExecInput &input) {
	input.GetLocalState<AvroScanState>().Scan(input.GetOutputChunk());
}

} // namespace

AvroFile::AvroFile(cxx::Context &context, const std::string &path) : buffer(AvroFileBuffer::Read(context, path)) {
	avro_file_reader_t file_reader;
	if (avro_file_reader_memory(buffer.data.get(), static_cast<int64_t>(buffer.size), &file_reader)) {
		throw AvroError();
	}
	reader.reset(file_reader);

	size_t file_block_count;
	if (avro_file_reader_get_block_count(reader.get(), &file_block_count)) {
		throw AvroError();
	}
	block_count = file_block_count;

	std::unique_ptr<std::remove_pointer_t<avro_schema_t>, SchemaDeleter> avro_schema(
	    avro_file_reader_get_writer_schema(reader.get()));
	auto schema_name = avro_schema_name(avro_schema.get());
	std::string root_name = schema_name ? schema_name : "avro_schema";

	avro_type = TransformSchema(avro_schema.get(), {});
	auto root = TransformAvroType(context, root_name, avro_type);
	value_iface.reset(avro_generic_class_from_schema(avro_schema.get()));
	if (!value_iface) {
		throw AvroError();
	}

	// special handling for root structs, we pull up the entries
	root_is_struct = root.type.GetTypeId() == LogicalTypeId::STRUCT;
	if (root_is_struct) {
		columns = std::move(root.children);
	} else {
		columns.push_back(std::move(root));
	}

	size_t metadata_count = 0;
	if (avro_file_reader_get_metadata_count(reader.get(), &metadata_count)) {
		throw InvalidInputError("Failed to get metadata count");
	}
	for (idx_t i = 0; i < metadata_count; i++) {
		const char *key = nullptr;
		const char *value = nullptr;
		size_t value_size = 0;
		if (avro_file_reader_get_metadata_by_index(reader.get(), i, &key, &value, &value_size)) {
			throw InvalidInputError("Failed to get metadata at index " + std::to_string(i));
		}
		if (!key) {
			continue;
		}
		metadata.emplace_back(key, value ? std::string(value, value_size) : std::string());
	}
}

void AvroReader::Register(cxx::Extension &extension, cxx::Context &context) {
	auto function = cxx::TableFunction::Create(extension);
	function.SetName("read_single_avro_file");
	function.GetSignature().AddParameter("path", context.CreateType(LogicalTypeId::VARCHAR));
	function.SetBindCallback(AvroScanBind)
	    .SetInitGlobalCallback(AvroScanInitGlobal)
	    .SetInitLocalCallback(AvroScanInitLocal)
	    .SetClaimBatchCallback(AvroScanClaimBatch)
	    .SetExecCallback(AvroScanExec)
	    .SetProjectionPushdown(true);
	function.Register();

	auto multi_file_function = cxx::MultiFileFunction::Create(extension);
	multi_file_function.SetName("read_avro").SetSingleFileFunction("read_single_avro_file").SetReaderType("Avro");
	multi_file_function.Register();
}

} // namespace avro

} // namespace duckdb
