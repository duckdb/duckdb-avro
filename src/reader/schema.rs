//! The Avro schema as DuckDB reads it: every node of the writer's schema becomes a [`Column`] with the DuckDB type it
//! is read as, the [`Shape`] its values are decoded by, and the field id it carries (what a multi-file scan maps the
//! columns of several files onto one another by).
//!
//! The rules follow the C++ extension: records are STRUCTs (an empty record the `"NULL"` type), arrays LISTs (or a MAP
//! when annotated with `"logicalType": "map"` over key/value records; the records may be wrapped in a union with null,
//! as BigQuery's Iceberg manifest exports write them, and a null item is then skipped when decoding since a MAP has no
//! place for an entry without key and value), maps `MAP(VARCHAR, v)`, enums ENUMs, fixed and bytes BLOBs. A union drops its null branches: one branch left is read as that type, nullable; none left is the
//! `"NULL"` type; several become `UNION(u<branch index> T, ...)`. Logical types are honoured on primitives only.
//!
//! As the spec asks, an unknown logical type (`char`, `time-interval-micros`, vendor names of every kind) and an
//! invalid one (a `decimal` without a usable precision or scale) are ignored: the underlying type is read. A known
//! logical type on an Avro type it does not belong to (`date` on a `string`) stays an error.

use avro_core::{Field, Schema, SchemaId, SchemaKind};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Error, Factory, LogicalType, Result, TypeId, Value};

/// The column name of a root value that is neither a record nor a named type.
const UNNAMED_ROOT: &str = "avro_schema";
/// The widest DECIMAL the engine has; a `decimal` of more digits is read as its underlying `bytes` / `fixed`.
const MAX_DECIMAL_PRECISION: i64 = 38;

/// How the values of a schema node are decoded.
#[derive(Debug, Clone)]
pub(crate) enum Shape {
    /// Every value is NULL; the schema node (`null`, an empty record, a union of such) is skipped with the decoder.
    Null(SchemaId),
    Boolean,
    /// An Avro `int`: INTEGER, or DATE (days).
    Int,
    /// An Avro `long`: BIGINT, TIME (micros), the TIMESTAMP variants (micros or nanos).
    Long,
    /// An Avro `int` of `time-millis`, range-checked and scaled to micros.
    TimeMillis,
    /// An Avro `long` of `time-micros`, range-checked.
    TimeMicros,
    /// An Avro `long` of `timestamp-millis` / `local-timestamp-millis`, scaled to micros.
    TimestampMillis,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    /// A `fixed(16)` of `uuid`.
    Uuid,
    /// A `string` of `uuid`: the RFC 4122 text form, parsed.
    UuidText,
    /// A `fixed(12)` of `duration`: months, days, milliseconds as little-endian unsigned 32-bit integers.
    Duration,
    /// A `decimal` on `bytes` (`fixed: None`) or on a fixed of the given size, stored in `width` bytes.
    Decimal {
        fixed: Option<usize>,
        width: usize,
    },
    /// An enum of `count` symbols, stored in `width` bytes.
    Enum {
        count: usize,
        width: usize,
    },
    Struct(Vec<Column>),
    /// An Avro `array`; `min_item_size` is [`Schema::min_encoded_size`] of its items, the bound of a block count.
    List {
        element: Box<Column>,
        min_item_size: usize,
    },
    /// An Avro `map` (`from_array: false`, string keys), or an array of key/value records annotated as a map;
    /// `min_item_size` is the fewest bytes one entry occupies, the bound of a block count. `item_union` is set when
    /// the array's items are a union of null and the record: the branch ids and the record's branch; a null item is
    /// skipped.
    Map {
        key: Box<Column>,
        value: Box<Column>,
        from_array: bool,
        item_union: Option<(Vec<SchemaId>, usize)>,
        min_item_size: usize,
    },
    /// A union of several non-null branches: `branch_members[b]` is the DuckDB member a branch is read into (`None`
    /// for a null branch, whose bytes are skipped through `branch_ids[b]`).
    Union {
        branch_ids: Vec<SchemaId>,
        branch_members: Vec<Option<usize>>,
        members: Vec<Column>,
    },
    /// A union with one non-null branch, read as that branch's type: `branch` is its index.
    Nullable {
        branch_ids: Vec<SchemaId>,
        branch: usize,
        child: Box<Column>,
    },
}

/// A column or nested field as DuckDB sees it.
#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) type_: LogicalType,
    pub(crate) shape: Shape,
    /// The Iceberg `field-id` / `element-id` / `key-id` / `value-id` the node carries.
    pub(crate) field_id: Option<i32>,
}

impl Column {
    fn new(name: &str, type_: LogicalType, shape: Shape) -> Column {
        Column { name: name.to_owned(), type_, shape, field_id: None }
    }

    /// The nested fields of the column, as the engine addresses them by child path: the fields of a STRUCT, the element
    /// of a LIST, the key and the value of a MAP (without the key/value STRUCT in between), the members of a UNION.
    pub(crate) fn id_children(&self) -> Vec<&Column> {
        match &self.shape {
            Shape::Struct(fields) => fields.iter().collect(),
            Shape::List { element, .. } => vec![element],
            Shape::Map { key, value, .. } => vec![key, value],
            Shape::Union { members, .. } => members.iter().collect(),
            Shape::Nullable { child, .. } => child.id_children(),
            _ => Vec::new(),
        }
    }
}

/// The columns a file is read as.
#[derive(Debug)]
pub(crate) struct RootColumns {
    pub(crate) columns: Vec<Column>,
    /// Whether the root is a record (possibly wrapped in a union with null) whose fields are the columns; otherwise
    /// the one column reads the root value.
    pub(crate) root_is_struct: bool,
}

/// Converts the root of `schema` into the columns of the scan.
pub(crate) fn root_columns(factory: Factory, schema: &Schema) -> Result<RootColumns> {
    let root_name = schema[schema.root()].kind.name().map_or(UNNAMED_ROOT, |n| n.name.as_str());
    let mut converter = Converter { factory, schema, ancestors: Vec::new() };
    let root = converter.convert(schema.root(), root_name)?;
    // the fields of a root record are the columns; a collapsed union of null and a record counts as a record
    let mut shape = &root.shape;
    while let Shape::Nullable { child, .. } = shape {
        shape = &child.shape;
    }
    if let Shape::Struct(_) = shape {
        let mut column = root;
        loop {
            match column.shape {
                Shape::Nullable { child, .. } => column = *child,
                Shape::Struct(fields) => return Ok(RootColumns { columns: fields, root_is_struct: true }),
                _ => return Err(Error::internal("root record shape changed while unwrapping")),
            }
        }
    }
    Ok(RootColumns { columns: vec![root], root_is_struct: false })
}

/// Whether a value of the (resolved) node is always NULL: `null` or an empty record.
pub(crate) fn is_null_type(schema: &Schema, id: SchemaId) -> bool {
    match &schema.resolved(id).kind {
        SchemaKind::Null => true,
        SchemaKind::Record { fields, .. } => fields.is_empty(),
        _ => false,
    }
}

/// The storage width in bytes of a DECIMAL of `precision` digits (`LogicalType::GetDecimalInternalTypeId`).
pub(crate) fn decimal_width(precision: u8) -> usize {
    match precision {
        0..=4 => 2,
        5..=9 => 4,
        10..=18 => 8,
        _ => 16,
    }
}

/// The storage width in bytes of an ENUM of `count` symbols (`LogicalType::GetEnumInternalTypeId`).
pub(crate) fn enum_width(count: usize) -> usize {
    if count <= u8::MAX as usize {
        1
    } else if count <= u16::MAX as usize {
        2
    } else {
        4
    }
}

/// The `decimal` attributes a reader honours: `(precision, scale)` when the precision is present and one a DECIMAL
/// holds (1..=38) and the scale (0 when absent, as the spec defaults it) lies in `0..=precision`; `None` otherwise,
/// which makes the annotation invalid and so ignored (the underlying `bytes` / `fixed` is read). The schema parser
/// leaves an attribute that is not a JSON integer (Hive 0.x wrote `"precision": "5"`) absent.
pub(crate) fn decimal_annotation(precision: Option<i64>, scale: Option<i64>) -> Option<(u8, u8)> {
    let precision = precision?;
    let scale = scale.unwrap_or(0);
    if !(1..=MAX_DECIMAL_PRECISION).contains(&precision) || !(0..=precision).contains(&scale) {
        return None;
    }
    Some((precision as u8, scale as u8))
}

/// How a union is read, from which of its branches are null types: a null branch has no DuckDB member, the others
/// are the members in branch order. No member: every value is NULL. One: that branch's type, nullable. More: a UNION.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnionLayout {
    Null,
    /// The one non-null branch.
    Nullable {
        branch: usize,
    },
    Union {
        member_count: usize,
    },
}

/// The union rule over `null_branch` (one flag per branch): the DuckDB member of every branch (`None` for a null
/// branch, else its position among the non-null branches) and the layout.
pub(crate) fn union_layout(null_branch: &[bool]) -> (Vec<Option<usize>>, UnionLayout) {
    let mut branch_members = vec![None; null_branch.len()];
    let mut member_count = 0;
    let mut first = None;
    for (branch, &is_null) in null_branch.iter().enumerate() {
        if !is_null {
            branch_members[branch] = Some(member_count);
            member_count += 1;
            first.get_or_insert(branch);
        }
    }
    let layout = match (member_count, first) {
        (0, _) => UnionLayout::Null,
        (1, Some(branch)) => UnionLayout::Nullable { branch },
        _ => UnionLayout::Union { member_count },
    };
    (branch_members, layout)
}

/// The DuckDB member name of union branch `index`: `u<index>` (what `format!("u{index}")` gives). The digits are
/// pushed as one-byte `&str`s (a `char` push of a symbolic value makes CBMC, in the proof harnesses, branch on its
/// UTF-8 encoding).
pub(crate) fn union_member_name(index: usize) -> String {
    const DIGITS: [&str; 10] = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];
    let mut digits = [0u8; 20];
    let mut n = 0;
    let mut rest = index;
    loop {
        digits[n] = (rest % 10) as u8;
        n += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let mut name = String::with_capacity(n + 1);
    name.push('u');
    while n > 0 {
        n -= 1;
        name.push_str(DIGITS[usize::from(digits[n])]);
    }
    name
}

/// The DuckDB logical type a primitive node's `logicalType` attribute selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Annotated {
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis { utc: bool },
    TimestampMicros { utc: bool },
    TimestampNanos { utc: bool },
    LocalTimestampMillis,
    LocalTimestampMicros,
    LocalTimestampNanos,
    Decimal { precision: u8, scale: u8 },
    Uuid,
    Duration,
}

/// The `logicalType` attribute `logical_type` of a primitive node, with its `adjust-to-utc` (false when absent),
/// `precision` and `scale` attributes; `None` when the name is unknown or the annotation invalid (ignored per the
/// spec: the underlying type is read).
pub(crate) fn parse_annotation(
    logical_type: &str,
    adjust_to_utc: bool,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Option<Annotated> {
    let utc = adjust_to_utc;
    Some(match logical_type {
        "date" => Annotated::Date,
        "decimal" => {
            // invalid (so ignored): no integer precision, a precision no DECIMAL holds, a scale outside 0..=precision
            let (precision, scale) = decimal_annotation(precision, scale)?;
            Annotated::Decimal { precision, scale }
        }
        "time-millis" => Annotated::TimeMillis,
        "time-micros" => Annotated::TimeMicros,
        "timestamp-millis" => Annotated::TimestampMillis { utc },
        "timestamp-micros" => Annotated::TimestampMicros { utc },
        "timestamp-nanos" => Annotated::TimestampNanos { utc },
        "uuid" => Annotated::Uuid,
        "duration" => Annotated::Duration,
        "local-timestamp-millis" => Annotated::LocalTimestampMillis,
        "local-timestamp-micros" => Annotated::LocalTimestampMicros,
        "local-timestamp-nanos" => Annotated::LocalTimestampNanos,
        // unknown: ignored, the underlying type is read
        _ => return None,
    })
}

/// A primitive Avro type as [`leaf`] sees it (`null` is not one: it is the `"NULL"` type whatever its annotation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeafKind {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    Fixed(usize),
}

/// The DuckDB type [`leaf`] selects: a type by id, or a DECIMAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeafType {
    Id(TypeId),
    Decimal { precision: u8, scale: u8 },
}

/// Why [`leaf`] refuses a node: a known logical type on an Avro type it does not belong to, `uuid` on a fixed whose
/// size is not 16, `duration` on a fixed whose size is not 12.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeafError {
    Mismatch,
    UuidSize,
    DurationSize,
}

/// The primitive Avro type `kind` of a schema node, or `None` for a nested or named type.
fn leaf_kind(kind: &SchemaKind) -> Option<LeafKind> {
    Some(match kind {
        SchemaKind::Boolean => LeafKind::Boolean,
        SchemaKind::Int => LeafKind::Int,
        SchemaKind::Long => LeafKind::Long,
        SchemaKind::Float => LeafKind::Float,
        SchemaKind::Double => LeafKind::Double,
        SchemaKind::String => LeafKind::String,
        SchemaKind::Bytes => LeafKind::Bytes,
        SchemaKind::Fixed { size, .. } => LeafKind::Fixed(*size),
        _ => return None,
    })
}

/// The type-mapping rule for the primitives: the DuckDB type and the shape a node of `kind` with `annotation` (its
/// parsed `logicalType`, `None` when absent, unknown or invalid) is read as.
pub(crate) fn leaf(kind: LeafKind, annotation: Option<Annotated>) -> std::result::Result<(LeafType, Shape), LeafError> {
    use LeafType::Id;
    let timestamp = |utc: bool| Id(if utc { type_id::TIMESTAMP_TZ } else { type_id::TIMESTAMP });
    Ok(match (kind, annotation) {
        (LeafKind::Boolean, None) => (Id(type_id::BOOLEAN), Shape::Boolean),
        (LeafKind::Int, None) => (Id(type_id::INTEGER), Shape::Int),
        (LeafKind::Int, Some(Annotated::Date)) => (Id(type_id::DATE), Shape::Int),
        (LeafKind::Int, Some(Annotated::TimeMillis)) => (Id(type_id::TIME), Shape::TimeMillis),
        (LeafKind::Long, None) => (Id(type_id::BIGINT), Shape::Long),
        (LeafKind::Long, Some(Annotated::TimeMicros)) => (Id(type_id::TIME), Shape::TimeMicros),
        (LeafKind::Long, Some(Annotated::TimestampMillis { utc })) => (timestamp(utc), Shape::TimestampMillis),
        (LeafKind::Long, Some(Annotated::TimestampMicros { utc })) => (timestamp(utc), Shape::Long),
        (LeafKind::Long, Some(Annotated::TimestampNanos { utc })) => {
            (Id(if utc { type_id::TIMESTAMP_TZ_NS } else { type_id::TIMESTAMP_NS }), Shape::Long)
        }
        (LeafKind::Long, Some(Annotated::LocalTimestampMillis)) => (Id(type_id::TIMESTAMP), Shape::TimestampMillis),
        (LeafKind::Long, Some(Annotated::LocalTimestampMicros)) => (Id(type_id::TIMESTAMP), Shape::Long),
        (LeafKind::Long, Some(Annotated::LocalTimestampNanos)) => (Id(type_id::TIMESTAMP_NS), Shape::Long),
        (LeafKind::Float, None) => (Id(type_id::FLOAT), Shape::Float),
        (LeafKind::Double, None) => (Id(type_id::DOUBLE), Shape::Double),
        (LeafKind::String, None) => (Id(type_id::VARCHAR), Shape::String),
        // the spec's uuid annotates a string (the fixed(16) form came with Avro 1.12)
        (LeafKind::String, Some(Annotated::Uuid)) => (Id(type_id::UUID), Shape::UuidText),
        (LeafKind::Bytes, None) => (Id(type_id::BLOB), Shape::Bytes),
        (LeafKind::Bytes, Some(Annotated::Decimal { precision, scale })) => {
            (LeafType::Decimal { precision, scale }, Shape::Decimal { fixed: None, width: decimal_width(precision) })
        }
        (LeafKind::Fixed(size), None) => (Id(type_id::BLOB), Shape::Fixed(size)),
        (LeafKind::Fixed(size), Some(Annotated::Decimal { precision, scale })) => (
            LeafType::Decimal { precision, scale },
            Shape::Decimal { fixed: Some(size), width: decimal_width(precision) },
        ),
        (LeafKind::Fixed(16), Some(Annotated::Uuid)) => (Id(type_id::UUID), Shape::Uuid),
        (LeafKind::Fixed(_), Some(Annotated::Uuid)) => return Err(LeafError::UuidSize),
        (LeafKind::Fixed(12), Some(Annotated::Duration)) => (Id(type_id::INTERVAL), Shape::Duration),
        (LeafKind::Fixed(_), Some(Annotated::Duration)) => return Err(LeafError::DurationSize),
        (_, Some(_)) => return Err(LeafError::Mismatch),
    })
}

struct Converter<'a> {
    factory: Factory,
    schema: &'a Schema,
    /// The records being converted, outermost first: a reference to one of them is a recursive type.
    ancestors: Vec<SchemaId>,
}

impl Converter<'_> {
    fn null_type(&self) -> Result<LogicalType> {
        LogicalType::parse(self.factory, "\"NULL\"")
    }

    fn primitive(&self, id: duckdb_ext_api::TypeId) -> Result<LogicalType> {
        LogicalType::new(self.factory, id)
    }

    /// The `logicalType` attribute of a primitive node; `None` when there is none, or when it is unknown or invalid
    /// (ignored per the spec: the underlying type is read).
    fn annotation(&self, id: SchemaId) -> Option<Annotated> {
        let node = &self.schema[id];
        let logical_type = node.logical_type.as_deref()?;
        parse_annotation(logical_type, node.adjust_to_utc.unwrap_or(false), node.precision, node.scale)
    }

    /// A primitive node: [`leaf`] of its kind and annotation, with the engine's type for what it selects.
    fn leaf_column(&self, id: SchemaId, kind: LeafKind, name: &str) -> Result<Column> {
        let (type_, shape) = match leaf(kind, self.annotation(id)) {
            Ok(leaf) => leaf,
            Err(LeafError::Mismatch) => return Err(self.mismatch(id)),
            Err(LeafError::UuidSize) => {
                return Err(Error::invalid_configuration("logical type is uuid, but size != 16"));
            }
            Err(LeafError::DurationSize) => {
                return Err(Error::invalid_input("Avro logical type 'duration' requires a fixed of size 12"));
            }
        };
        let type_ = match type_ {
            LeafType::Id(type_id) => self.primitive(type_id)?,
            LeafType::Decimal { precision, scale } => LogicalType::decimal(self.factory, precision, scale)?,
        };
        Ok(Column::new(name, type_, shape))
    }

    fn mismatch(&self, id: SchemaId) -> Error {
        let node = &self.schema[id];
        Error::invalid_input(format!(
            "Avro logical type '{}' is not supported on Avro type '{}'",
            node.logical_type.as_deref().unwrap_or(""),
            node.kind.type_name()
        ))
    }

    /// Converts the node `id` into the column `name`.
    fn convert(&mut self, id: SchemaId, name: &str) -> Result<Column> {
        let id = self.schema.resolve(id);
        let node = &self.schema[id];
        if let Some(kind) = leaf_kind(&node.kind) {
            return self.leaf_column(id, kind, name);
        }
        match &node.kind {
            // whatever its annotation
            SchemaKind::Null => Ok(Column::new(name, self.null_type()?, Shape::Null(id))),
            SchemaKind::Boolean
            | SchemaKind::Int
            | SchemaKind::Long
            | SchemaKind::Float
            | SchemaKind::Double
            | SchemaKind::String
            | SchemaKind::Bytes
            | SchemaKind::Fixed { .. } => Err(Error::internal("primitive Avro type not taken by leaf_kind")),
            SchemaKind::Enum { symbols, .. } => {
                let values = symbols
                    .iter()
                    .map(|symbol| Value::varchar(self.factory, symbol))
                    .collect::<Result<Vec<Value>>>()?;
                let params: Vec<(&str, &Value)> = values.iter().map(|v| ("", v)).collect();
                let type_ = LogicalType::with_params(self.factory, type_id::ENUM, &params)?;
                Ok(Column::new(name, type_, Shape::Enum { count: symbols.len(), width: enum_width(symbols.len()) }))
            }
            SchemaKind::Record { name: record_name, fields, .. } => {
                if self.ancestors.contains(&id) {
                    return Err(Error::invalid_input(format!(
                        "Recursive Avro types not supported: {}",
                        record_name.name
                    )));
                }
                if fields.is_empty() {
                    // ignored, but the marker keeps the offsets right
                    return Ok(Column::new(name, self.null_type()?, Shape::Null(id)));
                }
                self.ancestors.push(id);
                let children = self.fields(fields);
                self.ancestors.pop();
                let children = children?;
                let type_ = self.struct_type(&children)?;
                Ok(Column::new(name, type_, Shape::Struct(children)))
            }
            SchemaKind::Array { items, element_id } => {
                let mut element = self.convert(*items, "list")?;
                element.field_id = element_id.and_then(|id| i32::try_from(id).ok());
                let min_item_size = self.schema.min_encoded_size(*items);
                if node.logical_type.as_deref() != Some("map") {
                    let type_ = LogicalType::list(self.factory, &element.type_)?;
                    return Ok(Column::new(name, type_, Shape::List { element: Box::new(element), min_item_size }));
                }
                // the items may be a union of null and the key/value record (BigQuery's Iceberg manifest export)
                let (item_union, shape) = match element.shape {
                    Shape::Nullable { branch_ids, branch, child } => (Some((branch_ids, branch)), child.shape),
                    shape => (None, shape),
                };
                let Shape::Struct(mut fields) = shape else {
                    return Err(Error::invalid_input(
                        "Avro array with logical type 'map' must contain key/value records",
                    ));
                };
                if fields.len() != 2 {
                    return Err(Error::invalid_input(
                        "Avro array with logical type 'map' must contain key/value records",
                    ));
                }
                let mut value = fields.pop().ok_or_else(|| Error::internal("map value field"))?;
                let mut key = fields.pop().ok_or_else(|| Error::internal("map key field"))?;
                key.name = "key".to_owned();
                value.name = "value".to_owned();
                let type_ = self.map_type(&key.type_, &value.type_)?;
                Ok(Column::new(
                    name,
                    type_,
                    Shape::Map {
                        key: Box::new(key),
                        value: Box::new(value),
                        from_array: true,
                        item_union,
                        min_item_size,
                    },
                ))
            }
            SchemaKind::Map { values, key_id, value_id } => {
                let mut key = Column::new("key", self.primitive(type_id::VARCHAR)?, Shape::String);
                key.field_id = key_id.and_then(|id| i32::try_from(id).ok());
                let mut value = self.convert(*values, "value")?;
                value.field_id = value_id.and_then(|id| i32::try_from(id).ok());
                let type_ = self.map_type(&key.type_, &value.type_)?;
                // the key is a string: a length prefix at least
                let min_item_size = self.schema.min_encoded_size(*values).saturating_add(1);
                Ok(Column::new(
                    name,
                    type_,
                    Shape::Map {
                        key: Box::new(key),
                        value: Box::new(value),
                        from_array: false,
                        item_union: None,
                        min_item_size,
                    },
                ))
            }
            SchemaKind::Union { branches } => {
                let branch_ids = branches.clone();
                let mut children = Vec::with_capacity(branches.len());
                for (index, &branch) in branches.iter().enumerate() {
                    children.push(self.convert(branch, &union_member_name(index))?);
                }
                let null_branch: Vec<bool> =
                    children.iter().map(|child| matches!(child.shape, Shape::Null(_))).collect();
                let (branch_members, layout) = union_layout(&null_branch);
                let mut members: Vec<Column> =
                    children.into_iter().filter(|child| !matches!(child.shape, Shape::Null(_))).collect();
                match layout {
                    UnionLayout::Null => {
                        if branches.is_empty() {
                            return Err(Error::invalid_input("Empty union type"));
                        }
                        Ok(Column::new(name, self.null_type()?, Shape::Null(id)))
                    }
                    UnionLayout::Nullable { branch } => {
                        let child = members.pop().ok_or_else(|| Error::internal("union member"))?;
                        let type_ = child.type_.try_clone()?;
                        Ok(Column::new(name, type_, Shape::Nullable { branch_ids, branch, child: Box::new(child) }))
                    }
                    UnionLayout::Union { .. } => {
                        let types = members
                            .iter()
                            .map(|m| Value::type_value(self.factory, &m.type_))
                            .collect::<Result<Vec<_>>>()?;
                        let params: Vec<(&str, &Value)> =
                            members.iter().zip(types.iter()).map(|(m, t)| (m.name.as_str(), t)).collect();
                        let type_ = LogicalType::with_params(self.factory, type_id::UNION, &params)?;
                        Ok(Column::new(name, type_, Shape::Union { branch_ids, branch_members, members }))
                    }
                }
            }
            SchemaKind::Ref { .. } => Err(Error::internal("unresolved Avro schema reference")),
        }
    }

    fn fields(&mut self, fields: &[Field]) -> Result<Vec<Column>> {
        let mut children = Vec::with_capacity(fields.len());
        for field in fields {
            if field.name.is_empty() {
                return Err(Error::invalid_input("Empty avro field name"));
            }
            let mut child = self.convert(field.ty, &field.name)?;
            child.field_id = field.field_id.and_then(|id| i32::try_from(id).ok());
            children.push(child);
        }
        Ok(children)
    }

    fn struct_type(&self, children: &[Column]) -> Result<LogicalType> {
        let fields: Vec<(&str, &LogicalType)> = children.iter().map(|c| (c.name.as_str(), &c.type_)).collect();
        LogicalType::struct_(self.factory, &fields)
    }

    fn map_type(&self, key: &LogicalType, value: &LogicalType) -> Result<LogicalType> {
        let key = Value::type_value(self.factory, key)?;
        let value = Value::type_value(self.factory, value)?;
        LogicalType::with_params(self.factory, type_id::MAP, &[("", &key), ("", &value)])
    }
}
