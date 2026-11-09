//! The Avro schema as DuckDB reads it: every node of the writer's schema becomes a [`Column`] with the DuckDB type it
//! is read as, the [`Shape`] its values are decoded by, and the field id it carries (what a multi-file scan maps the
//! columns of several files onto one another by).
//!
//! The rules follow the C++ extension: records are STRUCTs (an empty record the `"NULL"` type), arrays LISTs (or a MAP
//! when annotated with `"logicalType": "map"` over key/value records), maps `MAP(VARCHAR, v)`, enums ENUMs, fixed and
//! bytes BLOBs. A union drops its null branches: one branch left is read as that type, nullable; none left is the
//! `"NULL"` type; several become `UNION(u<branch index> T, ...)`. Logical types are honoured on primitives only.

use avro_core::{Field, Schema, SchemaId, SchemaKind};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Error, Factory, LogicalType, Result, Value};

/// The column name of a root value that is neither a record nor a named type.
const UNNAMED_ROOT: &str = "avro_schema";

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
    /// An Avro `int` of `time-millis`, scaled to micros.
    TimeMillis,
    /// An Avro `long` of `timestamp-millis` / `local-timestamp-millis`, scaled to micros.
    TimestampMillis,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    /// A `fixed(16)` of `uuid`.
    Uuid,
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
    /// `min_item_size` is the fewest bytes one entry occupies, the bound of a block count.
    Map {
        key: Box<Column>,
        value: Box<Column>,
        from_array: bool,
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

/// The DuckDB logical type a primitive node's `logicalType` attribute selects.
enum Annotated {
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis { utc: bool },
    TimestampMicros { utc: bool },
    TimestampNanos { utc: bool },
    LocalTimestampMillis,
    Decimal { precision: u8, scale: u8 },
    Uuid,
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

    /// The `logicalType` attribute of a primitive node, validated against the Avro type it annotates.
    fn annotation(&self, id: SchemaId) -> Result<Option<Annotated>> {
        let node = &self.schema[id];
        let Some(logical_type) = node.logical_type.as_deref() else { return Ok(None) };
        let utc = node.adjust_to_utc.unwrap_or(false);
        let annotated = match logical_type {
            "date" => Annotated::Date,
            "decimal" => {
                let precision = node.precision.unwrap_or(0);
                let scale = node.scale.unwrap_or(0);
                let precision = u8::try_from(precision)
                    .map_err(|_| Error::invalid_input(format!("Avro decimal precision {precision} is out of range")))?;
                let scale = u8::try_from(scale)
                    .map_err(|_| Error::invalid_input(format!("Avro decimal scale {scale} is out of range")))?;
                Annotated::Decimal { precision, scale }
            }
            "time-millis" => Annotated::TimeMillis,
            "time-micros" => Annotated::TimeMicros,
            "timestamp-millis" => Annotated::TimestampMillis { utc },
            "timestamp-micros" => Annotated::TimestampMicros { utc },
            "timestamp-nanos" => Annotated::TimestampNanos { utc },
            "uuid" => {
                if node.fixed_size() != Some(16) {
                    return Err(Error::invalid_configuration("logical type is uuid, but size != 16"));
                }
                Annotated::Uuid
            }
            "local-timestamp-millis" => Annotated::LocalTimestampMillis,
            other => return Err(Error::not_implemented(format!("Unknown Avro logical type {other}"))),
        };
        Ok(Some(annotated))
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
        match &node.kind {
            SchemaKind::Null => {
                self.annotation(id)?;
                Ok(Column::new(name, self.null_type()?, Shape::Null(id)))
            }
            SchemaKind::Boolean => {
                if self.annotation(id)?.is_some() {
                    return Err(self.mismatch(id));
                }
                Ok(Column::new(name, self.primitive(type_id::BOOLEAN)?, Shape::Boolean))
            }
            SchemaKind::Int => {
                let (type_, shape) = match self.annotation(id)? {
                    None => (self.primitive(type_id::INTEGER)?, Shape::Int),
                    Some(Annotated::Date) => (self.primitive(type_id::DATE)?, Shape::Int),
                    Some(Annotated::TimeMillis) => (self.primitive(type_id::TIME)?, Shape::TimeMillis),
                    Some(_) => return Err(self.mismatch(id)),
                };
                Ok(Column::new(name, type_, shape))
            }
            SchemaKind::Long => {
                let timestamp = |utc: bool| if utc { type_id::TIMESTAMP_TZ } else { type_id::TIMESTAMP };
                let (type_, shape) = match self.annotation(id)? {
                    None => (self.primitive(type_id::BIGINT)?, Shape::Long),
                    Some(Annotated::TimeMicros) => (self.primitive(type_id::TIME)?, Shape::Long),
                    Some(Annotated::TimestampMillis { utc }) => {
                        (self.primitive(timestamp(utc))?, Shape::TimestampMillis)
                    }
                    Some(Annotated::TimestampMicros { utc }) => (self.primitive(timestamp(utc))?, Shape::Long),
                    Some(Annotated::TimestampNanos { utc }) => {
                        let id = if utc { type_id::TIMESTAMP_TZ_NS } else { type_id::TIMESTAMP_NS };
                        (self.primitive(id)?, Shape::Long)
                    }
                    Some(Annotated::LocalTimestampMillis) => {
                        (self.primitive(type_id::TIMESTAMP)?, Shape::TimestampMillis)
                    }
                    Some(_) => return Err(self.mismatch(id)),
                };
                Ok(Column::new(name, type_, shape))
            }
            SchemaKind::Float => {
                if self.annotation(id)?.is_some() {
                    return Err(self.mismatch(id));
                }
                Ok(Column::new(name, self.primitive(type_id::FLOAT)?, Shape::Float))
            }
            SchemaKind::Double => {
                if self.annotation(id)?.is_some() {
                    return Err(self.mismatch(id));
                }
                Ok(Column::new(name, self.primitive(type_id::DOUBLE)?, Shape::Double))
            }
            SchemaKind::String => {
                if self.annotation(id)?.is_some() {
                    return Err(self.mismatch(id));
                }
                Ok(Column::new(name, self.primitive(type_id::VARCHAR)?, Shape::String))
            }
            SchemaKind::Bytes => match self.annotation(id)? {
                None => Ok(Column::new(name, self.primitive(type_id::BLOB)?, Shape::Bytes)),
                Some(Annotated::Decimal { precision, scale }) => Ok(Column::new(
                    name,
                    LogicalType::decimal(self.factory, precision, scale)?,
                    Shape::Decimal { fixed: None, width: decimal_width(precision) },
                )),
                Some(_) => Err(self.mismatch(id)),
            },
            SchemaKind::Fixed { size, .. } => match self.annotation(id)? {
                None => Ok(Column::new(name, self.primitive(type_id::BLOB)?, Shape::Fixed(*size))),
                Some(Annotated::Decimal { precision, scale }) => Ok(Column::new(
                    name,
                    LogicalType::decimal(self.factory, precision, scale)?,
                    Shape::Decimal { fixed: Some(*size), width: decimal_width(precision) },
                )),
                Some(Annotated::Uuid) => Ok(Column::new(name, self.primitive(type_id::UUID)?, Shape::Uuid)),
                Some(_) => Err(self.mismatch(id)),
            },
            SchemaKind::Enum { symbols, .. } => {
                let values = symbols
                    .iter()
                    .map(|symbol| Value::varchar(self.factory, symbol))
                    .collect::<Result<Vec<Value>>>()?;
                let params: Vec<(&str, &Value)> = values.iter().map(|v| ("", v)).collect();
                let type_ = LogicalType::with_params(self.factory, type_id::ENUM, &params)?;
                Ok(Column::new(name, type_, Shape::Enum { count: symbols.len(), width: enum_width(symbols.len()) }))
            }
            SchemaKind::Record { name: record_name, fields } => {
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
                let Shape::Struct(mut fields) = element.shape else {
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
                    Shape::Map { key: Box::new(key), value: Box::new(value), from_array: true, min_item_size },
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
                    Shape::Map { key: Box::new(key), value: Box::new(value), from_array: false, min_item_size },
                ))
            }
            SchemaKind::Union { branches } => {
                let mut members = Vec::new();
                let mut branch_members = Vec::with_capacity(branches.len());
                let mut branch_ids = Vec::with_capacity(branches.len());
                for (index, &branch) in branches.iter().enumerate() {
                    branch_ids.push(branch);
                    let child = self.convert(branch, &format!("u{index}"))?;
                    if let Shape::Null(_) = child.shape {
                        branch_members.push(None);
                        continue;
                    }
                    branch_members.push(Some(members.len()));
                    members.push(child);
                }
                match members.len() {
                    0 => {
                        if branches.is_empty() {
                            return Err(Error::invalid_input("Empty union type"));
                        }
                        Ok(Column::new(name, self.null_type()?, Shape::Null(id)))
                    }
                    1 => {
                        let branch = branch_members
                            .iter()
                            .position(|m| m.is_some())
                            .ok_or_else(|| Error::internal("union member without a branch"))?;
                        let child = members.pop().ok_or_else(|| Error::internal("union member"))?;
                        let type_ = child.type_.try_clone()?;
                        Ok(Column::new(name, type_, Shape::Nullable { branch_ids, branch, child: Box::new(child) }))
                    }
                    _ => {
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
