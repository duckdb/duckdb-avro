//! How Avro schemas map onto DuckDB types.
//!
//! An Avro schema is first converted into an [`AvroType`] tree, which keeps what is needed to decode values, and then
//! into the [`AvroColumn`]s DuckDB sees. Unions with NULL get special treatment: the NULL does not become a union
//! member, and a union that is left with a single member is replaced by that member.

use duckdb_neo::{
    Parameters, QueryParameter, Result,
    connection::Context,
    logical_type::{LogicalType, LogicalTypeID},
};

use crate::avro::schema::{Node, NodeId, Schema};
use crate::error::{internal, invalid_configuration, invalid_input};

/// The kind of an Avro schema node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvroKind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record,
    Enum,
    Fixed,
    Array,
    Map,
    Union,
}

/// How the values of a primitive are converted on the way into DuckDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conversion {
    None,
    /// `time-millis`: range-checked milliseconds, scaled to microseconds.
    TimeMillis,
    /// `time-micros`: range-checked microseconds.
    TimeMicros,
    /// `timestamp-millis` and `local-timestamp-millis`: scaled to microseconds, the infinities kept.
    TimestampMillis,
    /// A `uuid` string, parsed from its text form.
    UuidText,
    /// A `duration` fixed(12): months, days and milliseconds.
    Duration,
}

/// An Avro schema node, together with the DuckDB type it is read as.
#[derive(Debug, Clone)]
pub struct AvroType {
    pub kind: AvroKind,
    pub type_id: LogicalTypeID,
    pub children: Vec<(String, AvroType)>,
    /// The DuckDB union member of each union branch, `None` for the NULL branches.
    pub union_members: Vec<Option<usize>>,
    pub field_id: Option<i32>,
    pub conversion: Conversion,
    pub decimal_width: u8,
    pub decimal_scale: u8,
    pub enum_symbols: Vec<String>,
    pub fixed_size: usize,
    /// The fewest bytes a value of this type is encoded in, which bounds how many items a block can claim to hold.
    pub min_size: usize,
}

impl AvroType {
    fn new(kind: AvroKind, type_id: LogicalTypeID) -> Self {
        AvroType {
            kind,
            type_id,
            children: Vec::new(),
            union_members: Vec::new(),
            field_id: None,
            conversion: Conversion::None,
            decimal_width: 0,
            decimal_scale: 0,
            enum_symbols: Vec::new(),
            fixed_size: 0,
            min_size: 0,
        }
    }

    /// The number of union branches that are not NULL.
    pub fn non_null_member_count(&self) -> usize {
        self.union_members.iter().flatten().count()
    }

    /// The branch of the only union member that is not NULL.
    pub fn single_member_branch(&self) -> Option<usize> {
        self.union_members.iter().position(Option::is_some)
    }
}

/// A column (or nested field) as DuckDB sees it. The children mirror the layout of the vector: the fields of a STRUCT,
/// the element of a LIST, the key/value STRUCT of a MAP and the members of a UNION.
#[derive(Debug, Clone)]
pub struct AvroColumn {
    pub name: String,
    pub logical_type: LogicalType,
    pub children: Vec<AvroColumn>,
    pub field_id: Option<i32>,
}

impl AvroColumn {
    fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        AvroColumn {
            name: name.into(),
            logical_type,
            children: Vec::new(),
            field_id: None,
        }
    }

    fn type_id(&self) -> Result<LogicalTypeID> {
        self.logical_type.id()
    }
}

const MAX_DECIMAL_PRECISION: i64 = 38;

enum Annotation {
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

fn logical_type_of(node: &Node) -> Option<(&str, Option<i64>, Option<i64>)> {
    match node {
        Node::Boolean {
            logical_type,
            precision,
            scale,
        }
        | Node::Int {
            logical_type,
            precision,
            scale,
        }
        | Node::Long {
            logical_type,
            precision,
            scale,
            ..
        }
        | Node::Float {
            logical_type,
            precision,
            scale,
        }
        | Node::Double {
            logical_type,
            precision,
            scale,
        }
        | Node::Bytes {
            logical_type,
            precision,
            scale,
        }
        | Node::String {
            logical_type,
            precision,
            scale,
        }
        | Node::Fixed {
            logical_type,
            precision,
            scale,
            ..
        } => Some((logical_type.as_deref()?, *precision, *scale)),
        _ => None,
    }
}

fn annotation(node: &Node) -> Option<Annotation> {
    // Unknown logical types and invalid decimals are ignored, as the specification asks: the underlying type is read
    let utc = matches!(
        node,
        Node::Long {
            adjust_to_utc: Some(true),
            ..
        }
    );
    let (logical_type, precision, scale) = logical_type_of(node)?;
    Some(match logical_type {
        "date" => Annotation::Date,
        "decimal" => {
            let precision = precision?;
            let scale = scale.unwrap_or(0);
            if !(1..=MAX_DECIMAL_PRECISION).contains(&precision)
                || !(0..=precision).contains(&scale)
            {
                return None;
            }
            Annotation::Decimal {
                precision: precision as u8,
                scale: scale as u8,
            }
        }
        "time-millis" => Annotation::TimeMillis,
        "time-micros" => Annotation::TimeMicros,
        "timestamp-millis" => Annotation::TimestampMillis { utc },
        "timestamp-micros" => Annotation::TimestampMicros { utc },
        "timestamp-nanos" => Annotation::TimestampNanos { utc },
        "local-timestamp-millis" => Annotation::LocalTimestampMillis,
        "local-timestamp-micros" => Annotation::LocalTimestampMicros,
        "local-timestamp-nanos" => Annotation::LocalTimestampNanos,
        "uuid" => Annotation::Uuid,
        "duration" => Annotation::Duration,
        _ => return None,
    })
}

fn unsupported_annotation(node: &Node, type_name: &str) -> duckdb_neo::Error {
    invalid_input(format!(
        "Avro logical type '{}' is not supported on Avro type '{type_name}'",
        logical_type_of(node).map_or("", |(logical_type, ..)| logical_type)
    ))
}

fn annotated(kind: AvroKind, type_id: LogicalTypeID, conversion: Conversion) -> AvroType {
    let mut result = AvroType::new(kind, type_id);
    result.conversion = conversion;
    result
}

fn decimal(kind: AvroKind, precision: u8, scale: u8) -> AvroType {
    let mut result = AvroType::new(kind, LogicalTypeID::Decimal);
    result.decimal_width = precision;
    result.decimal_scale = scale;
    result
}

fn primitive(node: &Node) -> Result<AvroType> {
    let annotation = annotation(node);
    let timestamp = |utc: bool| {
        if utc {
            LogicalTypeID::TimestampTz
        } else {
            LogicalTypeID::Timestamp
        }
    };
    Ok(match (node, annotation) {
        (Node::Boolean { .. }, None) => AvroType::new(AvroKind::Boolean, LogicalTypeID::Boolean),
        (Node::Boolean { .. }, Some(_)) => return Err(unsupported_annotation(node, "boolean")),
        (Node::Float { .. }, None) => AvroType::new(AvroKind::Float, LogicalTypeID::Float),
        (Node::Float { .. }, Some(_)) => return Err(unsupported_annotation(node, "float")),
        (Node::Double { .. }, None) => AvroType::new(AvroKind::Double, LogicalTypeID::Double),
        (Node::Double { .. }, Some(_)) => return Err(unsupported_annotation(node, "double")),
        (Node::Int { .. }, annotation) => match annotation {
            None => AvroType::new(AvroKind::Int, LogicalTypeID::Integer),
            Some(Annotation::Date) => AvroType::new(AvroKind::Int, LogicalTypeID::Date),
            Some(Annotation::TimeMillis) => {
                annotated(AvroKind::Int, LogicalTypeID::Time, Conversion::TimeMillis)
            }
            Some(_) => return Err(unsupported_annotation(node, "int")),
        },
        (Node::Long { .. }, annotation) => {
            let long = |type_id| AvroType::new(AvroKind::Long, type_id);
            let millis = |type_id| annotated(AvroKind::Long, type_id, Conversion::TimestampMillis);
            match annotation {
                None => long(LogicalTypeID::BigInt),
                Some(Annotation::TimeMicros) => {
                    annotated(AvroKind::Long, LogicalTypeID::Time, Conversion::TimeMicros)
                }
                Some(Annotation::TimestampMillis { utc }) => millis(timestamp(utc)),
                Some(Annotation::TimestampMicros { utc }) => long(timestamp(utc)),
                Some(Annotation::TimestampNanos { utc: true }) => {
                    long(LogicalTypeID::TimestampTzNs)
                }
                Some(Annotation::TimestampNanos { utc: false }) => long(LogicalTypeID::TimestampNs),
                Some(Annotation::LocalTimestampMillis) => millis(LogicalTypeID::Timestamp),
                Some(Annotation::LocalTimestampMicros) => long(LogicalTypeID::Timestamp),
                Some(Annotation::LocalTimestampNanos) => long(LogicalTypeID::TimestampNs),
                Some(_) => return Err(unsupported_annotation(node, "long")),
            }
        }
        (Node::String { .. }, annotation) => match annotation {
            None => AvroType::new(AvroKind::String, LogicalTypeID::Varchar),
            Some(Annotation::Uuid) => {
                annotated(AvroKind::String, LogicalTypeID::Uuid, Conversion::UuidText)
            }
            Some(_) => return Err(unsupported_annotation(node, "string")),
        },
        (Node::Bytes { .. }, annotation) => match annotation {
            None => AvroType::new(AvroKind::Bytes, LogicalTypeID::Blob),
            Some(Annotation::Decimal { precision, scale }) => {
                decimal(AvroKind::Bytes, precision, scale)
            }
            Some(_) => return Err(unsupported_annotation(node, "bytes")),
        },
        (Node::Fixed { size, .. }, annotation) => {
            let mut result = match annotation {
                None => AvroType::new(AvroKind::Fixed, LogicalTypeID::Blob),
                Some(Annotation::Decimal { precision, scale }) => {
                    decimal(AvroKind::Fixed, precision, scale)
                }
                Some(Annotation::Uuid) if *size == 16 => {
                    AvroType::new(AvroKind::Fixed, LogicalTypeID::Uuid)
                }
                Some(Annotation::Uuid) => {
                    return Err(invalid_configuration(
                        "logical type is uuid, but size != 16",
                    ));
                }
                Some(Annotation::Duration) if *size == 12 => annotated(
                    AvroKind::Fixed,
                    LogicalTypeID::Interval,
                    Conversion::Duration,
                ),
                Some(Annotation::Duration) => {
                    return Err(invalid_input(
                        "Avro logical type 'duration' requires a fixed of size 12",
                    ));
                }
                Some(_) => return Err(unsupported_annotation(node, "fixed")),
            };
            result.fixed_size =
                usize::try_from(*size).map_err(|_| invalid_input("Invalid Avro fixed size"))?;
            result
        }
        _ => return Err(internal("Avro schema node is not a primitive")),
    })
}

/// Converts the Avro schema node `id` into the type tree it is read with.
pub fn transform_schema(schema: &Schema, id: NodeId) -> Result<AvroType> {
    transform_node(schema, id, &mut TransformState::default())
}

// Named types can be referenced any number of times, so a small schema can expand into a huge or very deep tree.
const MAX_TYPE_DEPTH: usize = 128;
const MAX_TYPE_NODES: usize = 100_000;

#[derive(Default)]
struct TransformState {
    parent_records: Vec<NodeId>,
    depth: usize,
    nodes: usize,
}

fn transform_node(schema: &Schema, id: NodeId, state: &mut TransformState) -> Result<AvroType> {
    state.nodes += 1;
    if state.nodes > MAX_TYPE_NODES {
        return Err(invalid_input("Avro schema expands into too many types"));
    }
    if state.depth == MAX_TYPE_DEPTH {
        return Err(invalid_input("Avro schema is nested too deeply"));
    }
    state.depth += 1;
    let result = transform_kind(schema, id, state);
    state.depth -= 1;
    let mut result = result?;
    result.min_size = match result.kind {
        AvroKind::Null => 0,
        AvroKind::Float => 4,
        AvroKind::Double => 8,
        AvroKind::Fixed => result.fixed_size,
        AvroKind::Record => result
            .children
            .iter()
            .map(|(_, child)| child.min_size)
            .sum(),
        _ => 1,
    };
    Ok(result)
}

fn transform_kind(schema: &Schema, id: NodeId, state: &mut TransformState) -> Result<AvroType> {
    let node = schema.node(id);
    Ok(match node {
        Node::Null => AvroType::new(AvroKind::Null, LogicalTypeID::SqlNull),
        Node::Boolean { .. }
        | Node::Int { .. }
        | Node::Long { .. }
        | Node::Float { .. }
        | Node::Double { .. }
        | Node::Bytes { .. }
        | Node::String { .. }
        | Node::Fixed { .. } => primitive(node)?,
        Node::Union { branches } => {
            let mut result = AvroType::new(AvroKind::Union, LogicalTypeID::Union);
            let mut member_count = 0;
            for (index, branch) in branches.iter().enumerate() {
                let child = transform_node(schema, *branch, state)?;
                if child.type_id == LogicalTypeID::SqlNull {
                    result.union_members.push(None);
                } else {
                    result.union_members.push(Some(member_count));
                    member_count += 1;
                }
                result.children.push((format!("u{index}"), child));
            }
            result
        }
        Node::Record { name, fields, .. } => {
            if state.parent_records.contains(&id) {
                return Err(invalid_input(format!(
                    "Recursive Avro types not supported: {name}"
                )));
            }
            if fields.is_empty() {
                // Kept as a marker, so that the offsets of the fields around it stay right
                return Ok(AvroType::new(AvroKind::Record, LogicalTypeID::SqlNull));
            }
            state.parent_records.push(id);
            let mut result = AvroType::new(AvroKind::Record, LogicalTypeID::Struct);
            for field in fields {
                let mut child = transform_node(schema, field.schema, state)?;
                child.field_id = field.field_id;
                if field.name.is_empty() {
                    return Err(invalid_input("Empty avro field name"));
                }
                result.children.push((field.name.clone(), child));
            }
            state.parent_records.pop();
            result
        }
        Node::Enum { symbols, .. } => {
            let mut result = AvroType::new(AvroKind::Enum, LogicalTypeID::Enum);
            result.enum_symbols = symbols.clone();
            result
        }
        Node::Array {
            items,
            element_id,
            logical_type,
        } => {
            let mut child = transform_node(schema, *items, state)?;
            child.field_id = *element_id;
            let is_map = logical_type.as_deref() == Some("map");
            let type_id = if is_map {
                LogicalTypeID::Map
            } else {
                LogicalTypeID::List
            };
            let mut result = AvroType::new(AvroKind::Array, type_id);
            result.children.push(("list_entry".to_owned(), child));
            result
        }
        Node::Map {
            values,
            key_id,
            value_id,
        } => {
            let mut key = AvroType::new(AvroKind::String, LogicalTypeID::Varchar);
            key.field_id = *key_id;
            let mut value = transform_node(schema, *values, state)?;
            value.field_id = *value_id;
            let mut result = AvroType::new(AvroKind::Map, LogicalTypeID::Map);
            result.children.push(("key_entry".to_owned(), key));
            result.children.push(("value_entry".to_owned(), value));
            result
        }
        Node::Link { target } => transform_node(schema, *target, state)?,
    })
}

/// The SQLNULL type, which the type constructors do not create.
pub fn null_type(context: &Context) -> Result<LogicalType> {
    LogicalType::from_text(context, "\"NULL\"")
}

fn named_type(
    context: &Context,
    type_id: LogicalTypeID,
    children: &[AvroColumn],
) -> Result<LogicalType> {
    let params: Vec<(&str, &dyn QueryParameter)> = children
        .iter()
        .map(|child| {
            (
                child.name.as_str(),
                &child.logical_type as &dyn QueryParameter,
            )
        })
        .collect();
    LogicalType::create_from_id(context, type_id, Parameters::named(&params))
}

fn positional_type(
    context: &Context,
    type_id: LogicalTypeID,
    children: &[&LogicalType],
) -> Result<LogicalType> {
    let params: Vec<&dyn QueryParameter> = children
        .iter()
        .map(|child| *child as &dyn QueryParameter)
        .collect();
    LogicalType::create_from_id(context, type_id, Parameters::positional(&params))
}

fn primitive_type(context: &Context, avro_type: &AvroType) -> Result<LogicalType> {
    match avro_type.type_id {
        LogicalTypeID::SqlNull => null_type(context),
        LogicalTypeID::Decimal => {
            let width = i64::from(avro_type.decimal_width);
            let scale = i64::from(avro_type.decimal_scale);
            LogicalType::create_from_id(
                context,
                LogicalTypeID::Decimal,
                Parameters::positional(&[&width, &scale]),
            )
        }
        LogicalTypeID::Enum => {
            let symbols: Vec<&dyn QueryParameter> = avro_type
                .enum_symbols
                .iter()
                .map(|symbol| symbol as &dyn QueryParameter)
                .collect();
            LogicalType::create_from_id(
                context,
                LogicalTypeID::Enum,
                Parameters::positional(&symbols),
            )
        }
        type_id => LogicalType::create_from_id(context, type_id, Parameters::None),
    }
}

fn with_field_id(mut column: AvroColumn, avro_type: &AvroType) -> AvroColumn {
    if avro_type.field_id.is_some() {
        column.field_id = avro_type.field_id;
    }
    column
}

/// Converts an Avro type into the DuckDB column it is read into.
pub fn transform_avro_type(
    context: &Context,
    name: &str,
    avro_type: &AvroType,
) -> Result<AvroColumn> {
    match avro_type.type_id {
        LogicalTypeID::Struct => {
            let children = avro_type
                .children
                .iter()
                .map(|(child_name, child)| transform_avro_type(context, child_name, child))
                .collect::<Result<Vec<_>>>()?;
            let mut result =
                AvroColumn::new(name, named_type(context, LogicalTypeID::Struct, &children)?);
            result.children = children;
            Ok(with_field_id(result, avro_type))
        }
        LogicalTypeID::Map | LogicalTypeID::List => {
            let mut children = if avro_type.kind == AvroKind::Array {
                let element = transform_avro_type(context, "list", &avro_type.children[0].1)?;
                if avro_type.type_id == LogicalTypeID::List {
                    let list_type =
                        positional_type(context, LogicalTypeID::List, &[&element.logical_type])?;
                    let mut result = AvroColumn::new(name, list_type);
                    result.children.push(element);
                    return Ok(with_field_id(result, avro_type));
                }
                if element.type_id()? != LogicalTypeID::Struct || element.children.len() != 2 {
                    return Err(invalid_input(
                        "Avro array with logical type 'map' must contain key/value records",
                    ));
                }
                element.children
            } else {
                vec![
                    transform_avro_type(context, "key", &avro_type.children[0].1)?,
                    transform_avro_type(context, "value", &avro_type.children[1].1)?,
                ]
            };
            children[0].name = "key".to_owned();
            children[1].name = "value".to_owned();
            let map_type = positional_type(
                context,
                LogicalTypeID::Map,
                &[&children[0].logical_type, &children[1].logical_type],
            )?;
            let mut key_value = AvroColumn::new(
                "key_value",
                named_type(context, LogicalTypeID::Struct, &children)?,
            );
            key_value.children = children;
            let mut result = AvroColumn::new(name, map_type);
            result.children.push(key_value);
            Ok(with_field_id(result, avro_type))
        }
        LogicalTypeID::Union => {
            let mut children = avro_type
                .children
                .iter()
                .filter(|(_, child)| child.type_id != LogicalTypeID::SqlNull)
                .map(|(child_name, child)| transform_avro_type(context, child_name, child))
                .collect::<Result<Vec<_>>>()?;
            if children.len() == 1 {
                let mut result = children.pop().expect("one child");
                result.name = name.to_owned();
                return Ok(with_field_id(result, avro_type));
            }
            if children.is_empty() {
                if avro_type.children.is_empty() {
                    return Err(invalid_input("Empty union type"));
                }
                return Ok(with_field_id(
                    AvroColumn::new(name, null_type(context)?),
                    avro_type,
                ));
            }
            let mut result =
                AvroColumn::new(name, named_type(context, LogicalTypeID::Union, &children)?);
            result.children = children;
            Ok(with_field_id(result, avro_type))
        }
        _ => Ok(with_field_id(
            AvroColumn::new(name, primitive_type(context, avro_type)?),
            avro_type,
        )),
    }
}
