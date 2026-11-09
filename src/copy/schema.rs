//! The Avro schema of a `COPY ... TO (FORMAT avro)`: the DuckDB column types converted to an [`avro_core::Schema`] the
//! way the C++ extension's `JSONSchemaGenerator` builds its JSON (names, nullability, Iceberg field ids, the `map`
//! logical type over an array of key/value records, the sanitizing of field names).
//!
//! | DuckDB                                  | Avro                                                          |
//! |-----------------------------------------|---------------------------------------------------------------|
//! | VARCHAR / BLOB                          | `string` / `bytes`                                            |
//! | INTEGER / BIGINT                        | `int` / `long`                                                |
//! | FLOAT / DOUBLE / BOOLEAN                | `float` / `double` / `boolean`                                |
//! | "NULL"                                  | `null` (never wrapped in a union)                             |
//! | DATE                                    | `int` + `date`                                                |
//! | TIME                                    | `long` + `time-micros`                                        |
//! | TIMESTAMP                               | `long` + `timestamp-micros`                                   |
//! | TIMESTAMP_MS                            | `long` + `timestamp-millis` (the stored milliseconds unchanged) |
//! | TIMESTAMP_TZ                            | `long` + `timestamp-micros`, `adjust-to-utc`                  |
//! | TIMESTAMP_NS / TIMESTAMPTZ_NS           | `long` + `timestamp-nanos` (+ `adjust-to-utc`)                |
//! | UUID                                    | `fixed(16)` + `uuid`                                          |
//! | DECIMAL(P, S)                           | `fixed(n)` + `decimal`, n the smallest size holding P digits  |
//! | STRUCT                                  | `record` (the `__duckdb_empty_struct_marker` child skipped)   |
//! | LIST                                    | `array` (nullable items)                                      |
//! | MAP                                     | `array` + `"logicalType":"map"` of `record(key, value)`       |
//! | ENUM                                    | `enum` of the symbols in order (they must be Avro identifiers) |
//! | UNION                                   | `union` of the member types in member order (names dropped)   |
//!
//! Every field, element, key and value is nullable (`["null", T]`) unless FIELD_IDS says `__duckdb_nullable: false`.
//! A nullable UNION gets `null` as its first branch rather than a union around the union (Avro unions do not nest);
//! its members are the branches as they are, a member has no null slot of its own (a DuckDB union value is NULL as a
//! whole or has a valid member). Two members whose Avro types are the same unnamed type (two INTEGERs, two LISTs)
//! cannot be told apart in an Avro union and are refused, as is a UNION member or a `"NULL"` member.
//!
//! Named types (`record`, `fixed`) take the name of the field they belong to (`r<field id>` for a record with an
//! Iceberg field id, `k<key id>_v<value id>` for a map's entries), or `<avro type><n>` where there is no field (array
//! items). Avro requires those names to be distinct within a schema, which the field names of a nested query are not
//! (`{'value': {'x': 1}} AS s1, {'value': {'y': 2}} AS s2` has two records called `value`): a name already taken gets
//! the suffix `_2`, `_3`, ... in order of definition. Names that do not collide are unchanged.

use std::collections::{HashMap, HashSet};

use avro_core::{Field, Name, Schema, SchemaBuilder, SchemaId, SchemaKind, SchemaNode};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Error, Factory, LogicalType, Result, TypeId};

use super::field_ids::{ChildFieldIds, FieldId};
use super::types::{
    cannot_convert, decimal_width_scale, enum_symbols, list_child_type, map_key_value_types, struct_children,
    union_members,
};

/// The child a STRUCT without fields carries; it has no Avro field.
pub(crate) const EMPTY_STRUCT_MARKER: &str = "__duckdb_empty_struct_marker";

/// The Avro type name a DuckDB type maps to (also the base of a generated schema name).
fn convert_type_to_avro(type_: &LogicalType) -> Result<&'static str> {
    Ok(match type_.id()? {
        type_id::VARCHAR => "string",
        type_id::BLOB => "bytes",
        type_id::INTEGER => "int",
        type_id::BIGINT => "long",
        type_id::FLOAT => "float",
        type_id::DOUBLE => "double",
        type_id::BOOLEAN => "boolean",
        type_id::SQLNULL => "null",
        type_id::STRUCT => "record",
        type_id::DATE => "int",
        type_id::TIME => "long",
        type_id::TIMESTAMP | type_id::TIMESTAMP_NS | type_id::TIMESTAMP_MS => "long",
        type_id::TIMESTAMP_TZ | type_id::TIMESTAMP_TZ_NS => "long",
        type_id::UUID | type_id::DECIMAL => "fixed",
        type_id::LIST => "array",
        // a 'logicalType': map over an array of key/value records
        type_id::MAP => "array",
        type_id::ENUM => "enum",
        type_id::UNION => "union",
        _ => return Err(cannot_convert(type_)),
    })
}

fn is_temporal(id: TypeId) -> bool {
    matches!(
        id,
        type_id::DATE
            | type_id::TIME
            | type_id::TIMESTAMP
            | type_id::TIMESTAMP_MS
            | type_id::TIMESTAMP_NS
            | type_id::TIMESTAMP_TZ
            | type_id::TIMESTAMP_TZ_NS
    )
}

fn temporal_logical_type(type_: &LogicalType) -> Result<&'static str> {
    Ok(match type_.id()? {
        type_id::DATE => "date",
        type_id::TIME => "time-micros",
        type_id::TIMESTAMP | type_id::TIMESTAMP_TZ => "timestamp-micros",
        type_id::TIMESTAMP_MS => "timestamp-millis",
        type_id::TIMESTAMP_NS | type_id::TIMESTAMP_TZ_NS => "timestamp-nanos",
        _ => {
            return Err(Error::not_implemented(format!(
                "Can't convert logical type '{}' to Avro temporal type",
                type_.to_text()?
            )));
        }
    })
}

/// Bits needed for the largest value of each precision (before the sign bit): `ceil(P * log2(10))`.
const BITS_REQUIRED: [u32; 39] = [
    0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64, 67, 70, 74, 77, 80, 84, 87, 90, 94,
    97, 100, 103, 107, 110, 113, 117, 120, 123, 127,
];

/// The size of the `fixed` that holds every value of a DECIMAL of `precision` digits in two's complement.
pub(crate) fn min_bytes_required_for_decimal(precision: u8) -> usize {
    (BITS_REQUIRED[precision as usize] + 1).div_ceil(8) as usize
}

/// Whether the Avro type of a DuckDB type is a named one (`record`, `enum`, `fixed`).
fn is_named_schema(id: TypeId) -> bool {
    matches!(id, type_id::STRUCT | type_id::ENUM | type_id::UUID | type_id::DECIMAL)
}

/// avro-c's identifier rule: `[A-Za-z_][A-Za-z0-9_]*`.
fn is_avro_id(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().enumerate().all(|(i, c)| c.is_ascii_alphabetic() || c == b'_' || (i > 0 && c.is_ascii_digit()))
}

/// Whether the C++ `VerifyAvroName` accepts `name` (it accepts the empty name; avro-c's parse rejects it later).
fn verify_avro_name(name: &str) -> Result<()> {
    if name.is_empty() || is_avro_id(name) {
        return Ok(());
    }
    Err(Error::invalid_input(format!(
        "'{name}' is not a valid Avro identifier\nThe identifier has to match the regex: [A-Za-z_][A-Za-z0-9_]*"
    )))
}

/// The ids of a MAP's key and value, when FIELD_IDS names both.
struct MapKeyValueIds {
    key_id: i32,
    value_id: i32,
}

/// Builds the schema of the written columns.
pub(crate) struct SchemaGenerator<'a> {
    factory: Factory,
    names: &'a [String],
    types: &'a [LogicalType],
    pub(crate) sanitize_field_names: bool,
    pub(crate) root_name: String,
    pub(crate) field_ids: ChildFieldIds,
    generated_name_id: usize,
    named_schemas: HashSet<String>,
    builder: SchemaBuilder,
}

impl<'a> SchemaGenerator<'a> {
    pub(crate) fn new(factory: Factory, names: &'a [String], types: &'a [LogicalType]) -> Self {
        SchemaGenerator {
            factory,
            names,
            types,
            sanitize_field_names: false,
            root_name: "root".to_owned(),
            field_ids: ChildFieldIds::default(),
            generated_name_id: 0,
            named_schemas: HashSet::new(),
            builder: SchemaBuilder::new(),
        }
    }

    /// The schema: a record named `root_name` with one field per column.
    pub(crate) fn generate(&mut self) -> Result<Schema> {
        verify_avro_name(&self.root_name)?;
        // the root's name is taken first: a field of the same name gets the suffix
        self.named_schemas.insert(self.root_name.clone());
        let mut fields = Vec::with_capacity(self.names.len());
        let mut field_names = HashMap::new();
        // the ids are read while the builder is written: hold them outside `self` for the loop
        let field_ids = std::mem::take(&mut self.field_ids);
        let result = self.names.iter().zip(self.types).try_for_each(|(name, type_)| {
            let field = self.create_struct_field(name, type_, field_ids.find(name), &mut field_names)?;
            fields.push(field);
            Ok::<(), Error>(())
        });
        self.field_ids = field_ids;
        result?;
        let root = self.builder.add_kind(SchemaKind::Record {
            name: Name::simple(self.root_name.clone()),
            fields,
            is_error: false,
        });
        let builder = std::mem::take(&mut self.builder);
        let schema = builder.finish(root).map_err(|e| Error::invalid_input(e.message()))?;
        verify_identifiers(&schema)?;
        Ok(schema)
    }

    /// `name`, or `name_2`, `name_3`, ... when a named schema took it already (the field names of a nested query need
    /// not be distinct, Avro's type names must be).
    fn unique_schema_name(&mut self, name: &str) -> String {
        let mut candidate = name.to_owned();
        let mut suffix = 2;
        while self.named_schemas.contains(&candidate) {
            candidate = format!("{name}_{suffix}");
            suffix += 1;
        }
        self.named_schemas.insert(candidate.clone());
        candidate
    }

    fn generate_schema_name(&mut self, base: &str) -> Result<String> {
        let name = format!("{base}{}", self.generated_name_id);
        self.generated_name_id += 1;
        verify_avro_name(&name)?;
        Ok(self.unique_schema_name(&name))
    }

    /// The name of a named schema: the preset one, or a generated `<avro type><n>`; made unique either way.
    fn schema_name(&mut self, avro_type: &str, preset: Option<&str>) -> Result<Name> {
        match preset {
            Some(preset) => {
                verify_avro_name(preset)?;
                Ok(Name::simple(self.unique_schema_name(preset)))
            }
            None => Ok(Name::simple(self.generate_schema_name(avro_type)?)),
        }
    }

    /// Wraps a type in a union with null; `null` itself stays as it is (a union cannot repeat a type) and a union
    /// gets `null` as its first branch (a union cannot nest).
    fn make_nullable(&mut self, type_: SchemaId) -> SchemaId {
        let branches = match &self.builder.node(type_).kind {
            SchemaKind::Null => return type_,
            SchemaKind::Union { branches } => branches.clone(),
            _ => vec![type_],
        };
        let null = self.builder.add_kind(SchemaKind::Null);
        self.builder.add_kind(SchemaKind::Union { branches: std::iter::once(null).chain(branches).collect() })
    }

    fn create_json_type(
        &mut self,
        type_: &LogicalType,
        field_id: Option<&FieldId>,
        preset_schema_name: Option<&str>,
    ) -> Result<SchemaId> {
        let avro_type = convert_type_to_avro(type_)?;
        let type_id = type_.id()?;

        if type_id == type_id::STRUCT {
            let mut fields = Vec::new();
            let mut field_names = HashMap::new();
            for (child_name, child_type) in struct_children(type_)? {
                if child_name == EMPTY_STRUCT_MARKER {
                    continue;
                }
                let child_field_id = field_id.and_then(|f| f.children.find(&child_name));
                fields.push(self.create_struct_field(&child_name, &child_type, child_field_id, &mut field_names)?);
            }
            // the record's own name comes after its children, as the C++ generates them
            let name = self.schema_name(avro_type, preset_schema_name)?;
            return Ok(self.builder.add_kind(SchemaKind::Record { name, fields, is_error: false }));
        }
        if type_id == type_id::LIST {
            let list_child = list_child_type(type_)?;
            let element_field_id = field_id.and_then(|f| f.children.find("list"));
            let mut items = match element_field_id {
                Some(element) if list_child.id()? == type_id::STRUCT => {
                    let element_schema_name = format!("r{}", element.field_id());
                    self.create_json_type(&list_child, element_field_id, Some(&element_schema_name))?
                }
                _ => self.create_json_type(&list_child, element_field_id, None)?,
            };
            if element_field_id.is_none_or(|element| element.nullable) {
                items = self.make_nullable(items);
            }
            let element_id = element_field_id.map(|element| i64::from(element.field_id()));
            return Ok(self.builder.add_kind(SchemaKind::Array { items, element_id }));
        }
        if type_id == type_id::MAP {
            let (key_type, value_type) = map_key_value_types(type_)?;
            let entries_type = LogicalType::struct_(self.factory, &[("key", &key_type), ("value", &value_type)])?;
            let items = match map_key_value_ids(field_id)? {
                None => self.create_json_type(&entries_type, field_id, None)?,
                Some(ids) => {
                    let map_schema_name = format!("k{}_v{}", ids.key_id, ids.value_id);
                    self.create_json_type(&entries_type, field_id, Some(&map_schema_name))?
                }
            };
            let node = SchemaNode::new(SchemaKind::Array { items, element_id: None }).with_logical_type("map");
            return Ok(self.builder.add(node));
        }

        if type_id == type_id::ENUM {
            let symbols = enum_symbols(type_)?;
            for symbol in &symbols {
                if !is_avro_id(symbol) {
                    return Err(Error::invalid_input(format!(
                        "'{symbol}' is not a valid Avro enum symbol\nThe symbol has to match the regex: \
                         [A-Za-z_][A-Za-z0-9_]*"
                    )));
                }
            }
            let name = self.schema_name(avro_type, preset_schema_name)?;
            return Ok(self.builder.add_kind(SchemaKind::Enum { name, symbols, default: None }));
        }
        if type_id == type_id::UNION {
            let members = union_members(type_)?;
            let mut branches = Vec::with_capacity(members.len());
            // an Avro union tells its unnamed branches apart by type: no two of the same type
            let mut unnamed: HashMap<&'static str, &str> = HashMap::new();
            for (member_name, member_type) in &members {
                let member_id = member_type.id()?;
                if member_id == type_id::UNION || member_id == type_id::SQLNULL {
                    return Err(Error::not_implemented(format!(
                        "Can't convert UNION member '{member_name}' of type {} to an Avro union branch",
                        member_type.to_text()?
                    )));
                }
                let member_field_id = field_id.and_then(|f| f.children.find(member_name));
                let branch = self.create_json_type(member_type, member_field_id, None)?;
                let node = self.builder.node(branch);
                if !node.kind.is_definition()
                    && let Some(previous) = unnamed.insert(node.kind.type_name(), member_name)
                {
                    return Err(Error::invalid_input(format!(
                        "Can't convert UNION members '{previous}' and '{member_name}' to an Avro union: both are an \
                         Avro '{}', and a union holds every unnamed type once",
                        node.kind.type_name()
                    )));
                }
                branches.push(branch);
            }
            return Ok(self.builder.add_kind(SchemaKind::Union { branches }));
        }

        let primitive = match avro_type {
            "string" => SchemaKind::String,
            "bytes" => SchemaKind::Bytes,
            "int" => SchemaKind::Int,
            "long" => SchemaKind::Long,
            "float" => SchemaKind::Float,
            "double" => SchemaKind::Double,
            "boolean" => SchemaKind::Boolean,
            "null" => SchemaKind::Null,
            _ => SchemaKind::Null, // "fixed": replaced below
        };
        let node = if is_temporal(type_id) {
            let mut node = SchemaNode::new(primitive).with_logical_type(temporal_logical_type(type_)?);
            if type_id == type_id::TIMESTAMP_TZ || type_id == type_id::TIMESTAMP_TZ_NS {
                node = node.with_adjust_to_utc(true);
            }
            node
        } else if type_id == type_id::DECIMAL {
            let (width, scale) = decimal_width_scale(type_)?;
            let name = self.schema_name(avro_type, preset_schema_name)?;
            SchemaNode::new(SchemaKind::Fixed { name, size: min_bytes_required_for_decimal(width) })
                .with_decimal(i64::from(width), i64::from(scale))
        } else if type_id == type_id::UUID {
            let name = self.schema_name(avro_type, preset_schema_name)?;
            SchemaNode::new(SchemaKind::Fixed { name, size: 16 }).with_logical_type("uuid")
        } else {
            if is_named_schema(type_id) {
                // unreachable for the types `convert_type_to_avro` accepts; kept for parity with the C++
                self.schema_name(avro_type, preset_schema_name)?;
            }
            SchemaNode::new(primitive)
        };
        Ok(self.builder.add(node))
    }

    /// Iceberg's `AvroSchemaUtil` escaping (for ASCII; non-ASCII bytes are escaped the same way, the Avro identifier
    /// grammar only allows ASCII letters and digits).
    fn sanitize_field_name(&self, name: &str, field_names: &mut HashMap<String, String>) -> Result<String> {
        if !self.sanitize_field_names {
            return Ok(name.to_owned());
        }
        if name.is_empty() {
            return Err(Error::invalid_input("Cannot sanitize an empty Avro field name"));
        }
        let mut result = String::with_capacity(name.len());
        for (i, c) in name.bytes().enumerate() {
            let digit = c.is_ascii_digit();
            if c.is_ascii_alphabetic() || c == b'_' || (i > 0 && digit) {
                result.push(c as char);
            } else if digit {
                result.push('_');
                result.push(c as char);
            } else {
                result.push_str(&format!("_x{c:X}"));
            }
        }
        if let Some(previous) = field_names.get(&result) {
            return Err(Error::binder(format!(
                "SANITIZE_FIELD_NAMES maps both '{previous}' and '{name}' to '{result}' in the same Avro record"
            )));
        }
        field_names.insert(result.clone(), name.to_owned());
        Ok(result)
    }

    fn create_struct_field(
        &mut self,
        name: &str,
        type_: &LogicalType,
        field_id: Option<&FieldId>,
        field_names: &mut HashMap<String, String>,
    ) -> Result<Field> {
        let output_name = self.sanitize_field_name(name, field_names)?;
        let schema_name = match field_id {
            Some(id) if type_.id()? == type_id::STRUCT => format!("r{}", id.field_id()),
            _ => output_name.clone(),
        };
        let mut field_type = self.create_json_type(type_, field_id, Some(&schema_name))?;
        if field_id.is_none_or(|id| id.nullable) {
            field_type = self.make_nullable(field_type);
        }
        let mut field = Field::new(output_name, field_type);
        if let Some(id) = field_id {
            field.field_id = Some(i64::from(id.field_id()));
        }
        Ok(field)
    }
}

fn map_key_value_ids(field_id: Option<&FieldId>) -> Result<Option<MapKeyValueIds>> {
    let Some(field_id) = field_id else {
        return Ok(None);
    };
    match (field_id.children.find("key"), field_id.children.find("value")) {
        (Some(key), Some(value)) => Ok(Some(MapKeyValueIds { key_id: key.field_id(), value_id: value.field_id() })),
        _ => Ok(None),
    }
}

/// What avro-c's schema parser checks when the C++ hands it the JSON: every field name and every name of a named
/// type is an Avro identifier.
fn verify_identifiers(schema: &Schema) -> Result<()> {
    for node in schema.nodes() {
        match &node.kind {
            SchemaKind::Record { name, fields, .. } => {
                if !is_avro_id(&name.name) || fields.iter().any(|field| !is_avro_id(&field.name)) {
                    return Err(Error::invalid_input("Invalid Avro identifier"));
                }
            }
            SchemaKind::Enum { name, .. } | SchemaKind::Fixed { name, .. } if !is_avro_id(&name.name) => {
                return Err(Error::invalid_input("Invalid Avro identifier"));
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_fixed_sizes() {
        // the table of test/sql/copy/decimal_fixed_size.test
        let expected = [
            (1, 1),
            (2, 1),
            (3, 2),
            (4, 2),
            (5, 3),
            (6, 3),
            (7, 4),
            (8, 4),
            (9, 4),
            (10, 5),
            (11, 5),
            (12, 6),
            (13, 6),
            (14, 6),
            (15, 7),
            (16, 7),
            (17, 8),
            (18, 8),
            (19, 9),
            (20, 9),
            (21, 9),
            (22, 10),
            (23, 10),
            (24, 11),
            (25, 11),
            (26, 11),
            (27, 12),
            (28, 12),
            (29, 13),
            (30, 13),
            (31, 13),
            (32, 14),
            (33, 14),
            (34, 15),
            (35, 15),
            (36, 16),
            (37, 16),
            (38, 16),
        ];
        for (p, bytes) in expected {
            assert_eq!(min_bytes_required_for_decimal(p), bytes, "precision {p}");
        }
    }

    #[test]
    fn identifiers() {
        assert!(is_avro_id("a_1"));
        assert!(is_avro_id("_"));
        assert!(!is_avro_id("1a"));
        assert!(!is_avro_id("a b"));
        assert!(!is_avro_id(""));
        assert!(!is_avro_id("é"));
        assert!(verify_avro_name("").is_ok());
        assert!(verify_avro_name("a b").is_err());
    }
}
