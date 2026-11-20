//! The Avro schema JSON of the file a `COPY ... TO (FORMAT AVRO)` writes, generated from the column types.

use std::collections::{HashMap, HashSet};

use duckdb_neo::{
    Result,
    logical_type::{LogicalType, LogicalTypeID},
    types::DecimalSignature,
};
use serde_json::{Map, Value as Json, json};

use crate::error::{binder, internal, invalid_input, not_implemented};
use crate::field_ids::{ChildFieldIds, FieldId};

/// The child DuckDB adds to a STRUCT without fields; it has no Avro counterpart.
pub const EMPTY_STRUCT_MARKER: &str = "__duckdb_empty_struct_marker";

// Bits needed for the largest unscaled value of each precision, before the sign bit.
const DECIMAL_BITS_REQUIRED: [u32; 39] = [
    0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64, 67, 70, 74, 77,
    80, 84, 87, 90, 94, 97, 100, 103, 107, 110, 113, 117, 120, 123, 127,
];

/// The size of the Avro fixed that holds every value of a DECIMAL of `width` digits, sign included.
pub fn decimal_fixed_size(width: u8) -> Result<usize> {
    DECIMAL_BITS_REQUIRED
        .get(usize::from(width))
        .map(|bits| (bits + 1).div_ceil(8) as usize)
        .ok_or_else(|| internal(format!("Unsupported DECIMAL width {width}")))
}

/// The type of the `index`th parameter of a LIST, MAP or STRUCT type.
pub fn type_param(logical_type: &LogicalType, index: usize) -> Result<LogicalType> {
    logical_type
        .get_param(index)?
        .1
        .get::<LogicalType>()?
        .ok_or_else(|| internal("Type parameter is not a type"))
}

/// The field names and types of a STRUCT type, or the member names and types of a UNION type.
pub fn struct_children(logical_type: &LogicalType) -> Result<Vec<(String, LogicalType)>> {
    let mut children = Vec::new();
    for (name, value) in logical_type.get_params()? {
        let child_type = value
            .get::<LogicalType>()?
            .ok_or_else(|| internal("Type parameter is not a type"))?;
        children.push((name, child_type));
    }
    Ok(children)
}

/// The symbols of an ENUM type, in dictionary order.
pub fn enum_symbols(logical_type: &LogicalType) -> Result<Vec<String>> {
    let mut symbols = Vec::new();
    for (_, value) in logical_type.get_params()? {
        symbols.push(
            value
                .get::<String>()?
                .ok_or_else(|| internal("ENUM symbol is not a string"))?,
        );
    }
    Ok(symbols)
}

fn avro_type_name(logical_type: &LogicalType) -> Result<&'static str> {
    Ok(match logical_type.id()? {
        LogicalTypeID::Varchar => "string",
        LogicalTypeID::Blob => "bytes",
        LogicalTypeID::Integer | LogicalTypeID::Date => "int",
        LogicalTypeID::BigInt
        | LogicalTypeID::Time
        | LogicalTypeID::Timestamp
        | LogicalTypeID::TimestampNs
        | LogicalTypeID::TimestampMs
        | LogicalTypeID::TimestampTz
        | LogicalTypeID::TimestampTzNs => "long",
        LogicalTypeID::Float => "float",
        LogicalTypeID::Double => "double",
        LogicalTypeID::Boolean => "boolean",
        LogicalTypeID::SqlNull => "null",
        LogicalTypeID::Struct => "record",
        LogicalTypeID::Uuid | LogicalTypeID::Decimal => "fixed",
        LogicalTypeID::List | LogicalTypeID::Map => "array",
        LogicalTypeID::Enum => "enum",
        LogicalTypeID::Union => "union",
        _ => {
            return Err(not_implemented(format!(
                "Can't convert logical type '{}' to Avro type",
                logical_type.to_text()?
            )));
        }
    })
}

fn temporal_logical_type(id: LogicalTypeID) -> Option<&'static str> {
    match id {
        LogicalTypeID::Date => Some("date"),
        LogicalTypeID::Time => Some("time-micros"),
        LogicalTypeID::Timestamp | LogicalTypeID::TimestampTz => Some("timestamp-micros"),
        LogicalTypeID::TimestampMs => Some("timestamp-millis"),
        LogicalTypeID::TimestampNs | LogicalTypeID::TimestampTzNs => Some("timestamp-nanos"),
        _ => None,
    }
}

fn is_named(id: LogicalTypeID) -> bool {
    matches!(
        id,
        LogicalTypeID::Struct | LogicalTypeID::Enum | LogicalTypeID::Uuid | LogicalTypeID::Decimal
    )
}

fn is_avro_identifier(name: &str) -> bool {
    name.bytes()
        .enumerate()
        .all(|(index, c)| c.is_ascii_alphabetic() || c == b'_' || (index > 0 && c.is_ascii_digit()))
}

fn verify_avro_name(name: &str) -> Result<()> {
    if !is_avro_identifier(name) {
        return Err(invalid_input(format!(
            "'{name}' is not a valid Avro identifier\nThe identifier has to match the regex: [A-Za-z_][A-Za-z0-9_]*"
        )));
    }
    Ok(())
}

fn verify_enum_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty() || !is_avro_identifier(symbol) {
        return Err(invalid_input(format!(
            "'{symbol}' is not a valid Avro enum symbol\nThe symbol has to match the regex: [A-Za-z_][A-Za-z0-9_]*"
        )));
    }
    Ok(())
}

fn make_nullable(avro_type: Json) -> Json {
    match avro_type {
        // A union cannot hold null twice, nor nest in another union.
        Json::String(name) if name == "null" => Json::String(name),
        Json::Array(mut branches) => {
            branches.insert(0, Json::from("null"));
            Json::Array(branches)
        }
        avro_type => json!(["null", avro_type]),
    }
}

fn child_field_id<'a>(parent: Option<&'a FieldId>, name: &str) -> Option<&'a FieldId> {
    parent.and_then(|parent| parent.children.find(name))
}

/// Generates the schema JSON of the columns `names` with types `types`.
pub struct SchemaGenerator<'a> {
    names: &'a [String],
    types: &'a [LogicalType],
    pub sanitize_field_names: bool,
    pub root_name: String,
    pub field_ids: ChildFieldIds,
    generated_name_id: usize,
    named_schemas: HashSet<String>,
}

impl<'a> SchemaGenerator<'a> {
    /// A generator for the record `root` with the given columns.
    pub fn new(names: &'a [String], types: &'a [LogicalType]) -> Self {
        Self {
            names,
            types,
            sanitize_field_names: false,
            root_name: "root".to_owned(),
            field_ids: ChildFieldIds::default(),
            generated_name_id: 0,
            named_schemas: HashSet::new(),
        }
    }

    /// The schema JSON; it still has to be validated by parsing it.
    pub fn generate(mut self) -> Result<String> {
        let root_name = std::mem::take(&mut self.root_name);
        verify_avro_name(&root_name)?;
        self.named_schemas.insert(root_name.clone());
        let field_ids = std::mem::take(&mut self.field_ids);
        let mut field_names = HashMap::new();
        let mut fields = Vec::with_capacity(self.names.len());
        for (name, logical_type) in self.names.iter().zip(self.types) {
            fields.push(self.struct_field(
                name,
                logical_type,
                field_ids.find(name),
                &mut field_names,
            )?);
        }
        let root = json!({"type": "record", "name": root_name, "fields": fields});
        Ok(root.to_string())
    }

    fn unique_name(&mut self, name: &str) -> String {
        let mut candidate = name.to_owned();
        let mut suffix = 2;
        while self.named_schemas.contains(&candidate) {
            candidate = format!("{name}_{suffix}");
            suffix += 1;
        }
        self.named_schemas.insert(candidate.clone());
        candidate
    }

    fn generate_name(&mut self, base: &str) -> Result<String> {
        let name = format!("{base}{}", self.generated_name_id);
        self.generated_name_id += 1;
        verify_avro_name(&name)?;
        Ok(self.unique_name(&name))
    }

    fn set_name(
        &mut self,
        object: &mut Map<String, Json>,
        base: &str,
        preset: Option<&str>,
    ) -> Result<()> {
        let name = match preset {
            Some(preset) => {
                verify_avro_name(preset)?;
                self.unique_name(preset)
            }
            None => self.generate_name(base)?,
        };
        object.insert("name".to_owned(), Json::from(name));
        Ok(())
    }

    fn record(
        &mut self,
        children: &[(String, LogicalType)],
        field_id: Option<&FieldId>,
        preset_name: Option<&str>,
    ) -> Result<Json> {
        let mut field_names = HashMap::new();
        let mut fields = Vec::with_capacity(children.len());
        for (name, child_type) in children {
            if name == EMPTY_STRUCT_MARKER {
                continue;
            }
            fields.push(self.struct_field(
                name,
                child_type,
                child_field_id(field_id, name),
                &mut field_names,
            )?);
        }
        let mut object = Map::new();
        object.insert("type".to_owned(), Json::from("record"));
        object.insert("fields".to_owned(), Json::from(fields));
        self.set_name(&mut object, "record", preset_name)?;
        Ok(Json::Object(object))
    }

    fn union(&mut self, logical_type: &LogicalType, field_id: Option<&FieldId>) -> Result<Json> {
        let members = struct_children(logical_type)?;
        let mut branches = Vec::with_capacity(members.len());
        // Avro tells the branches of a union apart by type, and named types by name.
        let mut unnamed_members: HashMap<&str, &str> = HashMap::new();
        for (member_name, member_type) in &members {
            if matches!(
                member_type.id()?,
                LogicalTypeID::Union | LogicalTypeID::SqlNull
            ) {
                return Err(not_implemented(format!(
                    "Can't convert UNION member '{member_name}' of type {} to an Avro union branch",
                    member_type.to_text()?
                )));
            }
            branches.push(self.json_type(
                member_type,
                child_field_id(field_id, member_name),
                None,
            )?);
            let avro_type = avro_type_name(member_type)?;
            if !is_named(member_type.id()?)
                && let Some(previous) = unnamed_members.insert(avro_type, member_name)
            {
                return Err(invalid_input(format!(
                    "Can't convert UNION members '{previous}' and '{member_name}' to an Avro union: both are an Avro '{avro_type}', and a union holds every unnamed type once"
                )));
            }
        }
        Ok(Json::Array(branches))
    }

    fn json_type(
        &mut self,
        logical_type: &LogicalType,
        field_id: Option<&FieldId>,
        preset_name: Option<&str>,
    ) -> Result<Json> {
        let avro_type = avro_type_name(logical_type)?;
        let id = logical_type.id()?;
        let mut object = Map::new();
        object.insert("type".to_owned(), Json::from(avro_type));
        match id {
            LogicalTypeID::Struct => {
                return self.record(&struct_children(logical_type)?, field_id, preset_name);
            }
            LogicalTypeID::Union => return self.union(logical_type, field_id),
            LogicalTypeID::Enum => {
                let symbols = enum_symbols(logical_type)?;
                for symbol in &symbols {
                    verify_enum_symbol(symbol)?;
                }
                object.insert("symbols".to_owned(), Json::from(symbols));
            }
            LogicalTypeID::List => {
                let child_type = type_param(logical_type, 0)?;
                let element_field_id = child_field_id(field_id, "list");
                let element_name = match element_field_id {
                    Some(element) if child_type.id()? == LogicalTypeID::Struct => {
                        Some(format!("r{}", element.field_id()?))
                    }
                    _ => None,
                };
                let mut items =
                    self.json_type(&child_type, element_field_id, element_name.as_deref())?;
                if element_field_id.is_none_or(|element| element.nullable) {
                    items = make_nullable(items);
                }
                object.insert("items".to_owned(), items);
                if let Some(element) = element_field_id {
                    object.insert("element-id".to_owned(), Json::from(element.field_id()?));
                }
            }
            LogicalTypeID::Map => {
                object.insert("logicalType".to_owned(), Json::from("map"));
                let entries = [
                    ("key".to_owned(), type_param(logical_type, 0)?),
                    ("value".to_owned(), type_param(logical_type, 1)?),
                ];
                let entries_name = match (
                    child_field_id(field_id, "key"),
                    child_field_id(field_id, "value"),
                ) {
                    (Some(key), Some(value)) => {
                        Some(format!("k{}_v{}", key.field_id()?, value.field_id()?))
                    }
                    _ => None,
                };
                let items = self.record(&entries, field_id, entries_name.as_deref())?;
                object.insert("items".to_owned(), items);
            }
            LogicalTypeID::Decimal => {
                let signature = DecimalSignature::from_logical_type(logical_type)?;
                object.insert("logicalType".to_owned(), Json::from("decimal"));
                object.insert("scale".to_owned(), Json::from(signature.scale));
                object.insert("precision".to_owned(), Json::from(signature.width));
                object.insert(
                    "size".to_owned(),
                    Json::from(decimal_fixed_size(signature.width)?),
                );
            }
            LogicalTypeID::Uuid => {
                object.insert("logicalType".to_owned(), Json::from("uuid"));
                object.insert("size".to_owned(), Json::from(16));
            }
            _ => match temporal_logical_type(id) {
                Some(temporal) => {
                    object.insert("logicalType".to_owned(), Json::from(temporal));
                    if matches!(
                        id,
                        LogicalTypeID::TimestampTz | LogicalTypeID::TimestampTzNs
                    ) {
                        object.insert("adjust-to-utc".to_owned(), Json::from(true));
                    }
                }
                None => return Ok(Json::from(avro_type)),
            },
        }
        if is_named(id) {
            self.set_name(&mut object, avro_type, preset_name)?;
        }
        Ok(Json::Object(object))
    }

    fn sanitize_field_name(
        &self,
        name: &str,
        field_names: &mut HashMap<String, String>,
    ) -> Result<String> {
        if !self.sanitize_field_names {
            return Ok(name.to_owned());
        }
        if name.is_empty() {
            return Err(invalid_input("Cannot sanitize an empty Avro field name"));
        }
        // Iceberg's escaping for ASCII; UTF-8 bytes are escaped too, as Avro identifiers are ASCII only.
        let mut result = String::with_capacity(name.len());
        for (index, c) in name.bytes().enumerate() {
            if c.is_ascii_alphabetic() || c == b'_' || (index > 0 && c.is_ascii_digit()) {
                result.push(char::from(c));
            } else if c.is_ascii_digit() {
                result.push('_');
                result.push(char::from(c));
            } else {
                result.push_str(&format!("_x{c:X}"));
            }
        }
        if let Some(existing) = field_names.get(&result) {
            return Err(binder(format!(
                "SANITIZE_FIELD_NAMES maps both '{existing}' and '{name}' to '{result}' in the same Avro record"
            )));
        }
        field_names.insert(result.clone(), name.to_owned());
        Ok(result)
    }

    fn struct_field(
        &mut self,
        name: &str,
        logical_type: &LogicalType,
        field_id: Option<&FieldId>,
        field_names: &mut HashMap<String, String>,
    ) -> Result<Json> {
        let output_name = self.sanitize_field_name(name, field_names)?;
        let schema_name = match field_id {
            Some(field_id) if logical_type.id()? == LogicalTypeID::Struct => {
                format!("r{}", field_id.field_id()?)
            }
            _ => output_name.clone(),
        };
        let mut field_type = self.json_type(logical_type, field_id, Some(&schema_name))?;
        if field_id.is_none_or(|field_id| field_id.nullable) {
            field_type = make_nullable(field_type);
        }
        let mut field = Map::new();
        field.insert("type".to_owned(), field_type);
        if let Some(field_id) = field_id {
            field.insert("field-id".to_owned(), Json::from(field_id.field_id()?));
        }
        field.insert("name".to_owned(), Json::from(output_name));
        Ok(Json::Object(field))
    }
}
