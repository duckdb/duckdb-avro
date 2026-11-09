//! The `FIELD_IDS` option of `COPY ... TO (FORMAT avro)`: Iceberg field ids for the columns and their nested children
//! (a port of the C++ extension's `field_ids.cpp`, itself derived from the parquet extension's).
//!
//! `FIELD_IDS {col: 42, nested: {__duckdb_field_id: 43, __duckdb_nullable: false, child: 44}}`: a bare integer is the
//! id of a column, a STRUCT carries the column's own id under `__duckdb_field_id`, its nullability under
//! `__duckdb_nullable` and the ids of its children under their names. The children of a LIST are keyed `list`, those
//! of a MAP `key` and `value`. Names are matched case-insensitively.

use std::collections::{HashMap, HashSet};

use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Error, Factory, LogicalType, Result, Value};

use super::types::{list_child_type, map_key_value_types, struct_children};

/// The key of a column's own id inside a nested specification.
pub(crate) const DUCKDB_FIELD_ID: &str = "__duckdb_field_id";
/// The key of a column's nullability inside a nested specification.
pub(crate) const DUCKDB_NULLABLE_ID: &str = "__duckdb_nullable";

/// The ids of the children of a column (or of the top level), looked up by lowercase name.
#[derive(Debug, Default)]
pub(crate) struct ChildFieldIds {
    ids: Option<HashMap<String, FieldId>>,
}

impl ChildFieldIds {
    /// The id of the child `name`, matched case-insensitively.
    pub(crate) fn find(&self, name: &str) -> Option<&FieldId> {
        self.ids.as_ref()?.get(&name.to_ascii_lowercase())
    }

    fn ids(&mut self) -> &mut HashMap<String, FieldId> {
        self.ids.get_or_insert_with(HashMap::new)
    }
}

/// The id of one field: `set` is false for a nested specification that names children but no id of its own.
#[derive(Debug)]
pub(crate) struct FieldId {
    set: bool,
    field_id: i32,
    /// Whether the field's Avro type is wrapped in a union with `null` (the default).
    pub(crate) nullable: bool,
    pub(crate) children: ChildFieldIds,
}

impl FieldId {
    fn unset() -> FieldId {
        FieldId { set: false, field_id: 0, nullable: true, children: ChildFieldIds::default() }
    }

    fn new(field_id: i32, nullable: bool) -> FieldId {
        FieldId { set: true, field_id, nullable, children: ChildFieldIds::default() }
    }

    /// The id; an error for a specification without one, as the C++ reports it.
    pub(crate) fn field_id(&self) -> Result<i32> {
        if !self.set {
            return Err(Error::internal("Field id of the field is not set"));
        }
        Ok(self.field_id)
    }
}

/// The types of the children of a column, by lowercase name, in declaration order.
type NameToTypeMap = Vec<(String, LogicalType)>;

fn find_child_type<'a>(map: &'a NameToTypeMap, name: &str) -> Option<&'a LogicalType> {
    map.iter().find(|(n, _)| n == name).map(|(_, t)| t)
}

fn child_name_to_type_map(type_: &LogicalType) -> Result<NameToTypeMap> {
    let mut map = NameToTypeMap::new();
    match type_.id()? {
        type_id::LIST => map.push(("list".to_owned(), list_child_type(type_)?)),
        type_id::MAP => {
            let (key, value) = map_key_value_types(type_)?;
            map.push(("key".to_owned(), key));
            map.push(("value".to_owned(), value));
        }
        type_id::STRUCT => {
            for (child_name, child_type) in struct_children(type_)? {
                if child_name == DUCKDB_FIELD_ID {
                    return Err(Error::binder(format!(
                        "Cannot have column named \"{DUCKDB_FIELD_ID}\" with FIELD_IDS"
                    )));
                }
                map.push((child_name.to_ascii_lowercase(), child_type));
            }
        }
        _ => return Err(Error::internal("Unexpected type in GetChildNameToTypeMap")),
    }
    Ok(map)
}

fn get_field_ids(
    factory: Factory,
    field_ids_value: &Value,
    field_ids: &mut ChildFieldIds,
    unique_field_ids: &mut HashSet<u32>,
    name_to_type_map: &NameToTypeMap,
) -> Result<()> {
    let struct_type = field_ids_value.logical_type()?;
    if struct_type.id()? != type_id::STRUCT {
        return Err(Error::binder(format!(
            "Expected FIELD_IDS to be a STRUCT, e.g., {{col1: 42, col2: {{{DUCKDB_FIELD_ID}: 43, nested_col: 44}}, col3: 44}}"
        )));
    }
    for (i, (child_name, _)) in struct_children(&struct_type)?.into_iter().enumerate() {
        let col_name = child_name.to_ascii_lowercase();
        if col_name == DUCKDB_FIELD_ID || col_name == DUCKDB_NULLABLE_ID {
            continue;
        }
        let Some(col_type) = find_child_type(name_to_type_map, &col_name) else {
            let names: Vec<&str> = name_to_type_map.iter().map(|(n, _)| n.as_str()).collect();
            return Err(Error::binder(format!(
                "Column name \"{col_name}\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS \
                 if this column is a partition column. Available column names: [{}]",
                names.join(", ")
            )));
        };

        let child_value = field_ids_value.child(i)?;
        let child_type = child_value.logical_type()?;
        let mut field_id_value: Option<Value> = None;
        let mut field_id_nullable: Option<Value> = None;
        let mut has_child_field_ids = false;
        // the nested specification, kept for the recursion into the children
        let mut nested_value: Option<Value> = None;
        if child_type.id()? == type_id::STRUCT {
            for (nested_i, (field_id_or_nested_col, _)) in struct_children(&child_type)?.into_iter().enumerate() {
                if field_id_or_nested_col == DUCKDB_FIELD_ID {
                    field_id_value = Some(child_value.child(nested_i)?);
                } else if field_id_or_nested_col == DUCKDB_NULLABLE_ID {
                    field_id_nullable = Some(child_value.child(nested_i)?);
                } else {
                    has_child_field_ids = true;
                }
            }
            nested_value = Some(child_value);
        } else {
            field_id_value = Some(child_value);
        }

        let field_id = match field_id_value {
            Some(value) => {
                let field_id_int = value.cast(factory, &LogicalType::new(factory, type_id::INTEGER)?)?.as_i32()?;
                if !unique_field_ids.insert(field_id_int as u32) {
                    return Err(Error::binder(format!("Duplicate field_id {field_id_int} found in FIELD_IDS")));
                }
                match field_id_nullable {
                    Some(nullable) => {
                        let nullable = nullable.cast(factory, &LogicalType::new(factory, type_id::BOOLEAN)?)?;
                        FieldId::new(field_id_int, nullable.as_bool()?)
                    }
                    None => FieldId::new(field_id_int, true),
                }
            }
            None => FieldId::unset(),
        };
        // `emplace`: an earlier entry of the same lowercase name is kept
        let inserted = field_ids.ids().entry(col_name.clone()).or_insert(field_id);

        if let Some(nested_value) = nested_value.filter(|_| has_child_field_ids) {
            let type_id = col_type.id()?;
            if type_id != type_id::LIST && type_id != type_id::MAP && type_id != type_id::STRUCT {
                return Err(Error::binder(format!(
                    "Column \"{col_name}\" with type \"{}\" cannot have a nested FIELD_IDS specification",
                    col_type.name()?
                )));
            }
            let child_map = child_name_to_type_map(col_type)?;
            get_field_ids(factory, &nested_value, &mut inserted.children, unique_field_ids, &child_map)?;
        }
    }
    Ok(())
}

/// Parses the `FIELD_IDS` value against the written columns.
pub(crate) fn parse_field_ids(
    factory: Factory,
    input: &Value,
    names: &[String],
    types: &[LogicalType],
) -> Result<ChildFieldIds> {
    let mut unique_field_ids = HashSet::new();
    let mut name_to_type_map = NameToTypeMap::new();
    for (name, type_) in names.iter().zip(types) {
        if name == DUCKDB_FIELD_ID {
            return Err(Error::binder(format!(
                "Cannot have a column named \"{DUCKDB_FIELD_ID}\" when writing FIELD_IDS"
            )));
        }
        name_to_type_map.push((name.to_ascii_lowercase(), type_.try_clone()?));
    }
    let mut result = ChildFieldIds::default();
    get_field_ids(factory, input, &mut result, &mut unique_field_ids, &name_to_type_map)?;
    Ok(result)
}
