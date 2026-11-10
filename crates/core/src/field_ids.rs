//! The `FIELD_IDS` option of `COPY ... TO (FORMAT AVRO)`: Iceberg-style field ids for columns and nested fields.

use std::collections::{HashMap, HashSet};

use duckdb_neo::{
    Parameters, Result,
    connection::Context,
    logical_type::{LogicalType, LogicalTypeID},
    value::Value,
};

use crate::copy::schema::type_param;
use crate::error::{binder, internal, invalid_input};

/// The key of a `FIELD_IDS` struct that sets the id of the field itself.
pub const DUCKDB_FIELD_ID: &str = "__duckdb_field_id";
/// The key of a `FIELD_IDS` struct that sets whether the field is nullable.
pub const DUCKDB_NULLABLE_ID: &str = "__duckdb_nullable";

/// The field id of one column or nested field, and those of its children.
#[derive(Debug)]
pub struct FieldId {
    id: Option<i32>,
    pub nullable: bool,
    pub children: ChildFieldIds,
}

impl Default for FieldId {
    fn default() -> Self {
        Self {
            id: None,
            nullable: true,
            children: ChildFieldIds::default(),
        }
    }
}

impl FieldId {
    /// The id; a field that is only listed for the ids of its children has none, which is an error here.
    pub fn field_id(&self) -> Result<i32> {
        self.id
            .ok_or_else(|| internal("Field id of the field is not set"))
    }
}

/// The field ids of the children of a column (or of the columns), keyed by lowercase name.
#[derive(Debug, Default)]
pub struct ChildFieldIds {
    ids: HashMap<String, FieldId>,
}

impl ChildFieldIds {
    /// The field id of a child, looked up by name case-insensitively.
    pub fn find(&self, name: &str) -> Option<&FieldId> {
        self.ids.get(&name.to_ascii_lowercase())
    }
}

/// Parses the `FIELD_IDS` value for the columns `names` of types `types`.
pub fn parse_field_ids(
    context: &Context,
    input: &Value,
    names: &[String],
    types: &[LogicalType],
) -> Result<ChildFieldIds> {
    let mut name_to_type = Vec::with_capacity(names.len());
    for (name, logical_type) in names.iter().zip(types) {
        if name == DUCKDB_FIELD_ID {
            return Err(binder(format!(
                "Cannot have a column named \"{DUCKDB_FIELD_ID}\" when writing FIELD_IDS"
            )));
        }
        name_to_type.push((name.to_ascii_lowercase(), logical_type.clone()));
    }
    let mut result = ChildFieldIds::default();
    let mut unique_field_ids = HashSet::new();
    parse_children(
        context,
        input,
        &mut result,
        &mut unique_field_ids,
        &name_to_type,
    )?;
    Ok(result)
}

fn child_name_to_type(logical_type: &LogicalType) -> Result<Vec<(String, LogicalType)>> {
    match logical_type.id()? {
        LogicalTypeID::List => Ok(vec![("list".to_owned(), type_param(logical_type, 0)?)]),
        LogicalTypeID::Map => Ok(vec![
            ("key".to_owned(), type_param(logical_type, 0)?),
            ("value".to_owned(), type_param(logical_type, 1)?),
        ]),
        LogicalTypeID::Struct => {
            let mut result = Vec::new();
            for (name, value) in logical_type.get_params()? {
                if name == DUCKDB_FIELD_ID {
                    return Err(binder(format!(
                        "Cannot have column named \"{DUCKDB_FIELD_ID}\" with FIELD_IDS"
                    )));
                }
                let child_type = value
                    .get::<LogicalType>()?
                    .ok_or_else(|| internal("Type parameter is not a type"))?;
                result.push((name.to_ascii_lowercase(), child_type));
            }
            Ok(result)
        }
        _ => Err(internal("Unexpected type for nested FIELD_IDS")),
    }
}

fn non_null<T>(value: Option<T>, column: &str) -> Result<T> {
    value.ok_or_else(|| invalid_input(format!("FIELD_IDS of column \"{column}\" can not be NULL")))
}

fn parse_children(
    context: &Context,
    field_ids_value: &Value,
    field_ids: &mut ChildFieldIds,
    unique_field_ids: &mut HashSet<i32>,
    name_to_type: &[(String, LogicalType)],
) -> Result<()> {
    let struct_type = field_ids_value.logical_type()?;
    if struct_type.id()? != LogicalTypeID::Struct {
        return Err(binder(format!(
            "Expected FIELD_IDS to be a STRUCT, e.g., {{col1: 42, col2: {{{DUCKDB_FIELD_ID}: 43, nested_col: 44}}, col3: 44}}"
        )));
    }
    for (index, (name, _)) in struct_type.get_params()?.into_iter().enumerate() {
        let col_name = name.to_ascii_lowercase();
        if col_name == DUCKDB_FIELD_ID || col_name == DUCKDB_NULLABLE_ID {
            continue;
        }
        let Some((_, col_type)) = name_to_type.iter().find(|(name, _)| *name == col_name) else {
            let names = name_to_type
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(binder(format!(
                "Column name \"{col_name}\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS if this column is a partition column. Available column names: [{names}]"
            )));
        };

        let child_value = field_ids_value.get_child(index)?;
        let child_type = child_value.logical_type()?;
        let mut field_id_value = None;
        let mut nullable_value = None;
        let mut has_child_field_ids = false;
        if child_type.id()? == LogicalTypeID::Struct {
            for (nested_index, (nested_name, _)) in child_type.get_params()?.into_iter().enumerate()
            {
                if nested_name == DUCKDB_FIELD_ID {
                    field_id_value = Some(child_value.get_child(nested_index)?);
                } else if nested_name == DUCKDB_NULLABLE_ID {
                    nullable_value = Some(child_value.get_child(nested_index)?);
                } else {
                    has_child_field_ids = true;
                }
            }
        } else {
            field_id_value = Some(field_ids_value.get_child(index)?);
        }

        let mut field_id = FieldId::default();
        if let Some(field_id_value) = field_id_value {
            let integer =
                LogicalType::create_from_id(context, LogicalTypeID::Integer, Parameters::None)?;
            let id = non_null(
                field_id_value.cast(context, &integer)?.get::<i32>()?,
                &col_name,
            )?;
            if !unique_field_ids.insert(id) {
                return Err(binder(format!(
                    "Duplicate field_id {id} found in FIELD_IDS"
                )));
            }
            field_id.id = Some(id);
            if let Some(nullable_value) = nullable_value {
                let boolean =
                    LogicalType::create_from_id(context, LogicalTypeID::Boolean, Parameters::None)?;
                field_id.nullable = non_null(
                    nullable_value.cast(context, &boolean)?.get::<bool>()?,
                    &col_name,
                )?;
            }
        }
        let entry = field_ids.ids.entry(col_name.clone()).or_insert(field_id);

        if has_child_field_ids {
            if !matches!(
                col_type.id()?,
                LogicalTypeID::List | LogicalTypeID::Map | LogicalTypeID::Struct
            ) {
                return Err(binder(format!(
                    "Column \"{col_name}\" with type \"{}\" cannot have a nested FIELD_IDS specification",
                    col_type.name()?
                )));
            }
            parse_children(
                context,
                &child_value,
                &mut entry.children,
                unique_field_ids,
                &child_name_to_type(col_type)?,
            )?;
        }
    }
    Ok(())
}
