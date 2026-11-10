//! The Avro schema model. Nodes live in an arena so that named types can be referenced from several places and
//! recursive schemas are representable: a reference to a named type is a `Node::Link` to the node defining it.

use std::collections::HashMap;
use std::fmt::Write as _;

use serde_json::Value;

use super::error::{AvroError, Result};

pub type NodeId = usize;

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Null,
    Boolean,
    Int {
        logical_type: Option<String>,
    },
    Long {
        logical_type: Option<String>,
        adjust_to_utc: Option<bool>,
    },
    Float,
    Double,
    Bytes {
        logical_type: Option<String>,
        precision: Option<i32>,
        scale: Option<i32>,
    },
    String,
    Record {
        name: String,
        namespace: Option<String>,
        fields: Vec<Field>,
    },
    Enum {
        name: String,
        namespace: Option<String>,
        symbols: Vec<String>,
    },
    Fixed {
        name: String,
        namespace: Option<String>,
        size: i64,
        logical_type: Option<String>,
        precision: Option<i32>,
        scale: Option<i32>,
    },
    Array {
        items: NodeId,
        element_id: Option<i32>,
        logical_type: Option<String>,
    },
    Map {
        values: NodeId,
        key_id: Option<i32>,
        value_id: Option<i32>,
    },
    Union {
        branches: Vec<NodeId>,
    },
    /// A reference to a named type (record, enum or fixed) defined elsewhere in the schema.
    Link {
        target: NodeId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub schema: NodeId,
    pub field_id: Option<i32>,
}

/// A parsed Avro schema: an arena of nodes and the id of the root node.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    nodes: Vec<Node>,
    root: NodeId,
}

impl Schema {
    /// Parses a schema the way avro-c does. Accepts any JSON value at the root (e.g. `"int"`).
    pub fn parse(json: &[u8]) -> Result<Schema> {
        let value: Value = serde_json::from_slice(json)
            .map_err(|e| AvroError::new(format!("Error parsing JSON: {e}")))?;
        let mut parser = Parser::default();
        let root = parser.parse(&value, None)?;
        Ok(Schema {
            nodes: parser.nodes,
            root,
        })
    }

    pub fn root(&self) -> NodeId {
        self.root
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id]
    }

    /// Follows `Link`s until reaching a non-link node.
    pub fn resolve(&self, mut id: NodeId) -> NodeId {
        while let Node::Link { target } = self.nodes[id] {
            id = target;
        }
        id
    }

    /// The schema rendered exactly as avro-c's `avro_schema_to_json` renders it (this is what goes in files).
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        self.write_json(self.root, None, &mut out);
        out
    }

    fn write_json(&self, id: NodeId, parent_namespace: Option<&str>, out: &mut String) {
        match &self.nodes[id] {
            Node::Null => out.push_str("\"null\""),
            Node::Boolean => out.push_str("{\"type\":\"boolean\"}"),
            Node::Int { logical_type } => {
                out.push_str("{\"type\":\"int\"");
                write_logical_type(logical_type, out);
                out.push('}');
            }
            Node::Long {
                logical_type,
                adjust_to_utc,
            } => {
                out.push_str("{\"type\":\"long\"");
                write_logical_type(logical_type, out);
                if let Some(adjust_to_utc) = adjust_to_utc {
                    let _ = write!(out, ",\"adjust-to-utc\":{adjust_to_utc}");
                }
                out.push('}');
            }
            Node::Float => out.push_str("{\"type\":\"float\"}"),
            Node::Double => out.push_str("{\"type\":\"double\"}"),
            Node::Bytes {
                logical_type,
                precision,
                scale,
            } => {
                out.push_str("{\"type\":\"bytes\"");
                write_logical_type(logical_type, out);
                write_number_member("precision", *precision, out);
                write_number_member("scale", *scale, out);
                out.push('}');
            }
            Node::String => out.push_str("{\"type\":\"string\"}"),
            Node::Record {
                name,
                namespace,
                fields,
            } => {
                write_named_start("record", name, namespace.as_deref(), parent_namespace, out);
                out.push_str("\"fields\":[");
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = write!(out, "{{\"name\":\"{}\",\"type\":", field.name);
                    self.write_json(field.schema, namespace.as_deref(), out);
                    write_number_member("field-id", field.field_id, out);
                    out.push('}');
                }
                out.push_str("]}");
            }
            Node::Enum {
                name,
                namespace,
                symbols,
            } => {
                write_named_start("enum", name, namespace.as_deref(), parent_namespace, out);
                out.push_str("\"symbols\":[");
                for (i, symbol) in symbols.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = write!(out, "\"{symbol}\"");
                }
                out.push_str("]}");
            }
            Node::Fixed {
                name,
                namespace,
                size,
                logical_type,
                precision,
                scale,
            } => {
                write_named_start("fixed", name, namespace.as_deref(), parent_namespace, out);
                let _ = write!(out, "\"size\":{size}");
                write_logical_type(logical_type, out);
                write_number_member("precision", *precision, out);
                write_number_member("scale", *scale, out);
                out.push('}');
            }
            Node::Array {
                items,
                element_id,
                logical_type,
            } => {
                out.push_str("{\"type\":\"array\"");
                write_number_member("element-id", *element_id, out);
                if let Some(logical_type) = logical_type {
                    // The space after the colon is what avro-c writes.
                    let _ = write!(out, ",\"logicalType\": \"{logical_type}\"");
                }
                out.push_str(",\"items\":");
                self.write_json(*items, parent_namespace, out);
                out.push('}');
            }
            Node::Map {
                values,
                key_id,
                value_id,
            } => {
                out.push_str("{\"type\":\"map\"");
                write_number_member("key-id", *key_id, out);
                write_number_member("value-id", *value_id, out);
                out.push_str(",\"values\":");
                self.write_json(*values, parent_namespace, out);
                out.push('}');
            }
            Node::Union { branches } => {
                out.push('[');
                for (i, branch) in branches.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    self.write_json(*branch, parent_namespace, out);
                }
                out.push(']');
            }
            Node::Link { target } => {
                let (name, namespace) = match &self.nodes[*target] {
                    Node::Record {
                        name, namespace, ..
                    }
                    | Node::Enum {
                        name, namespace, ..
                    }
                    | Node::Fixed {
                        name, namespace, ..
                    } => (name.as_str(), namespace.as_deref()),
                    _ => ("", None),
                };
                out.push('"');
                if let Some(namespace) = namespace
                    && Some(namespace) != parent_namespace
                {
                    let _ = write!(out, "{namespace}.");
                }
                let _ = write!(out, "{name}\"");
            }
        }
    }
}

fn write_logical_type(logical_type: &Option<String>, out: &mut String) {
    if let Some(logical_type) = logical_type {
        let _ = write!(out, ",\"logicalType\":\"{logical_type}\"");
    }
}

fn write_number_member(key: &str, value: Option<i32>, out: &mut String) {
    if let Some(value) = value {
        let _ = write!(out, ",\"{key}\":{value}");
    }
}

fn write_named_start(
    type_name: &str,
    name: &str,
    namespace: Option<&str>,
    parent_namespace: Option<&str>,
    out: &mut String,
) {
    let _ = write!(out, "{{\"type\":\"{type_name}\",\"name\":\"{name}\",");
    if namespace != parent_namespace {
        let _ = write!(out, "\"namespace\":\"{}\",", namespace.unwrap_or(""));
    }
}

#[derive(Default)]
struct Parser {
    nodes: Vec<Node>,
    named: HashMap<String, NodeId>,
}

impl Parser {
    fn push(&mut self, node: Node) -> NodeId {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn parse(&mut self, json: &Value, namespace: Option<&str>) -> Result<NodeId> {
        let type_value = match json {
            Value::Array(branches) => return self.parse_union(branches, namespace),
            Value::Object(object) => object.get("type"),
            other => Some(other),
        };
        let Some(Value::String(type_name)) = type_value else {
            return Err(AvroError::new("\"type\" field must be a string"));
        };
        let node = match type_name.as_str() {
            "string" => Node::String,
            "bytes" => {
                let logical_type = string_member(json, "logicalType");
                let (precision, scale) = if logical_type.is_some() {
                    (
                        number_member(json, "precision"),
                        number_member(json, "scale"),
                    )
                } else {
                    (None, None)
                };
                Node::Bytes {
                    logical_type,
                    precision,
                    scale,
                }
            }
            "int" => Node::Int {
                logical_type: string_member(json, "logicalType"),
            },
            "long" => {
                let logical_type = string_member(json, "logicalType");
                let adjust_to_utc = match logical_type {
                    Some(_) => member(json, "adjust-to-utc").map(|v| v == &Value::Bool(true)),
                    None => None,
                };
                Node::Long {
                    logical_type,
                    adjust_to_utc,
                }
            }
            "float" => Node::Float,
            "double" => Node::Double,
            "boolean" => Node::Boolean,
            "null" => Node::Null,
            "record" => return self.parse_record(json, namespace),
            "enum" => return self.parse_enum(json, namespace),
            "array" => {
                let items = member(json, "items")
                    .ok_or_else(|| AvroError::new("Array type must have \"items\""))?;
                let items = self.parse(items, namespace)?;
                Node::Array {
                    items,
                    element_id: id_member(json, "element-id"),
                    logical_type: string_member(json, "logicalType"),
                }
            }
            "map" => {
                let values = member(json, "values")
                    .ok_or_else(|| AvroError::new("Map type must have \"values\""))?;
                let values = self.parse(values, namespace)?;
                Node::Map {
                    values,
                    key_id: id_member(json, "key-id"),
                    value_id: id_member(json, "value-id"),
                }
            }
            "fixed" => return self.parse_fixed(json, namespace),
            other => match self.named.get(&qualify_name(other, namespace)) {
                Some(&target) => Node::Link { target },
                None => return Err(AvroError::new(format!("Unknown Avro \"type\": {other}"))),
            },
        };
        Ok(self.push(node))
    }

    fn parse_union(&mut self, branches: &[Value], namespace: Option<&str>) -> Result<NodeId> {
        if branches.is_empty() {
            return Err(AvroError::new("Union type must have at least one branch"));
        }
        let branches = branches
            .iter()
            .map(|branch| self.parse(branch, namespace))
            .collect::<Result<Vec<_>>>()?;
        Ok(self.push(Node::Union { branches }))
    }

    fn parse_record(&mut self, json: &Value, parent_namespace: Option<&str>) -> Result<NodeId> {
        let Some(Value::String(full_name)) = member(json, "name") else {
            return Err(AvroError::new("Record type must have a \"name\""));
        };
        let Some(Value::Array(json_fields)) = member(json, "fields") else {
            return Err(AvroError::new("Record type must have \"fields\""));
        };
        let (name, namespace) = split_full_name(json, full_name, parent_namespace)?;
        let full_name = qualify_name(&name, namespace.as_deref());
        let id = self.push(Node::Record {
            name,
            namespace: namespace.clone(),
            fields: Vec::new(),
        });
        self.register(full_name, id, "record")?;

        let mut fields = Vec::with_capacity(json_fields.len());
        for (i, json_field) in json_fields.iter().enumerate() {
            if !json_field.is_object() {
                return Err(AvroError::new(format!("Record field {i} must be an array")));
            }
            let field_name = member(json_field, "name")
                .ok_or_else(|| AvroError::new(format!("Record field {i} must have a \"name\"")))?;
            let field_type = member(json_field, "type")
                .ok_or_else(|| AvroError::new(format!("Record field {i} must have a \"type\"")))?;
            let schema = self.parse(field_type, namespace.as_deref())?;
            let field_id = id_member(json_field, "field-id");
            let Value::String(field_name) = field_name else {
                return Err(AvroError::new(
                    "Invalid field name in avro_schema_record_field_append",
                ));
            };
            if !is_avro_id(field_name) {
                return Err(AvroError::new("Invalid Avro identifier"));
            }
            fields.push(Field {
                name: field_name.clone(),
                schema,
                field_id,
            });
        }
        if let Node::Record { fields: slot, .. } = &mut self.nodes[id] {
            *slot = fields;
        }
        Ok(id)
    }

    fn parse_enum(&mut self, json: &Value, parent_namespace: Option<&str>) -> Result<NodeId> {
        let Some(Value::String(full_name)) = member(json, "name") else {
            return Err(AvroError::new("Enum type must have a \"name\""));
        };
        let Some(Value::Array(json_symbols)) = member(json, "symbols") else {
            return Err(AvroError::new("Enum type must have \"symbols\""));
        };
        if json_symbols.is_empty() {
            return Err(AvroError::new("Enum type must have at least one symbol"));
        }
        let (name, namespace) = split_full_name(json, full_name, parent_namespace)?;
        let full_name = qualify_name(&name, namespace.as_deref());
        let id = self.push(Node::Enum {
            name,
            namespace,
            symbols: Vec::new(),
        });
        self.register(full_name, id, "enum")?;
        let symbols = json_symbols
            .iter()
            .enumerate()
            .map(|(i, symbol)| match symbol {
                Value::String(symbol) => Ok(symbol.clone()),
                _ => Err(AvroError::new(format!("Enum symbol {i} must be a string"))),
            })
            .collect::<Result<Vec<_>>>()?;
        if let Node::Enum { symbols: slot, .. } = &mut self.nodes[id] {
            *slot = symbols;
        }
        Ok(id)
    }

    fn parse_fixed(&mut self, json: &Value, parent_namespace: Option<&str>) -> Result<NodeId> {
        let Some(size) = member(json, "size").and_then(Value::as_i64) else {
            return Err(AvroError::new("Fixed type must have a \"size\""));
        };
        let Some(Value::String(full_name)) = member(json, "name") else {
            return Err(AvroError::new("Fixed type must have a \"name\""));
        };
        let (name, namespace) = split_full_name(json, full_name, parent_namespace)?;
        let full_name = qualify_name(&name, namespace.as_deref());
        let id = self.push(Node::Fixed {
            name,
            namespace,
            size,
            logical_type: string_member(json, "logicalType"),
            precision: number_member(json, "precision"),
            scale: number_member(json, "scale"),
        });
        self.register(full_name, id, "fixed")?;
        Ok(id)
    }

    fn register(&mut self, full_name: String, id: NodeId, kind: &str) -> Result<()> {
        if self.named.contains_key(&full_name) {
            return Err(AvroError::new(format!("Cannot save {kind} schema")));
        }
        self.named.insert(full_name, id);
        Ok(())
    }
}

fn member<'v>(json: &'v Value, key: &str) -> Option<&'v Value> {
    json.as_object().and_then(|object| object.get(key))
}

fn string_member(json: &Value, key: &str) -> Option<String> {
    member(json, key).and_then(Value::as_str).map(str::to_owned)
}

fn id_member(json: &Value, key: &str) -> Option<i32> {
    let id = member(json, key).and_then(Value::as_i64)? as i32;
    (id != i32::MAX).then_some(id)
}

fn number_member(json: &Value, key: &str) -> Option<i32> {
    let value = member(json, key)?.as_i64().unwrap_or(0) as i32;
    (value != -1).then_some(value)
}

fn qualify_name(name: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(namespace) if !name.contains('.') => format!("{namespace}.{name}"),
        _ => name.to_owned(),
    }
}

fn split_full_name(
    json: &Value,
    full_name: &str,
    parent_namespace: Option<&str>,
) -> Result<(String, Option<String>)> {
    let (name, namespace) = if let Some(dot) = full_name.rfind('.') {
        (&full_name[dot + 1..], Some(&full_name[..dot]))
    } else if let Some(Value::String(namespace)) = member(json, "namespace") {
        (
            full_name,
            Some(namespace.as_str()).filter(|ns| !ns.is_empty()),
        )
    } else {
        (full_name, parent_namespace)
    };
    if !is_avro_id(name) {
        return Err(AvroError::new("Invalid Avro identifier"));
    }
    Ok((name.to_owned(), namespace.map(str::to_owned)))
}

fn is_avro_id(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(json: &str) -> String {
        Schema::parse(json.as_bytes()).unwrap().to_json()
    }

    fn error(json: &str) -> String {
        Schema::parse(json.as_bytes()).unwrap_err().message
    }

    #[test]
    fn renders_nullable_int_record() {
        assert_eq!(
            render(
                r#"{"type":"record","name":"root","fields":[{"name":"a","type":["null","int"]}]}"#
            ),
            r#"{"type":"record","name":"root","fields":[{"name":"a","type":["null",{"type":"int"}]}]}"#
        );
    }

    #[test]
    fn renders_primitives() {
        assert_eq!(render(r#""null""#), r#""null""#);
        assert_eq!(render(r#"{"type":"null"}"#), r#""null""#);
        for name in [
            "string", "bytes", "int", "long", "float", "double", "boolean",
        ] {
            assert_eq!(
                render(&format!("\"{name}\"")),
                format!("{{\"type\":\"{name}\"}}")
            );
        }
    }

    #[test]
    fn renders_logical_types() {
        assert_eq!(
            render(r#"{"type":"int","logicalType":"date","doc":"x"}"#),
            r#"{"type":"int","logicalType":"date"}"#
        );
        assert_eq!(
            render(r#"{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}"#),
            r#"{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}"#
        );
        assert_eq!(
            render(r#"{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":"yes"}"#),
            r#"{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":false}"#
        );
        assert_eq!(
            render(r#"{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}"#),
            r#"{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}"#
        );
        assert_eq!(
            render(r#"{"type":"bytes","logicalType":"decimal","precision":"9"}"#),
            r#"{"type":"bytes","logicalType":"decimal","precision":0}"#
        );
        assert_eq!(
            render(r#"{"type":"string","logicalType":"uuid"}"#),
            r#"{"type":"string"}"#
        );
        assert_eq!(
            render(r#"{"type":"int","logicalType":5}"#),
            r#"{"type":"int"}"#
        );
    }

    #[test]
    fn extras_without_logical_type_are_dropped() {
        assert_eq!(
            render(r#"{"type":"long","adjust-to-utc":true}"#),
            r#"{"type":"long"}"#
        );
        assert_eq!(
            render(r#"{"type":"bytes","precision":4,"scale":2}"#),
            r#"{"type":"bytes"}"#
        );
    }

    #[test]
    fn renders_fixed() {
        assert_eq!(
            render(
                r#"{"type":"fixed","name":"dec","size":16,"logicalType":"decimal","precision":38,"scale":10}"#
            ),
            r#"{"type":"fixed","name":"dec","size":16,"logicalType":"decimal","precision":38,"scale":10}"#
        );
        assert_eq!(
            render(r#"{"type":"fixed","name":"f","size":4,"precision":3}"#),
            r#"{"type":"fixed","name":"f","size":4,"precision":3}"#
        );
        assert_eq!(
            render(r#"{"type":"fixed","name":"a.b.f","size":4}"#),
            r#"{"type":"fixed","name":"f","namespace":"a.b","size":4}"#
        );
    }

    #[test]
    fn renders_array_and_map() {
        assert_eq!(
            render(r#"{"type":"array","items":"int","element-id":3,"logicalType":"map"}"#),
            r#"{"type":"array","element-id":3,"logicalType": "map","items":{"type":"int"}}"#
        );
        assert_eq!(
            render(r#"{"type":"array","items":"int","element-id":2147483647}"#),
            r#"{"type":"array","items":{"type":"int"}}"#
        );
        assert_eq!(
            render(r#"{"type":"map","values":"string","key-id":1,"value-id":2}"#),
            r#"{"type":"map","key-id":1,"value-id":2,"values":{"type":"string"}}"#
        );
        assert_eq!(
            render(r#"{"type":"map","values":"string","key-id":1.5}"#),
            r#"{"type":"map","values":{"type":"string"}}"#
        );
    }

    #[test]
    fn ids_truncate_to_i32() {
        assert_eq!(
            render(r#"{"type":"array","items":"int","element-id":4294967297}"#),
            r#"{"type":"array","element-id":1,"items":{"type":"int"}}"#
        );
    }

    #[test]
    fn renders_enum_and_field_ids() {
        assert_eq!(
            render(
                r#"{"type":"record","name":"r","fields":[{"name":"e","field-id":7,"type":{"type":"enum","name":"E","symbols":["A","B"]}},{"name":"e","type":"E"}]}"#
            ),
            r#"{"type":"record","name":"r","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]},"field-id":7},{"name":"e","type":"E"}]}"#
        );
    }

    #[test]
    fn renders_namespaces() {
        let json = r#"{"type":"record","name":"Outer","namespace":"com.x","fields":[
            {"name":"same","type":{"type":"record","name":"Inner","fields":[]}},
            {"name":"other","type":{"type":"record","name":"org.y.Other","fields":[
                {"name":"back","type":"com.x.Inner"},
                {"name":"own","type":"Other"},
                {"name":"none","type":{"type":"enum","name":"Plain","namespace":"","symbols":["A"]}}
            ]}},
            {"name":"inner_again","type":"Inner"},
            {"name":"other_again","type":"org.y.Other"}
        ]}"#;
        assert_eq!(
            render(json),
            concat!(
                r#"{"type":"record","name":"Outer","namespace":"com.x","fields":["#,
                r#"{"name":"same","type":{"type":"record","name":"Inner","fields":[]}},"#,
                r#"{"name":"other","type":{"type":"record","name":"Other","namespace":"org.y","fields":["#,
                r#"{"name":"back","type":"com.x.Inner"},"#,
                r#"{"name":"own","type":"Other"},"#,
                r#"{"name":"none","type":{"type":"enum","name":"Plain","namespace":"","symbols":["A"]}}]}},"#,
                r#"{"name":"inner_again","type":"Inner"},"#,
                r#"{"name":"other_again","type":"org.y.Other"}]}"#
            )
        );
    }

    #[test]
    fn unqualified_lookup_needs_current_namespace() {
        let json = r#"{"type":"record","name":"a.R","fields":[
            {"name":"x","type":{"type":"fixed","name":"F","namespace":"","size":1}},
            {"name":"y","type":"F"}]}"#;
        assert_eq!(error(json), "Unknown Avro \"type\": F");
    }

    #[test]
    fn recursive_schema_uses_links() {
        let json = r#"{"type":"record","name":"Node","fields":[
            {"name":"value","type":"long"},
            {"name":"next","type":["null","Node"]}]}"#;
        let schema = Schema::parse(json.as_bytes()).unwrap();
        let Node::Record { fields, .. } = schema.node(schema.root()) else {
            panic!("not a record");
        };
        let Node::Union { branches } = schema.node(fields[1].schema) else {
            panic!("not a union");
        };
        assert_eq!(
            schema.node(branches[1]),
            &Node::Link {
                target: schema.root()
            }
        );
        assert_eq!(schema.resolve(branches[1]), schema.root());
        assert_eq!(schema.resolve(fields[0].schema), fields[0].schema);
        assert_eq!(
            schema.to_json(),
            r#"{"type":"record","name":"Node","fields":[{"name":"value","type":{"type":"long"}},{"name":"next","type":["null","Node"]}]}"#
        );
    }

    #[test]
    fn rendering_round_trips() {
        let json = r#"{"type":"record","name":"r","namespace":"n","fields":[
            {"name":"a","type":{"type":"array","items":{"type":"map","values":["null","r"]}}},
            {"name":"f","type":{"type":"fixed","name":"m.F","size":2}},
            {"name":"g","type":"m.F"}]}"#;
        let schema = Schema::parse(json.as_bytes()).unwrap();
        let rendered = schema.to_json();
        let reparsed = Schema::parse(rendered.as_bytes()).unwrap();
        assert_eq!(reparsed, schema);
        assert_eq!(reparsed.to_json(), rendered);
    }

    #[test]
    fn parse_errors() {
        assert!(error("{").starts_with("Error parsing JSON: "));
        assert_eq!(error("5"), "\"type\" field must be a string");
        assert_eq!(
            error(r#"{"type":{"type":"int"}}"#),
            "\"type\" field must be a string"
        );
        assert_eq!(error(r#"{"name":"x"}"#), "\"type\" field must be a string");
        assert_eq!(error(r#""integer""#), "Unknown Avro \"type\": integer");
        assert_eq!(error("[]"), "Union type must have at least one branch");
        assert_eq!(error(r#""record""#), "Record type must have a \"name\"");
        assert_eq!(
            error(r#"{"type":"record","name":"r"}"#),
            "Record type must have \"fields\""
        );
        assert_eq!(
            error(r#"{"type":"record","name":"1r","fields":[]}"#),
            "Invalid Avro identifier"
        );
        assert_eq!(
            error(r#"{"type":"record","name":"r","fields":[1]}"#),
            "Record field 0 must be an array"
        );
        assert_eq!(
            error(
                r#"{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"type":"int"}]}"#
            ),
            "Record field 1 must have a \"name\""
        );
        assert_eq!(
            error(r#"{"type":"record","name":"r","fields":[{"name":"a"}]}"#),
            "Record field 0 must have a \"type\""
        );
        assert_eq!(
            error(r#"{"type":"record","name":"r","fields":[{"name":5,"type":"nope"}]}"#),
            "Unknown Avro \"type\": nope"
        );
        assert_eq!(
            error(r#"{"type":"record","name":"r","fields":[{"name":5,"type":"int"}]}"#),
            "Invalid field name in avro_schema_record_field_append"
        );
        assert_eq!(
            error(r#"{"type":"record","name":"r","fields":[{"name":"a-b","type":"int"}]}"#),
            "Invalid Avro identifier"
        );
        assert_eq!(
            error(
                r#"{"type":"record","name":"r","fields":[{"name":"x","type":"r"},{"name":"y","type":{"type":"record","name":"r","fields":[]}}]}"#
            ),
            "Cannot save record schema"
        );
        assert_eq!(
            error(r#"{"type":"enum","symbols":["A"]}"#),
            "Enum type must have a \"name\""
        );
        assert_eq!(
            error(r#"{"type":"enum","name":"E"}"#),
            "Enum type must have \"symbols\""
        );
        assert_eq!(
            error(r#"{"type":"enum","name":"E","symbols":[]}"#),
            "Enum type must have at least one symbol"
        );
        assert_eq!(
            error(r#"{"type":"enum","name":"E","symbols":["A",1]}"#),
            "Enum symbol 1 must be a string"
        );
        assert_eq!(
            error(
                r#"["null",{"type":"enum","name":"E","symbols":["A"]},{"type":"enum","name":"E","symbols":["B"]}]"#
            ),
            "Cannot save enum schema"
        );
        assert_eq!(
            error(r#"{"type":"array"}"#),
            "Array type must have \"items\""
        );
        assert_eq!(error(r#"{"type":"map"}"#), "Map type must have \"values\"");
        assert_eq!(
            error(r#"{"type":"fixed","name":"F","size":"4"}"#),
            "Fixed type must have a \"size\""
        );
        assert_eq!(
            error(r#"{"type":"fixed","size":4}"#),
            "Fixed type must have a \"name\""
        );
        assert_eq!(
            error(r#"[{"type":"fixed","name":"F","size":4},{"type":"fixed","name":"F","size":4}]"#),
            "Cannot save fixed schema"
        );
        assert_eq!(
            error(r#"{"type":"fixed","name":"a.","size":4}"#),
            "Invalid Avro identifier"
        );
    }

    #[test]
    fn duplicate_field_names_are_kept() {
        let schema =
            Schema::parse(br#"{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"a","type":"long"}]}"#)
                .unwrap();
        let Node::Record { fields, .. } = schema.node(schema.root()) else {
            panic!("not a record");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(
            schema.node(fields[1].schema),
            &Node::Long {
                logical_type: None,
                adjust_to_utc: None
            }
        );
    }

    #[test]
    fn symbols_are_not_validated() {
        assert_eq!(
            render(r#"{"type":"enum","name":"E","symbols":["a b","1"]}"#),
            r#"{"type":"enum","name":"E","symbols":["a b","1"]}"#
        );
    }

    #[test]
    fn leading_dot_gives_empty_namespace() {
        assert_eq!(
            render(r#"{"type":"fixed","name":".F","size":1}"#),
            r#"{"type":"fixed","name":"F","namespace":"","size":1}"#
        );
    }

    #[test]
    fn deep_nesting_is_an_error() {
        let json = format!("{}\"int\"{}", "[".repeat(100_000), "]".repeat(100_000));
        assert!(error(&json).starts_with("Error parsing JSON: "));
        let mut nested = String::from("\"int\"");
        for _ in 0..60 {
            nested = format!(r#"{{"type":"array","items":{nested}}}"#);
        }
        assert!(Schema::parse(nested.as_bytes()).is_ok());
    }
}
