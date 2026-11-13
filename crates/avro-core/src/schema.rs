//! The Avro schema model (Avro 1.12): parsing from JSON, serialization back to JSON, and an arena of nodes the
//! consumer walks by [`SchemaId`].
//!
//! Named types (`record`, `enum`, `fixed`) are stored once, where they are defined; every later use by name is a
//! [`SchemaKind::Ref`] node that carries the id of the definition. A reference is therefore always one hop away
//! ([`Schema::resolve`]) and recursive schemas are representable without cycles in memory: a walker that descends into
//! a `Ref` is responsible for noticing when it revisits a record (the DuckDB layer rejects recursion by record name).

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::ops::Index;

use crate::error::{Error, Result};
use crate::json::{self, Json};

/// Nesting depth of schema JSON after which parsing stops.
const MAX_DEPTH: usize = 256;

/// Index of a node in a [`Schema`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaId(u32);

impl SchemaId {
    /// The position in [`Schema::nodes`].
    pub fn index(self) -> usize {
        self.0 as usize
    }

    /// The id of the node at `index`, for the proof harnesses' hand-built arenas ([`Schema::unchecked`]).
    #[cfg(kani)]
    pub fn from_index(index: usize) -> SchemaId {
        SchemaId(index as u32)
    }
}

/// The name of a named type: the simple name plus an optional namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Name {
    pub name: String,
    pub namespace: Option<String>,
}

impl Name {
    /// A name without a namespace.
    pub fn simple(name: impl Into<String>) -> Self {
        Name { name: name.into(), namespace: None }
    }

    /// A name in a namespace (`None` or the empty string mean no namespace).
    pub fn new(name: impl Into<String>, namespace: Option<&str>) -> Self {
        Name { name: name.into(), namespace: namespace.filter(|ns| !ns.is_empty()).map(str::to_owned) }
    }

    /// Splits a possibly dotted name per the spec: a dotted name carries its own namespace, a plain name takes
    /// `default_namespace`.
    pub fn parse(text: &str, default_namespace: Option<&str>) -> Self {
        // a byte scan: `.` is ASCII, so the byte index is the char boundary `rfind` would give
        match text.bytes().rposition(|b| b == b'.') {
            Some(dot) => Name::new(&text[dot + 1..], Some(&text[..dot])),
            None => Name::new(text, default_namespace),
        }
    }

    /// `namespace.name`, or just `name` without a namespace.
    pub fn fullname(&self) -> String {
        match &self.namespace {
            Some(ns) => {
                let mut full = String::with_capacity(ns.len() + 1 + self.name.len());
                full.push_str(ns);
                full.push('.');
                full.push_str(&self.name);
                full
            }
            None => self.name.clone(),
        }
    }
}

/// A field of a record.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    /// The field's type.
    pub ty: SchemaId,
    /// The `default` attribute as written (never interpreted by this crate).
    pub default: Option<Json>,
    /// The Iceberg `field-id` attribute.
    pub field_id: Option<i64>,
    /// Every other attribute (`doc`, `aliases`, `order`, ...), in document order.
    pub extra: Vec<(String, Json)>,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: SchemaId) -> Self {
        Field { name: name.into(), ty, default: None, field_id: None, extra: Vec::new() }
    }

    pub fn with_field_id(mut self, field_id: i64) -> Self {
        self.field_id = Some(field_id);
        self
    }
}

/// The structural part of a schema node.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaKind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record {
        name: Name,
        fields: Vec<Field>,
        /// Declared as `{"type": "error"}`, the error record of an Avro protocol (the same shape as a record; Java
        /// accepts it anywhere a record is allowed, and so do files in the wild). Written back as `error`.
        is_error: bool,
    },
    Enum {
        name: Name,
        symbols: Vec<String>,
        default: Option<String>,
    },
    Array {
        items: SchemaId,
        element_id: Option<i64>,
    },
    Map {
        values: SchemaId,
        key_id: Option<i64>,
        value_id: Option<i64>,
    },
    Union {
        branches: Vec<SchemaId>,
    },
    Fixed {
        name: Name,
        size: usize,
    },
    /// A use of an already defined named type; `target` is the id of the defining node.
    Ref {
        name: Name,
        target: SchemaId,
    },
}

impl SchemaKind {
    /// The Avro type name as it appears in JSON (`"record"`, `"int"`, ...); the target's for a reference.
    pub fn type_name(&self) -> &'static str {
        match self {
            SchemaKind::Null => "null",
            SchemaKind::Boolean => "boolean",
            SchemaKind::Int => "int",
            SchemaKind::Long => "long",
            SchemaKind::Float => "float",
            SchemaKind::Double => "double",
            SchemaKind::Bytes => "bytes",
            SchemaKind::String => "string",
            SchemaKind::Record { is_error: false, .. } => "record",
            SchemaKind::Record { is_error: true, .. } => "error",
            SchemaKind::Enum { .. } => "enum",
            SchemaKind::Array { .. } => "array",
            SchemaKind::Map { .. } => "map",
            SchemaKind::Union { .. } => "union",
            SchemaKind::Fixed { .. } => "fixed",
            SchemaKind::Ref { .. } => "ref",
        }
    }

    /// The name of a record, enum, fixed or reference node.
    pub fn name(&self) -> Option<&Name> {
        match self {
            SchemaKind::Record { name, .. }
            | SchemaKind::Enum { name, .. }
            | SchemaKind::Fixed { name, .. }
            | SchemaKind::Ref { name, .. } => Some(name),
            _ => None,
        }
    }

    /// Whether this node defines a named type (a `Ref` does not).
    pub fn is_definition(&self) -> bool {
        matches!(self, SchemaKind::Record { .. } | SchemaKind::Enum { .. } | SchemaKind::Fixed { .. })
    }

    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            SchemaKind::Null
                | SchemaKind::Boolean
                | SchemaKind::Int
                | SchemaKind::Long
                | SchemaKind::Float
                | SchemaKind::Double
                | SchemaKind::Bytes
                | SchemaKind::String
        )
    }
}

/// A schema node: the structure plus the attributes the consumer interprets.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaNode {
    pub kind: SchemaKind,
    /// The `logicalType` attribute, kept on every node kind (also on arrays: Iceberg's `"logicalType":"map"`).
    pub logical_type: Option<String>,
    /// The `precision` attribute of a decimal.
    pub precision: Option<i64>,
    /// The `scale` attribute of a decimal.
    pub scale: Option<i64>,
    /// The `adjust-to-utc` attribute of a timestamp.
    pub adjust_to_utc: Option<bool>,
    /// Every attribute not modelled above, in document order.
    pub extra: Vec<(String, Json)>,
}

impl SchemaNode {
    /// A node without attributes.
    pub fn new(kind: SchemaKind) -> Self {
        SchemaNode { kind, logical_type: None, precision: None, scale: None, adjust_to_utc: None, extra: Vec::new() }
    }

    pub fn with_logical_type(mut self, logical_type: impl Into<String>) -> Self {
        self.logical_type = Some(logical_type.into());
        self
    }

    pub fn with_decimal(mut self, precision: i64, scale: i64) -> Self {
        self.logical_type = Some("decimal".to_owned());
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    pub fn with_adjust_to_utc(mut self, adjust_to_utc: bool) -> Self {
        self.adjust_to_utc = Some(adjust_to_utc);
        self
    }

    /// The `size` of a fixed node.
    pub fn fixed_size(&self) -> Option<usize> {
        match &self.kind {
            SchemaKind::Fixed { size, .. } => Some(*size),
            _ => None,
        }
    }
}

/// The named-type map of a [`Schema`]: a deterministic hasher over a map of at most a few hundred entries filled once,
/// at parse time (`RandomState` seeds itself from the OS random source, whose retry loop the proof harnesses cannot
/// model; a schema is no hash-flooding target).
type NameMap = HashMap<String, SchemaId, BuildHasherDefault<DefaultHasher>>;

/// A parsed schema: an arena of nodes with a root.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    nodes: Vec<SchemaNode>,
    root: SchemaId,
    /// Full name of every named type -> its defining node.
    named: NameMap,
    /// [`Schema::min_encoded_size`] of every node.
    min_sizes: Vec<usize>,
}

impl Index<SchemaId> for Schema {
    type Output = SchemaNode;

    fn index(&self, id: SchemaId) -> &SchemaNode {
        &self.nodes[id.index()]
    }
}

impl Schema {
    /// Parses schema JSON text.
    pub fn parse(text: &str) -> Result<Schema> {
        Schema::from_json(&Json::parse(text)?)
    }

    /// Builds a schema from parsed JSON.
    pub fn from_json(json: &Json) -> Result<Schema> {
        let mut parser = ParseState { builder: SchemaBuilder::new(), named: HashMap::new() };
        let root = parser.node(json, None, 0)?;
        parser.builder.finish(root)
    }

    /// A single-node schema (a primitive, typically).
    pub fn primitive(kind: SchemaKind) -> Schema {
        let mut builder = SchemaBuilder::new();
        let root = builder.add(SchemaNode::new(kind));
        let min_sizes = min_encoded_sizes(&builder.nodes);
        Schema { nodes: builder.nodes, root, named: NameMap::default(), min_sizes }
    }

    /// The schema over `nodes` (indexed by [`SchemaId::from_index`]) rooted at `root`, without
    /// [`SchemaBuilder::finish`]'s validation and with [`Schema::min_encoded_size`] of every node as the caller
    /// states it: for the proof harnesses (`make kani`), whose arenas are concrete and well-formed by construction.
    /// The validation hashes field names with `RandomState`, whose seeding from the OS random source is out of CBMC's
    /// reach, and [`min_encoded_sizes`] recurses from inside its loops, which CBMC unrolls into minutes even for a
    /// handful of nodes (the harness `leaf_sizes` proves its leaf rule, the unit tests its compound rule). The
    /// harnesses back the vectors with arrays on their own stack: CBMC propagates the variant of a node it can see
    /// on the stack and prunes the arms of every `match` on it, while a node behind a heap allocation keeps every arm
    /// (and every recursive call) alive.
    #[cfg(kani)]
    pub fn unchecked(nodes: Vec<SchemaNode>, root: SchemaId, min_sizes: Vec<usize>) -> Schema {
        assert_eq!(min_sizes.len(), nodes.len());
        Schema { nodes, root, named: NameMap::default(), min_sizes }
    }

    pub fn root(&self) -> SchemaId {
        self.root
    }

    /// All nodes, indexable by [`SchemaId::index`].
    pub fn nodes(&self) -> &[SchemaNode] {
        &self.nodes
    }

    pub fn node(&self, id: SchemaId) -> &SchemaNode {
        &self.nodes[id.index()]
    }

    /// Follows a `Ref` to the node it names; any other id is returned unchanged.
    pub fn resolve(&self, id: SchemaId) -> SchemaId {
        match &self.nodes[id.index()].kind {
            SchemaKind::Ref { target, .. } => *target,
            _ => id,
        }
    }

    /// The node a `Ref` points to (or `id`'s own node).
    pub fn resolved(&self, id: SchemaId) -> &SchemaNode {
        self.node(self.resolve(id))
    }

    /// The defining node of the named type `fullname` (`namespace.name`, or the bare name without a namespace).
    pub fn named(&self, fullname: &str) -> Option<SchemaId> {
        self.named.get(fullname).copied()
    }

    /// The fewest bytes one value of node `id` occupies in the binary encoding: 0 for `null`, a `fixed` of size 0
    /// and a record of such fields only; at least 1 for everything else (a `boolean`, the varint of an `int`, `long`
    /// or `enum`, the length prefix of `bytes` and `string`, the index of a union, the end marker of an array or map).
    /// A decoder bounds the item count of an array or map block by it ([`crate::Decoder::read_block_count`]). The
    /// value is conservative, never above the true minimum: a recursive type counts as 0 where it refers back to
    /// itself, so a legitimate file never fails the bound.
    pub fn min_encoded_size(&self, id: SchemaId) -> usize {
        self.min_sizes[id.index()]
    }

    /// Full names of all named types with their defining nodes, in definition order.
    pub fn named_types(&self) -> Vec<(String, SchemaId)> {
        let mut all: Vec<(String, SchemaId)> = self.named.iter().map(|(n, id)| (n.clone(), *id)).collect();
        all.sort_by_key(|(_, id)| *id);
        all
    }

    /// The compact JSON serialization in the shape avro-c writes: named types fully defined at their first
    /// occurrence and referenced by name afterwards, `namespace` only where it differs from the enclosing one,
    /// non-null primitives in object form (`{"type":"int"}`), attributes in avro-c's order (`type`, `name`,
    /// `namespace`, structure, then `logicalType`, `precision`, `scale`, `adjust-to-utc`), fields as
    /// `{"name":..,"type":..,"field-id":..}`. Attributes avro-c drops (field defaults, enum defaults, unknown
    /// attributes) are appended after the modelled ones, so parsing the output reproduces this schema.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        let mut writer = JsonWriter { schema: self, defined: HashSet::new() };
        writer.write(self.root, None, &mut out);
        out
    }

    /// `to_json` of a subtree.
    pub fn to_json_at(&self, id: SchemaId) -> String {
        let mut out = String::new();
        let mut writer = JsonWriter { schema: self, defined: HashSet::new() };
        writer.write(id, None, &mut out);
        out
    }
}

/// Builds a [`Schema`] node by node; the writer side constructs schemas with it.
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    nodes: Vec<SchemaNode>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        SchemaBuilder { nodes: Vec::new() }
    }

    /// Adds a node and returns its id. Children must be added before their parent (the parent refers to them by
    /// id); a `Ref` may carry any `target`, [`SchemaBuilder::finish`] resolves it by name.
    pub fn add(&mut self, node: SchemaNode) -> SchemaId {
        self.nodes.push(node);
        SchemaId((self.nodes.len() - 1) as u32)
    }

    /// Adds a node of `kind` without attributes.
    pub fn add_kind(&mut self, kind: SchemaKind) -> SchemaId {
        self.add(SchemaNode::new(kind))
    }

    /// A node added before (a builder-side caller deciding on a node's kind, e.g. whether it is already `null`).
    pub fn node(&self, id: SchemaId) -> &SchemaNode {
        &self.nodes[id.index()]
    }

    /// Adds a reference to the named type `name` (resolved by [`SchemaBuilder::finish`]).
    pub fn add_ref(&mut self, name: Name) -> SchemaId {
        self.add(SchemaNode::new(SchemaKind::Ref { name, target: SchemaId(u32::MAX) }))
    }

    /// Validates the arena (child ids in range, named types unique, every reference resolvable) and returns the
    /// schema rooted at `root`.
    pub fn finish(mut self, root: SchemaId) -> Result<Schema> {
        let len = self.nodes.len();
        if len > u32::MAX as usize {
            return Err(Error::invalid_input("schema has too many nodes"));
        }
        let check = |id: SchemaId| -> Result<()> {
            if id.index() >= len {
                return Err(Error::invalid_input(format!("schema node id {} out of range", id.0)));
            }
            Ok(())
        };
        check(root)?;
        let mut named = NameMap::default();
        for (index, node) in self.nodes.iter().enumerate() {
            match &node.kind {
                SchemaKind::Record { name, fields, .. } => {
                    let mut seen = HashSet::new();
                    for field in fields {
                        check(field.ty)?;
                        if !seen.insert(field.name.as_str()) {
                            return Err(Error::invalid_input(format!(
                                "duplicate field name '{}' in record '{}'",
                                field.name, name.name
                            )));
                        }
                    }
                }
                SchemaKind::Array { items, .. } => check(*items)?,
                SchemaKind::Map { values, .. } => check(*values)?,
                SchemaKind::Union { branches } => {
                    for branch in branches {
                        check(*branch)?;
                    }
                }
                _ => {}
            }
            if let Some(name) = node.kind.name().filter(|_| node.kind.is_definition()) {
                let fullname = name.fullname();
                if named.insert(fullname.clone(), SchemaId(index as u32)).is_some() {
                    return Err(Error::invalid_input(format!(
                        "Avro schema by the name of '{fullname}' already exists, names of 'record', 'enum' and \
                         'fixed' types have to be distinct"
                    )));
                }
            }
        }
        for node in &mut self.nodes {
            if let SchemaKind::Ref { name, target } = &mut node.kind {
                let fullname = name.fullname();
                let resolved = named
                    .get(&fullname)
                    .or_else(|| name.namespace.as_ref().and(named.get(&name.name)))
                    .copied()
                    .ok_or_else(|| Error::invalid_input(format!("No schema type named {fullname}")))?;
                *target = resolved;
            }
        }
        let min_sizes = min_encoded_sizes(&self.nodes);
        Ok(Schema { nodes: self.nodes, root, named, min_sizes })
    }
}

/// [`Schema::min_encoded_size`] of every node of a validated arena (references resolved).
fn min_encoded_sizes(nodes: &[SchemaNode]) -> Vec<usize> {
    #[derive(Clone, Copy)]
    enum Memo {
        Unknown,
        /// On the current path: a recursive type, counted as 0.
        InProgress,
        Known(usize),
    }
    fn size(nodes: &[SchemaNode], memo: &mut [Memo], id: SchemaId) -> usize {
        let index = id.index();
        match memo[index] {
            Memo::Known(size) => return size,
            Memo::InProgress => return 0,
            Memo::Unknown => memo[index] = Memo::InProgress,
        }
        let result = match &nodes[index].kind {
            SchemaKind::Null => 0,
            SchemaKind::Boolean
            | SchemaKind::Int
            | SchemaKind::Long
            | SchemaKind::Bytes
            | SchemaKind::String
            | SchemaKind::Enum { .. }
            | SchemaKind::Array { .. }
            | SchemaKind::Map { .. } => 1,
            SchemaKind::Float => 4,
            SchemaKind::Double => 8,
            SchemaKind::Fixed { size, .. } => *size,
            SchemaKind::Record { fields, .. } => {
                fields.iter().fold(0usize, |sum, field| sum.saturating_add(size(nodes, memo, field.ty)))
            }
            SchemaKind::Union { branches } => {
                1 + branches.iter().map(|branch| size(nodes, memo, *branch)).min().unwrap_or(0)
            }
            SchemaKind::Ref { target, .. } => size(nodes, memo, *target),
        };
        memo[index] = Memo::Known(result);
        result
    }
    let mut memo = vec![Memo::Unknown; nodes.len()];
    (0..nodes.len()).map(|index| size(nodes, &mut memo, SchemaId(index as u32))).collect()
}

/// Keys of a schema object that are structure or modelled attributes rather than `extra` ones.
const COMMON_KEYS: &[&str] = &["type", "logicalType", "precision", "scale", "adjust-to-utc"];

struct ParseState {
    builder: SchemaBuilder,
    /// Full name -> defining node, for resolving references while parsing.
    named: HashMap<String, SchemaId>,
}

impl ParseState {
    fn node(&mut self, json: &Json, enclosing_ns: Option<&str>, depth: usize) -> Result<SchemaId> {
        if depth > MAX_DEPTH {
            return Err(Error::invalid_input("Avro schema nesting too deep"));
        }
        match json {
            Json::Array(branches) => {
                let mut ids = Vec::with_capacity(branches.len());
                for branch in branches {
                    ids.push(self.node(branch, enclosing_ns, depth + 1)?);
                }
                Ok(self.builder.add_kind(SchemaKind::Union { branches: ids }))
            }
            Json::String(type_name) => {
                let node = self.by_name(type_name, enclosing_ns)?;
                Ok(self.builder.add(node))
            }
            Json::Object(members) => {
                let type_json =
                    json.get("type").ok_or_else(|| Error::invalid_input("Avro schema object lacks \"type\""))?;
                let type_name = match type_json {
                    Json::String(s) => s.as_str(),
                    // `{"type": {...}}` / `{"type": [...]}`: the inner schema with the outer attributes added
                    other => {
                        let id = self.node(other, enclosing_ns, depth + 1)?;
                        let extra: Vec<(String, Json)> = members.iter().filter(|(k, _)| k != "type").cloned().collect();
                        self.builder.nodes[id.index()].extra.extend(extra);
                        return Ok(id);
                    }
                };
                let mut node = match type_name {
                    "record" => return self.record(json, enclosing_ns, depth, false),
                    "error" => return self.record(json, enclosing_ns, depth, true),
                    "enum" => self.enum_(json, enclosing_ns)?,
                    "fixed" => self.fixed(json, enclosing_ns)?,
                    "array" => {
                        let items_json =
                            json.get("items").ok_or_else(|| Error::invalid_input("Array type must have \"items\""))?;
                        let items = self.node(items_json, enclosing_ns, depth + 1)?;
                        let element_id = int_attr(json, "element-id")?;
                        let mut node = SchemaNode::new(SchemaKind::Array { items, element_id });
                        node.extra = extras(members, &["items", "element-id"]);
                        node
                    }
                    "map" => {
                        let values_json =
                            json.get("values").ok_or_else(|| Error::invalid_input("Map type must have \"values\""))?;
                        let values = self.node(values_json, enclosing_ns, depth + 1)?;
                        let key_id = int_attr(json, "key-id")?;
                        let value_id = int_attr(json, "value-id")?;
                        let mut node = SchemaNode::new(SchemaKind::Map { values, key_id, value_id });
                        node.extra = extras(members, &["values", "key-id", "value-id"]);
                        node
                    }
                    _ => {
                        let mut node = self.by_name(type_name, enclosing_ns)?;
                        node.extra = extras(members, &[]);
                        node
                    }
                };
                apply_common_attrs(&mut node, json)?;
                Ok(self.builder.add(node))
            }
            _ => Err(Error::invalid_input("Avro schema must be a string, an object or an array")),
        }
    }

    /// A primitive by name, or a reference to a named type.
    fn by_name(&self, type_name: &str, enclosing_ns: Option<&str>) -> Result<SchemaNode> {
        let kind = match type_name {
            "null" => SchemaKind::Null,
            "boolean" => SchemaKind::Boolean,
            "int" => SchemaKind::Int,
            "long" => SchemaKind::Long,
            "float" => SchemaKind::Float,
            "double" => SchemaKind::Double,
            "bytes" => SchemaKind::Bytes,
            "string" => SchemaKind::String,
            _ => {
                let name = Name::parse(type_name, enclosing_ns);
                let target = self
                    .named
                    .get(&name.fullname())
                    .or_else(|| self.named.get(&name.name))
                    .copied()
                    .ok_or_else(|| Error::invalid_input(format!("No schema type named {type_name}")))?;
                // the reference carries the definition's name so `fullname` lookups agree
                let name = self.builder.nodes[target.index()].kind.name().cloned().unwrap_or(name);
                SchemaKind::Ref { name, target }
            }
        };
        Ok(SchemaNode::new(kind))
    }

    fn define(&mut self, json: &Json, enclosing_ns: Option<&str>, what: &str) -> Result<Name> {
        let name_text = json
            .get("name")
            .and_then(Json::as_str)
            .ok_or_else(|| Error::invalid_input(format!("{what} type must have a \"name\"")))?;
        if name_text.is_empty() {
            return Err(Error::invalid_input(format!("{what} type has an empty \"name\"")));
        }
        let name = if name_text.contains('.') {
            Name::parse(name_text, None)
        } else if let Some(ns) = json.get("namespace").and_then(Json::as_str) {
            Name::new(name_text, Some(ns))
        } else {
            Name::new(name_text, enclosing_ns)
        };
        let fullname = name.fullname();
        if self.named.contains_key(&fullname) {
            return Err(Error::invalid_input(format!(
                "Avro schema by the name of '{fullname}' already exists, names of 'record', 'enum' and 'fixed' \
                 types have to be distinct"
            )));
        }
        // the definition's id is the next node added; reserve it so recursive references resolve
        self.named.insert(fullname, SchemaId(self.builder.nodes.len() as u32));
        Ok(name)
    }

    fn record(&mut self, json: &Json, enclosing_ns: Option<&str>, depth: usize, is_error: bool) -> Result<SchemaId> {
        let members = json.as_object().unwrap_or(&[]);
        let name = self.define(json, enclosing_ns, if is_error { "Error" } else { "Record" })?;
        let fields_json = json
            .get("fields")
            .and_then(Json::as_array)
            .ok_or_else(|| Error::invalid_input("Record type must have \"fields\""))?;
        // the record's node must sit at the reserved id: add a placeholder, fill in the fields afterwards
        let mut node = SchemaNode::new(SchemaKind::Record { name: name.clone(), fields: Vec::new(), is_error });
        node.extra = extras(members, &["name", "namespace", "fields"]);
        apply_common_attrs(&mut node, json)?;
        let id = self.builder.add(node);
        let mut fields = Vec::with_capacity(fields_json.len());
        let mut seen = HashSet::new();
        for (index, field_json) in fields_json.iter().enumerate() {
            let field_members = field_json
                .as_object()
                .ok_or_else(|| Error::invalid_input(format!("Record field {index} must be an object")))?;
            let field_name = field_json
                .get("name")
                .and_then(Json::as_str)
                .ok_or_else(|| Error::invalid_input(format!("Record field {index} must have a \"name\"")))?;
            if field_name.is_empty() {
                return Err(Error::invalid_input("Empty avro field name"));
            }
            if !seen.insert(field_name) {
                return Err(Error::invalid_input(format!(
                    "duplicate field name '{field_name}' in record '{}'",
                    name.name
                )));
            }
            let type_json = field_json
                .get("type")
                .ok_or_else(|| Error::invalid_input(format!("Record field {index} must have a \"type\"")))?;
            let ty = self.node(type_json, name.namespace.as_deref(), depth + 1)?;
            fields.push(Field {
                name: field_name.to_owned(),
                ty,
                default: field_json.get("default").cloned(),
                field_id: int_attr(field_json, "field-id")?,
                extra: extras(field_members, &["name", "type", "default", "field-id"]),
            });
        }
        if let SchemaKind::Record { fields: slot, .. } = &mut self.builder.nodes[id.index()].kind {
            *slot = fields;
        }
        Ok(id)
    }

    fn enum_(&mut self, json: &Json, enclosing_ns: Option<&str>) -> Result<SchemaNode> {
        let members = json.as_object().unwrap_or(&[]);
        let name = self.define(json, enclosing_ns, "Enum")?;
        let symbols_json = json
            .get("symbols")
            .and_then(Json::as_array)
            .ok_or_else(|| Error::invalid_input("Enum type must have \"symbols\""))?;
        let mut symbols = Vec::with_capacity(symbols_json.len());
        let mut seen = HashSet::new();
        for (index, symbol) in symbols_json.iter().enumerate() {
            let symbol =
                symbol.as_str().ok_or_else(|| Error::invalid_input(format!("Enum symbol {index} must be a string")))?;
            if !seen.insert(symbol) {
                return Err(Error::invalid_input(format!("duplicate enum symbol '{symbol}' in '{}'", name.name)));
            }
            symbols.push(symbol.to_owned());
        }
        let default = json.get("default").and_then(Json::as_str).map(str::to_owned);
        let mut node = SchemaNode::new(SchemaKind::Enum { name, symbols, default });
        node.extra = extras(members, &["name", "namespace", "symbols", "default"]);
        Ok(node)
    }

    fn fixed(&mut self, json: &Json, enclosing_ns: Option<&str>) -> Result<SchemaNode> {
        let members = json.as_object().unwrap_or(&[]);
        let name = self.define(json, enclosing_ns, "Fixed")?;
        let size = json
            .get("size")
            .and_then(Json::as_u64)
            .and_then(|s| usize::try_from(s).ok())
            .ok_or_else(|| Error::invalid_input("Fixed type must have a non-negative integer \"size\""))?;
        let mut node = SchemaNode::new(SchemaKind::Fixed { name, size });
        node.extra = extras(members, &["name", "namespace", "size"]);
        Ok(node)
    }
}

/// An integer attribute; a present attribute of another type is an error.
fn int_attr(json: &Json, key: &str) -> Result<Option<i64>> {
    match json.get(key) {
        None | Some(Json::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .map(Some)
            .ok_or_else(|| Error::invalid_input(format!("Avro attribute \"{key}\" must be an integer"))),
    }
}

/// The logical type attributes. The spec asks a reader to ignore an invalid logical type and use the underlying
/// type, so an attribute of the wrong JSON type (Hive 0.x wrote `"precision": "5"`) is not an error: it is kept
/// verbatim among the `extra` attributes (so `to_json` reproduces it) and the modelled slot stays empty, which the
/// consumer treats as the attribute being absent.
fn apply_common_attrs(node: &mut SchemaNode, json: &Json) -> Result<()> {
    if let Some(lt) = json.get("logicalType") {
        match lt.as_str() {
            Some(lt) => node.logical_type = Some(lt.to_owned()),
            None => node.extra.push(("logicalType".to_owned(), lt.clone())),
        }
    }
    for (key, slot) in [("precision", &mut node.precision), ("scale", &mut node.scale)] {
        match json.get(key) {
            None | Some(Json::Null) => {}
            Some(v) => match v.as_i64() {
                Some(i) => *slot = Some(i),
                None => node.extra.push((key.to_owned(), v.clone())),
            },
        }
    }
    if let Some(utc) = json.get("adjust-to-utc") {
        match utc.as_bool() {
            Some(utc) => node.adjust_to_utc = Some(utc),
            None => node.extra.push(("adjust-to-utc".to_owned(), utc.clone())),
        }
    }
    Ok(())
}

/// The members that are neither common nor in `structural`.
fn extras(members: &[(String, Json)], structural: &[&str]) -> Vec<(String, Json)> {
    members
        .iter()
        .filter(|(k, _)| !COMMON_KEYS.contains(&k.as_str()) && !structural.contains(&k.as_str()))
        .cloned()
        .collect()
}

struct JsonWriter<'a> {
    schema: &'a Schema,
    /// Full names already written in full.
    defined: HashSet<String>,
}

impl JsonWriter<'_> {
    fn name_prefix(&self, name: &Name, parent_ns: Option<&str>, out: &mut String) {
        out.push_str(",\"name\":");
        json::write_string(&name.name, out);
        if name.namespace.as_deref() != parent_ns {
            out.push_str(",\"namespace\":");
            json::write_string(name.namespace.as_deref().unwrap_or(""), out);
        }
    }

    fn attrs(&self, node: &SchemaNode, out: &mut String) {
        if let Some(lt) = &node.logical_type {
            out.push_str(",\"logicalType\":");
            json::write_string(lt, out);
        }
        if let Some(p) = node.precision {
            let _ = write!(out, ",\"precision\":{p}");
        }
        if let Some(s) = node.scale {
            let _ = write!(out, ",\"scale\":{s}");
        }
        if let Some(utc) = node.adjust_to_utc {
            let _ = write!(out, ",\"adjust-to-utc\":{utc}");
        }
        extras_json(&node.extra, out);
    }

    fn write(&mut self, id: SchemaId, parent_ns: Option<&str>, out: &mut String) {
        let node = self.schema.node(id);
        match &node.kind {
            SchemaKind::Null if node.logical_type.is_none() && node.extra.is_empty() => out.push_str("\"null\""),
            kind if kind.is_primitive() => {
                let _ = write!(out, "{{\"type\":\"{}\"", kind.type_name());
                self.attrs(node, out);
                out.push('}');
            }
            SchemaKind::Record { name, fields, is_error } => {
                if !self.defined.insert(name.fullname()) {
                    return self.reference(name, parent_ns, out);
                }
                out.push_str(if *is_error { "{\"type\":\"error\"" } else { "{\"type\":\"record\"" });
                self.name_prefix(name, parent_ns, out);
                out.push_str(",\"fields\":[");
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"name\":");
                    json::write_string(&field.name, out);
                    out.push_str(",\"type\":");
                    self.write(field.ty, name.namespace.as_deref(), out);
                    if let Some(field_id) = field.field_id {
                        let _ = write!(out, ",\"field-id\":{field_id}");
                    }
                    if let Some(default) = &field.default {
                        out.push_str(",\"default\":");
                        default.write_to(out);
                    }
                    extras_json(&field.extra, out);
                    out.push('}');
                }
                out.push(']');
                self.attrs(node, out);
                out.push('}');
            }
            SchemaKind::Enum { name, symbols, default } => {
                if !self.defined.insert(name.fullname()) {
                    return self.reference(name, parent_ns, out);
                }
                out.push_str("{\"type\":\"enum\"");
                self.name_prefix(name, parent_ns, out);
                out.push_str(",\"symbols\":[");
                for (i, symbol) in symbols.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    json::write_string(symbol, out);
                }
                out.push(']');
                if let Some(default) = default {
                    out.push_str(",\"default\":");
                    json::write_string(default, out);
                }
                self.attrs(node, out);
                out.push('}');
            }
            SchemaKind::Fixed { name, size } => {
                if !self.defined.insert(name.fullname()) {
                    return self.reference(name, parent_ns, out);
                }
                out.push_str("{\"type\":\"fixed\"");
                self.name_prefix(name, parent_ns, out);
                let _ = write!(out, ",\"size\":{size}");
                self.attrs(node, out);
                out.push('}');
            }
            SchemaKind::Array { items, element_id } => {
                out.push_str("{\"type\":\"array\"");
                if let Some(element_id) = element_id {
                    let _ = write!(out, ",\"element-id\":{element_id}");
                }
                // avro-c writes an array's logicalType before its items
                if let Some(lt) = &node.logical_type {
                    out.push_str(",\"logicalType\":");
                    json::write_string(lt, out);
                }
                out.push_str(",\"items\":");
                self.write(*items, parent_ns, out);
                let mut rest = node.clone();
                rest.logical_type = None;
                self.attrs(&rest, out);
                out.push('}');
            }
            SchemaKind::Map { values, key_id, value_id } => {
                out.push_str("{\"type\":\"map\"");
                if let Some(key_id) = key_id {
                    let _ = write!(out, ",\"key-id\":{key_id}");
                }
                if let Some(value_id) = value_id {
                    let _ = write!(out, ",\"value-id\":{value_id}");
                }
                out.push_str(",\"values\":");
                self.write(*values, parent_ns, out);
                self.attrs(node, out);
                out.push('}');
            }
            SchemaKind::Union { branches } => {
                out.push('[');
                for (i, branch) in branches.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    self.write(*branch, parent_ns, out);
                }
                out.push(']');
            }
            SchemaKind::Ref { name, target } => {
                if self.defined.contains(&name.fullname()) {
                    self.reference(name, parent_ns, out);
                } else {
                    // the first occurrence of the definition is a Ref (a builder-made schema): define it here
                    self.write(*target, parent_ns, out);
                }
            }
            // `Null` with attributes (`{"type":"null",...}`)
            SchemaKind::Null => {
                out.push_str("{\"type\":\"null\"");
                self.attrs(node, out);
                out.push('}');
            }
            _ => {}
        }
    }

    /// A use of an already written named type: qualified when its namespace differs from the enclosing one.
    fn reference(&self, name: &Name, parent_ns: Option<&str>, out: &mut String) {
        match &name.namespace {
            Some(ns) if Some(ns.as_str()) != parent_ns => json::write_string(&name.fullname(), out),
            _ => json::write_string(&name.name, out),
        }
    }
}

fn extras_json(extra: &[(String, Json)], out: &mut String) {
    for (k, v) in extra {
        out.push(',');
        json::write_string(k, out);
        out.push(':');
        v.write_to(out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avro_c_shape() {
        // the schema the DuckDB COPY writer produces for a decimal, a timestamptz and an Iceberg-style record
        let text = r#"{"type":"record","name":"root","fields":[
            {"name":"d","type":["null",{"type":"fixed","name":"fixed0","size":9,"logicalType":"decimal","precision":19,"scale":4}]},
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}},
            {"name":"n","type":"null"},
            {"name":"r","type":{"type":"record","name":"r102","fields":[{"name":"v","type":{"type":"int"},"field-id":1000}]},"field-id":102},
            {"name":"m","type":{"type":"array","logicalType":"map","element-id":5,"items":{"type":"record","name":"k1_v2","fields":[{"name":"key","type":{"type":"string"}},{"name":"value","type":{"type":"map","key-id":7,"value-id":8,"values":"r102"}}]}}},
            {"name":"again","type":"fixed0"}]}"#;
        let schema = Schema::parse(text).unwrap();
        let expected = concat!(
            r#"{"type":"record","name":"root","fields":["#,
            r#"{"name":"d","type":["null",{"type":"fixed","name":"fixed0","size":9,"logicalType":"decimal","precision":19,"scale":4}]},"#,
            r#"{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}},"#,
            r#"{"name":"n","type":"null"},"#,
            r#"{"name":"r","type":{"type":"record","name":"r102","fields":[{"name":"v","type":{"type":"int"},"field-id":1000}]},"field-id":102},"#,
            r#"{"name":"m","type":{"type":"array","element-id":5,"logicalType":"map","items":{"type":"record","name":"k1_v2","fields":[{"name":"key","type":{"type":"string"}},{"name":"value","type":{"type":"map","key-id":7,"value-id":8,"values":"r102"}}]}}},"#,
            r#"{"name":"again","type":"fixed0"}]}"#
        );
        assert_eq!(schema.to_json(), expected);
        // the output parses back to the same schema
        assert_eq!(Schema::parse(&schema.to_json()).unwrap().to_json(), expected);
        assert_eq!(schema.named("fixed0").map(|id| schema[id].fixed_size()), Some(Some(9)));
    }

    #[test]
    fn namespaces_and_references() {
        let text = r#"{"type":"record","name":"a.b.Outer","fields":[
            {"name":"x","type":{"type":"record","name":"Inner","fields":[{"name":"back","type":["null","Outer"]}]}},
            {"name":"y","type":"a.b.Inner"},
            {"name":"z","type":{"type":"enum","name":"E","namespace":"","symbols":["A","B"],"default":"A"}},
            {"name":"w","type":"E"}]}"#;
        let schema = Schema::parse(text).unwrap();
        assert!(schema.named("a.b.Outer").is_some());
        assert!(schema.named("a.b.Inner").is_some());
        assert!(schema.named("E").is_some());
        let expected = concat!(
            r#"{"type":"record","name":"Outer","namespace":"a.b","fields":["#,
            r#"{"name":"x","type":{"type":"record","name":"Inner","fields":[{"name":"back","type":["null","Outer"]}]}},"#,
            r#"{"name":"y","type":"Inner"},"#,
            r#"{"name":"z","type":{"type":"enum","name":"E","namespace":"","symbols":["A","B"],"default":"A"}},"#,
            r#"{"name":"w","type":"E"}]}"#
        );
        assert_eq!(schema.to_json(), expected);
        assert_eq!(Schema::parse(expected).unwrap().to_json(), expected);
        // recursion is representable: the Ref points back at the root
        let root = schema.root();
        if let SchemaKind::Record { fields, .. } = &schema[root].kind
            && let SchemaKind::Record { fields: inner, .. } = &schema[fields[0].ty].kind
            && let SchemaKind::Union { branches } = &schema[inner[0].ty].kind
        {
            assert_eq!(schema.resolve(branches[1]), root);
            return;
        }
        panic!("unexpected structure");
    }

    #[test]
    fn error_is_a_record() {
        // Java's Schema.parse accepts the protocol error record wherever a record is allowed
        let text = r#"{"type":"record","name":"r","fields":[{"name":"e","type":{"type":"error","name":"E","fields":[{"name":"msg","type":"string"}]}},{"name":"again","type":"E"}]}"#;
        let schema = Schema::parse(text).unwrap();
        let e = schema.named("E").unwrap();
        assert!(matches!(schema[e].kind, SchemaKind::Record { is_error: true, .. }));
        assert_eq!(schema[e].kind.type_name(), "error");
        assert_eq!(
            schema.to_json(),
            r#"{"type":"record","name":"r","fields":[{"name":"e","type":{"type":"error","name":"E","fields":[{"name":"msg","type":{"type":"string"}}]}},{"name":"again","type":"E"}]}"#
        );
        assert_eq!(Schema::parse(&schema.to_json()).unwrap(), schema);
    }

    #[test]
    fn invalid_logical_type_attributes_are_kept_verbatim() {
        // Hive 0.x: precision and scale as strings; a numeric logicalType. Neither is modelled, both survive to_json
        let text = r#"{"type":"record","name":"r","fields":[{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":"5","scale":"2"}},{"name":"x","type":{"type":"long","logicalType":5}}]}"#;
        let schema = Schema::parse(text).unwrap();
        let d = match &schema[schema.root()].kind {
            SchemaKind::Record { fields, .. } => &schema[fields[0].ty],
            _ => panic!("record"),
        };
        assert_eq!(d.logical_type.as_deref(), Some("decimal"));
        assert_eq!((d.precision, d.scale), (None, None));
        assert_eq!(
            schema.to_json(),
            r#"{"type":"record","name":"r","fields":[{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":"5","scale":"2"}},{"name":"x","type":{"type":"long","logicalType":5}}]}"#
        );
        assert_eq!(Schema::parse(&schema.to_json()).unwrap(), schema);
    }

    #[test]
    fn rejects_bad_schemas() {
        for bad in [
            r#"{"type":"record","name":"r"}"#,
            r#"{"type":"record","fields":[]}"#,
            r#"{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"a","type":"int"}]}"#,
            r#"{"type":"record","name":"r","fields":[{"name":"a","type":"Nope"}]}"#,
            r#"[{"type":"fixed","name":"f","size":1},{"type":"fixed","name":"f","size":1}]"#,
            r#"{"type":"fixed","name":"f","size":-1}"#,
            r#"{"type":"enum","name":"e"}"#,
            r#"{"type":"array"}"#,
            r#"{"type":"map"}"#,
            r#"42"#,
            r#"{"type":"record","name":"r","fields":[{"name":"","type":"int"}]}"#,
        ] {
            assert!(Schema::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn builder() {
        let mut b = SchemaBuilder::new();
        let int = b.add_kind(SchemaKind::Int);
        let rec = b.add_kind(SchemaKind::Record {
            name: Name::simple("node"),
            fields: vec![Field::new("v", int).with_field_id(1)],
            is_error: false,
        });
        let again = b.add_ref(Name::simple("node"));
        let root = b.add_kind(SchemaKind::Record {
            name: Name::simple("root"),
            fields: vec![Field::new("a", rec), Field::new("b", again)],
            is_error: false,
        });
        let schema = b.finish(root).unwrap();
        assert_eq!(
            schema.to_json(),
            r#"{"type":"record","name":"root","fields":[{"name":"a","type":{"type":"record","name":"node","fields":[{"name":"v","type":{"type":"int"},"field-id":1}]}},{"name":"b","type":"node"}]}"#
        );
    }

    /// The compound part of the `min_encoded_size` rule (the leaf part is proven by the Kani harness `leaf_sizes`):
    /// a record is the sum of its fields (0 for the empty record and for zero-width fields only), a union 1 plus its
    /// smallest branch, an array or map 1, a reference its target, and a type on a recursion cycle 0 where it refers
    /// back to itself.
    #[test]
    fn min_encoded_sizes_of_compound_types() {
        let mut b = SchemaBuilder::new();
        let null = b.add_kind(SchemaKind::Null);
        let boolean = b.add_kind(SchemaKind::Boolean);
        let double = b.add_kind(SchemaKind::Double);
        let fixed0 = b.add_kind(SchemaKind::Fixed { name: Name::simple("f0"), size: 0 });
        let fixed3 = b.add_kind(SchemaKind::Fixed { name: Name::simple("f3"), size: 3 });
        let empty = b.add_kind(SchemaKind::Record { name: Name::simple("empty"), fields: vec![], is_error: false });
        let zero_width = b.add_kind(SchemaKind::Record {
            name: Name::simple("zero_width"),
            fields: vec![Field::new("n", null), Field::new("f", fixed0), Field::new("e", empty)],
            is_error: false,
        });
        let pair = b.add_kind(SchemaKind::Record {
            name: Name::simple("pair"),
            fields: vec![Field::new("b", boolean), Field::new("d", double), Field::new("z", zero_width)],
            is_error: false,
        });
        let union_with_null = b.add_kind(SchemaKind::Union { branches: vec![null, double] });
        let union_of_sized = b.add_kind(SchemaKind::Union { branches: vec![fixed3, pair] });
        let array = b.add_kind(SchemaKind::Array { items: pair, element_id: None });
        let map = b.add_kind(SchemaKind::Map { values: null, key_id: None, value_id: None });
        let again = b.add_ref(Name::simple("pair"));
        let root = b.add_kind(SchemaKind::Record {
            name: Name::simple("root"),
            fields: vec![Field::new("u", union_with_null), Field::new("l", array), Field::new("p", again)],
            is_error: false,
        });
        let schema = b.finish(root).unwrap();
        assert_eq!(schema.min_encoded_size(empty), 0);
        assert_eq!(schema.min_encoded_size(zero_width), 0);
        assert_eq!(schema.min_encoded_size(pair), 9);
        assert_eq!(schema.min_encoded_size(union_with_null), 1);
        assert_eq!(schema.min_encoded_size(union_of_sized), 4);
        assert_eq!(schema.min_encoded_size(array), 1);
        assert_eq!(schema.min_encoded_size(map), 1);
        assert_eq!(schema.min_encoded_size(again), 9);
        assert_eq!(schema.min_encoded_size(root), 1 + 1 + 9);

        // a linked list: the node refers back to itself through a union; the self reference counts as 0, so the node
        // is its int (1) plus the union's index (1), and it must not loop
        let list = Schema::parse(
            r#"{"type":"record","name":"node","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","node"]}]}"#,
        )
        .unwrap();
        assert_eq!(list.min_encoded_size(list.root()), 2);
        let SchemaKind::Record { fields, .. } = &list[list.root()].kind else { panic!() };
        assert_eq!(list.min_encoded_size(fields[1].ty), 1);
    }
}

/// Kani proof harness (`cargo kani -p avro-core`, `make kani`): [`Schema::min_encoded_size`] of every leaf kind.
///
/// The rule: the size is 0 exactly for `null`, a `fixed` of size 0, a record whose fields are all zero-width and a
/// reference to such a type; a union is `1 + min(branches)`, an array or map is 1 (the end marker), the other
/// primitives their fixed width or their 1-byte minimum varint or length prefix. The harness proves the leaf part of
/// it; the compound part (records, unions, arrays, maps, references, recursion) is beyond bounded model checking of
/// [`min_encoded_sizes`] as written: CBMC inlines a recursive function once per call site per unwinding, and `size`
/// recurses from inside its field and branch loops, so an arena three levels deep needs thousands of copies. The
/// compound rule is covered by the unit tests instead.
///
/// The harness takes the size from [`min_encoded_sizes`] over a one-node stack array, the table
/// [`Schema::min_encoded_size`] indexes ([`SchemaBuilder::finish`] only adds the name validation, a `HashMap` whose
/// hashing is out of CBMC's reach). The kind is never a symbolic value (reading the `fields` of a node whose variant
/// is symbolic makes CBMC dereference a pointer that may be anything): [`for_any_leaf`] branches on a symbolic
/// selector and runs the check once per kind with the kind concrete, the size of a `fixed` being the symbolic datum.
/// The node is forgotten rather than dropped (its drop glue, recursive through `Json`, is no part of the property).
#[cfg(kani)]
mod verification {
    use super::*;

    /// Runs `check(kind, size)` for a leaf kind chosen by a symbolic selector, `size` being the minimum encoded size
    /// the rule above gives the kind; a `fixed` has a symbolic size `0..=32`. Each arm is a separate, concrete copy of
    /// the check.
    fn for_any_leaf(check: impl Fn(SchemaKind, usize)) {
        let selector: u8 = kani::any();
        match selector {
            0 => check(SchemaKind::Null, 0),
            1 => check(SchemaKind::Boolean, 1),
            2 => check(SchemaKind::Int, 1),
            3 => check(SchemaKind::Long, 1),
            4 => check(SchemaKind::Float, 4),
            5 => check(SchemaKind::Double, 8),
            6 => check(SchemaKind::Bytes, 1),
            7 => check(SchemaKind::String, 1),
            8 => check(SchemaKind::Enum { name: Name::simple(""), symbols: Vec::new(), default: None }, 1),
            9 => {
                let size: usize = kani::any();
                kani::assume(size <= 32);
                check(SchemaKind::Fixed { name: Name::simple(""), size }, size);
            }
            _ => {}
        }
    }

    /// Every primitive, enum and fixed leaf has the minimum encoded byte count the rule gives it, and it is 0 exactly
    /// for `null` and `fixed(0)`. Bound: one node (unwind 2: a leaf recurses no further).
    #[kani::proof]
    #[kani::unwind(2)]
    fn leaf_sizes() {
        for_any_leaf(|kind, expected| {
            let zero_width = matches!(kind, SchemaKind::Null | SchemaKind::Fixed { size: 0, .. });
            let nodes = [SchemaNode::new(kind)];
            let sizes = min_encoded_sizes(&nodes);
            assert_eq!(sizes[0], expected);
            assert_eq!(sizes[0] == 0, zero_width);
            std::mem::forget(nodes);
        });
    }

    /// `Name::parse` on every string of at most 12 bytes with each default namespace (none, empty, `ns`): the name
    /// is what follows the last `.`, the namespace what precedes it (the default namespace only without a dot; an
    /// empty namespace is none), and `fullname` reproduces the text exactly unless its only dot leads (`.a` parses to
    /// the name `a` without a namespace and prints as `a`). About four minutes of CBMC: behind the `kani-heavy`
    /// feature, which `make kani` enables and the CI job does not.
    #[cfg(feature = "kani-heavy")]
    #[kani::proof]
    #[kani::unwind(18)]
    fn name_parse_splits_at_the_last_dot_and_fullname_inverts_it() {
        let buf: [u8; 12] = kani::any();
        let text = crate::kani_support::any_str(&buf);
        let which: u8 = kani::any();
        let default = match which {
            0 => None,
            1 => Some(""),
            _ => Some("ns"),
        };
        let bytes = text.as_bytes();
        let mut last_dot = None;
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'.' {
                last_dot = Some(i);
            }
            i += 1;
        }
        let name = Name::parse(text, default);
        match last_dot {
            None => {
                assert_eq!(name.name, text);
                assert_eq!(name.namespace.as_deref(), default.filter(|ns| !ns.is_empty()));
            }
            Some(dot) => {
                assert_eq!(name.name.as_bytes(), &bytes[dot + 1..]);
                assert_eq!(name.namespace.as_deref().map(str::as_bytes), (dot > 0).then_some(&bytes[..dot]));
            }
        }
        let full = name.fullname();
        let leading_only_dot = last_dot == Some(0);
        match (last_dot, default) {
            (None, Some(ns)) if !ns.is_empty() => assert_eq!(full.as_bytes(), [ns.as_bytes(), b".", bytes].concat()),
            _ => assert_eq!(full.as_bytes(), if leading_only_dot { &bytes[1..] } else { bytes }),
        }
    }
}
