//! A small JSON reader and compact writer, standard library only.
//!
//! Objects keep their keys in insertion order (the Avro schema JSON an object container stores is reproduced in the
//! order it was written), integers stay exact (`i64`, then `u64`, then `f64`), and the writer emits the compact form
//! without any whitespace, like jansson's `JSON_COMPACT | JSON_PRESERVE_ORDER` used by avro-c.

use std::fmt::Write as _;

use crate::error::{Error, Result};

/// Nesting depth after which parsing stops, so hostile input cannot overflow the stack.
const MAX_DEPTH: usize = 512;

/// A JSON value.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// An integer literal that fits an `i64`.
    Int(i64),
    /// A positive integer literal above `i64::MAX` that fits a `u64`.
    UInt(u64),
    /// Any other number literal (fraction, exponent, or out of the 64-bit integer range).
    Float(f64),
    String(String),
    Array(Vec<Json>),
    /// An object; keys in insertion order, duplicates kept as written.
    Object(Vec<(String, Json)>),
}

impl Json {
    /// Parses a complete JSON document (trailing whitespace allowed, nothing else).
    pub fn parse(text: &str) -> Result<Json> {
        let mut p = Parser { s: text.as_bytes(), pos: 0 };
        let v = p.value(0)?;
        p.skip_ws();
        if p.pos != p.s.len() {
            return Err(p.err("trailing characters after the JSON value"));
        }
        Ok(v)
    }

    /// The member `key` of an object (the first one when the key repeats); `None` for other values.
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// The value as an `i64` when it is an integer literal in range (floats with an integral value included).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Int(i) => Some(*i),
            Json::UInt(_) => None,
            Json::Float(f) if f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Int(i) => u64::try_from(*i).ok(),
            Json::UInt(u) => Some(*u),
            Json::Float(f) if f.fract() == 0.0 && *f >= 0.0 && *f < 1.8446744073709552e19 => Some(*f as u64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Int(i) => Some(*i as f64),
            Json::UInt(u) => Some(*u as f64),
            Json::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Object(m) => Some(m),
            _ => None,
        }
    }

    /// The compact serialization (no whitespace, keys in insertion order).
    pub fn to_string(&self) -> String {
        let mut out = String::new();
        self.write_to(&mut out);
        out
    }

    /// Appends the compact serialization to `out`.
    pub fn write_to(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Int(i) => {
                let _ = write!(out, "{i}");
            }
            Json::UInt(u) => {
                let _ = write!(out, "{u}");
            }
            Json::Float(f) => write_float(*f, out),
            Json::String(s) => write_string(s, out),
            Json::Array(a) => {
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    v.write_to(out);
                }
                out.push(']');
            }
            Json::Object(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_string(k, out);
                    out.push(':');
                    v.write_to(out);
                }
                out.push('}');
            }
        }
    }
}

/// Writes a float with Rust's shortest round-trip digits, in exponent form outside `[1e-5, 1e16)`; JSON has no
/// NaN/infinity, those become `null`.
fn write_float(f: f64, out: &mut String) {
    if !f.is_finite() {
        out.push_str("null");
    } else if f == 0.0 {
        out.push_str(if f.is_sign_negative() { "-0.0" } else { "0.0" });
    } else if f.abs() >= 1e16 || f.abs() < 1e-5 {
        let _ = write!(out, "{f:e}");
    } else if f.fract() == 0.0 {
        // keep an integral float recognisable as a float
        let _ = write!(out, "{f:.1}");
    } else {
        let _ = write!(out, "{f}");
    }
}

/// Writes `text` as a quoted JSON string, escaping the control characters, quotes and backslashes.
pub fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

struct Parser<'a> {
    s: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> Error {
        Error::invalid_input(format!("malformed JSON at byte {}: {what}", self.pos))
    }

    fn skip_ws(&mut self) {
        while self.pos < self.s.len() && matches!(self.s[self.pos], b' ' | b'\t' | b'\n' | b'\r') {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.pos).copied()
    }

    fn expect(&mut self, c: u8) -> Result<()> {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.err(&format!("expected '{}'", c as char)))
        }
    }

    fn literal(&mut self, word: &[u8], value: Json) -> Result<Json> {
        if self.s[self.pos..].starts_with(word) {
            self.pos += word.len();
            Ok(value)
        } else {
            Err(self.err("unexpected character"))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json> {
        if depth > MAX_DEPTH {
            return Err(self.err("nesting too deep"));
        }
        self.skip_ws();
        match self.peek() {
            Some(b'{') => {
                self.pos += 1;
                let mut members = Vec::new();
                self.skip_ws();
                if self.peek() == Some(b'}') {
                    self.pos += 1;
                    return Ok(Json::Object(members));
                }
                loop {
                    self.skip_ws();
                    let key = self.string()?;
                    self.skip_ws();
                    self.expect(b':')?;
                    let v = self.value(depth + 1)?;
                    members.push((key, v));
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.pos += 1,
                        Some(b'}') => {
                            self.pos += 1;
                            return Ok(Json::Object(members));
                        }
                        _ => return Err(self.err("expected ',' or '}'")),
                    }
                }
            }
            Some(b'[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.skip_ws();
                if self.peek() == Some(b']') {
                    self.pos += 1;
                    return Ok(Json::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.pos += 1,
                        Some(b']') => {
                            self.pos += 1;
                            return Ok(Json::Array(items));
                        }
                        _ => return Err(self.err("expected ',' or ']'")),
                    }
                }
            }
            Some(b'"') => Ok(Json::String(self.string()?)),
            Some(b't') => self.literal(b"true", Json::Bool(true)),
            Some(b'f') => self.literal(b"false", Json::Bool(false)),
            Some(b'n') => self.literal(b"null", Json::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            Some(_) => Err(self.err("unexpected character")),
            None => Err(self.err("unexpected end of input")),
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while self.pos < self.s.len() && self.s[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        self.pos - start
    }

    fn number(&mut self) -> Result<Json> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        if self.peek() == Some(b'0') {
            self.pos += 1;
        } else if self.digits() == 0 {
            return Err(self.err("expected a digit"));
        }
        let mut integral = true;
        if self.peek() == Some(b'.') {
            self.pos += 1;
            integral = false;
            if self.digits() == 0 {
                return Err(self.err("expected a digit after '.'"));
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            integral = false;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if self.digits() == 0 {
                return Err(self.err("expected a digit in the exponent"));
            }
        }
        // the slice holds only ASCII digits and punctuation
        let text = std::str::from_utf8(&self.s[start..self.pos]).map_err(|_| self.err("number"))?;
        if integral {
            if let Ok(i) = text.parse::<i64>() {
                return Ok(Json::Int(i));
            }
            if let Ok(u) = text.parse::<u64>() {
                return Ok(Json::UInt(u));
            }
        }
        text.parse::<f64>().map(Json::Float).map_err(|_| self.err("number"))
    }

    fn hex4(&mut self) -> Result<u32> {
        let hex = self.s.get(self.pos..self.pos + 4).ok_or_else(|| self.err("short \\u escape"))?;
        let text = std::str::from_utf8(hex).map_err(|_| self.err("\\u escape"))?;
        let code = u32::from_str_radix(text, 16).map_err(|_| self.err("\\u escape"))?;
        self.pos += 4;
        Ok(code)
    }

    fn string(&mut self) -> Result<String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let c = self.peek().ok_or_else(|| self.err("unterminated string"))?;
            self.pos += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let e = self.peek().ok_or_else(|| self.err("unterminated escape"))?;
                    self.pos += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let mut code = self.hex4()?;
                            if (0xD800..0xDC00).contains(&code) {
                                // a high surrogate must be followed by an escaped low surrogate
                                if self.s[self.pos..].starts_with(b"\\u") {
                                    self.pos += 2;
                                    let low = self.hex4()?;
                                    if (0xDC00..0xE000).contains(&low) {
                                        code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                                    } else {
                                        return Err(self.err("invalid surrogate pair"));
                                    }
                                } else {
                                    return Err(self.err("lone high surrogate"));
                                }
                            }
                            out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                        }
                        _ => return Err(self.err("unknown escape")),
                    }
                }
                c if c < 0x20 => return Err(self.err("control character in string")),
                _ => {
                    // copy the (already valid, the input is a &str) UTF-8 sequence through
                    let start = self.pos - 1;
                    let len = utf8_len(c);
                    let end = (start + len).min(self.s.len());
                    out.push_str(std::str::from_utf8(&self.s[start..end]).map_err(|_| self.err("invalid UTF-8"))?);
                    self.pos = end;
                }
            }
        }
    }
}

fn utf8_len(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::Json;

    #[test]
    fn round_trip_preserves_order_and_numbers() {
        let text = r#"{"b":1,"a":[true,null,-2.5,1e3,"x\n\u00e9\ud83d\ude00"],"c":18446744073709551615,"d":{}}"#;
        let v = Json::parse(text).unwrap();
        assert_eq!(v.get("b"), Some(&Json::Int(1)));
        assert_eq!(v.get("c"), Some(&Json::UInt(u64::MAX)));
        assert_eq!(v.to_string(), r#"{"b":1,"a":[true,null,-2.5,1000.0,"x\né😀"],"c":18446744073709551615,"d":{}}"#);
        assert_eq!(
            Json::parse("[1e300,-1.5e-9,0.5,-0.0,3.4028234663852886e38]").unwrap().to_string(),
            "[1e300,-1.5e-9,0.5,-0.0,3.4028234663852886e38]"
        );
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "{", "[1,]", "{\"a\" 1}", "01", "1.", "\"\\x\"", "tru", "[1] x", "\"\u{1}\""] {
            assert!(Json::parse(bad).is_err(), "{bad:?}");
        }
        let deep = "[".repeat(100_000);
        assert!(Json::parse(&deep).is_err());
    }
}
