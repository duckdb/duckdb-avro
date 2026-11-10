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
        let mut p = Parser { text, s: text.as_bytes(), pos: 0 };
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
            Json::Int(i) => write_integer(*i < 0, i.unsigned_abs(), out),
            Json::UInt(u) => write_integer(false, *u, out),
            Json::Float(f) => write_float(*f, out),
            Json::String(s) => write_string(s, out),
            Json::Array(_) | Json::Object(_) => {
                // one call site for both container kinds (see `write_items`)
                let mut array_items;
                let mut object_items;
                let (items, open, close): (&mut dyn Iterator<Item = (Option<&str>, &Json)>, u8, u8) = match self {
                    Json::Array(a) => {
                        array_items = a.iter().map(|v| (None, v));
                        (&mut array_items, b'[', b']')
                    }
                    Json::Object(m) => {
                        object_items = m.iter().map(|(k, v)| (Some(k.as_str()), v));
                        (&mut object_items, b'{', b'}')
                    }
                    _ => return,
                };
                write_items(items, open, close, out);
            }
        }
    }
}

/// Writes `items` (each with its key for an object) between `open` and `close`, comma-separated. The one place the
/// writer recurses from: CBMC (the proof harnesses) inlines a recursive function once per call site per unwinding, so
/// arrays and objects share this site through the `dyn` iterator.
fn write_items(items: &mut dyn Iterator<Item = (Option<&str>, &Json)>, open: u8, close: u8, out: &mut String) {
    out.push(open as char);
    for (i, (key, value)) in items.enumerate() {
        if i > 0 {
            out.push(',');
        }
        if let Some(key) = key {
            write_string(key, out);
            out.push(':');
        }
        value.write_to(out);
    }
    out.push(close as char);
}

/// The decimal digits as one-byte strings: pushed as `&str` (a one-byte copy), not as a `char` (whose UTF-8 encoding
/// of a symbolic value CBMC, in the proof harnesses, has to branch on).
const DIGITS: [&str; 10] = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"];

/// Writes an integer in decimal (what `Display` prints), digit by digit: the proof harnesses walk this writer, and
/// `core::fmt` is far beyond what CBMC explores.
fn write_integer(negative: bool, magnitude: u64, out: &mut String) {
    if negative {
        out.push('-');
    }
    let mut digits = [0u8; 20];
    let mut n = 0;
    let mut rest = magnitude;
    loop {
        digits[n] = (rest % 10) as u8;
        n += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    while n > 0 {
        n -= 1;
        out.push_str(DIGITS[usize::from(digits[n])]);
    }
}

/// An integer literal (an optional `-` and digits, as the grammar checked) as `Int` when it fits an i64 and `UInt` when
/// it fits a u64 (what `text.parse::<i64>()`, then `text.parse::<u64>()` would accept); `None` beyond.
fn parse_integer(text: &str) -> Option<Json> {
    let (negative, digits) = match text.as_bytes() {
        [b'-', digits @ ..] => (true, digits),
        digits => (false, digits),
    };
    let mut magnitude: u64 = 0;
    for &d in digits {
        magnitude = magnitude.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    if negative {
        // down to -2^63; `-0` is 0
        (magnitude <= 1 << 63).then(|| Json::Int((magnitude as i64).wrapping_neg()))
    } else if let Ok(i) = i64::try_from(magnitude) {
        Some(Json::Int(i))
    } else {
        Some(Json::UInt(magnitude))
    }
}

/// `text.parse::<f64>()`: the one number path the proof harnesses stub (`core`'s decimal-to-float conversion is out of
/// CBMC's reach).
pub(crate) fn parse_float(text: &str) -> Option<f64> {
    text.parse::<f64>().ok()
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

/// Writes `text` as a quoted JSON string, escaping the control characters, quotes and backslashes. Everything that is
/// escaped is ASCII, so the text is walked by bytes and the runs between escapes are copied as they are (every run
/// boundary is an ASCII byte, hence a char boundary).
pub fn write_string(text: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    let bytes = text.as_bytes();
    let mut run_start = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let escape: Option<&str> = match b {
            b'"' => Some("\\\""),
            b'\\' => Some("\\\\"),
            b'\n' => Some("\\n"),
            b'\r' => Some("\\r"),
            b'\t' => Some("\\t"),
            0x08 => Some("\\b"),
            0x0c => Some("\\f"),
            _ => None,
        };
        if escape.is_some() || b < 0x20 {
            out.push_str(&text[run_start..i]);
            match escape {
                Some(escape) => out.push_str(escape),
                None => {
                    // `\u00XX`, lowercase hex (what `{:04x}` prints for a code below 0x20)
                    out.push_str("\\u00");
                    out.push(HEX[usize::from(b >> 4)] as char);
                    out.push(HEX[usize::from(b & 0xf)] as char);
                }
            }
            run_start = i + 1;
        }
        i += 1;
    }
    out.push_str(&text[run_start..]);
    out.push('"');
}

struct Parser<'a> {
    text: &'a str,
    /// `text.as_bytes()`
    s: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    /// `malformed JSON at byte <pos>: <what>`; under `cfg(kani)` (the proof harnesses) just `what`, as the crate's
    /// error macros do: `core::fmt` is out of CBMC's reach and no property is about message text.
    #[cfg(not(kani))]
    fn err(&self, what: &str) -> Error {
        Error::invalid_input(format!("malformed JSON at byte {}: {what}", self.pos))
    }

    #[cfg(kani)]
    fn err(&self, what: &str) -> Error {
        Error::invalid_input(what)
    }

    /// The error for a missing punctuation character `c`.
    #[cfg(not(kani))]
    fn expected(&self, c: u8) -> Error {
        self.err(&format!("expected '{}'", c as char))
    }

    #[cfg(kani)]
    fn expected(&self, c: u8) -> Error {
        self.err("expected a punctuation character")
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
            Err(self.expected(c))
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
            Some(open @ (b'{' | b'[')) => self.container(open == b'{', depth),
            Some(b'"') => Ok(Json::String(self.string()?)),
            Some(b't') => self.literal(b"true", Json::Bool(true)),
            Some(b'f') => self.literal(b"false", Json::Bool(false)),
            Some(b'n') => self.literal(b"null", Json::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            Some(_) => Err(self.err("unexpected character")),
            None => Err(self.err("unexpected end of input")),
        }
    }

    /// An object (`keyed`) or array after its opening bracket. The one place the parser recurses from: CBMC (the proof
    /// harnesses) inlines a recursive function once per call site per unwinding, so both container kinds share it.
    fn container(&mut self, keyed: bool, depth: usize) -> Result<Json> {
        let close = if keyed { b'}' } else { b']' };
        self.pos += 1;
        let mut members = Vec::new();
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(close) {
            self.pos += 1;
            return Ok(if keyed { Json::Object(members) } else { Json::Array(items) });
        }
        loop {
            let key = if keyed {
                self.skip_ws();
                let key = self.string()?;
                self.skip_ws();
                self.expect(b':')?;
                Some(key)
            } else {
                None
            };
            let value = self.value(depth + 1)?;
            match key {
                Some(key) => members.push((key, value)),
                None => items.push(value),
            }
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(c) if c == close => {
                    self.pos += 1;
                    return Ok(if keyed { Json::Object(members) } else { Json::Array(items) });
                }
                _ => Err(self.err(if keyed { "expected ',' or '}'" } else { "expected ',' or ']'" }))?,
            }
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
        // the literal holds only ASCII digits and punctuation: a char boundary at both ends
        let text = &self.text[start..self.pos];
        if integral && let Some(integer) = parse_integer(text) {
            return Ok(integer);
        }
        parse_float(text).map(Json::Float).ok_or_else(|| self.err("number"))
    }

    fn hex4(&mut self) -> Result<u32> {
        if self.pos + 4 > self.s.len() {
            return Err(self.err("short \\u escape"));
        }
        // not a char boundary: a non-ASCII byte in the range, which is no hex digit either way
        let text = self.text.get(self.pos..self.pos + 4).ok_or_else(|| self.err("\\u escape"))?;
        let code = u32::from_str_radix(text, 16).map_err(|_| self.err("\\u escape"))?;
        self.pos += 4;
        Ok(code)
    }

    fn string(&mut self) -> Result<String> {
        self.expect(b'"')?;
        // the decoded string is never longer than the literal: size the buffer once from a scan to the closing quote
        let mut end = self.pos;
        while end < self.s.len() && self.s[end] != b'"' {
            end += if self.s[end] == b'\\' { 2 } else { 1 };
        }
        let mut out = String::with_capacity(end.min(self.s.len()) - self.pos);
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
                    // copy the UTF-8 sequence through: the input is a `&str`, so `utf8_len` of its first byte ends
                    // at a char boundary
                    let start = self.pos - 1;
                    let end = start + utf8_len(c);
                    out.push_str(&self.text[start..end]);
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

/// Kani proof harnesses (`cargo kani -p avro-core`, `make kani`) over the number layer of the parser and the writer:
/// `Parser::number` (the literal grammar and the `i64` -> `u64` -> `f64` rule), `parse_integer` and `write_integer`.
/// The recursive value parser (`value` / `container` / `string`, growable `Vec`s and `String`s at every level) and
/// `write_string` (a loop of symbolic-length appends) are beyond CBMC even for one-byte inputs; they stay with the
/// unit tests. The one piece of the number path out of reach is `core`'s decimal-to-float conversion (`parse_float`),
/// which the harnesses stub with a symbolic result where a float literal can be reached.
#[cfg(kani)]
#[allow(unsafe_code)]
mod verification {
    use super::*;

    /// `parse_float` for inputs that may hold a float literal: some float, which no property below depends on.
    #[allow(dead_code)] // referenced from `kani::stub` attributes only
    fn any_float(_text: &str) -> Option<f64> {
        Some(kani::any())
    }

    /// `Parser::number` at the start of `text` (the caller saw a `-` or a digit).
    fn number(text: &str) -> Result<Json> {
        let mut parser = Parser { text, s: text.as_bytes(), pos: 0 };
        parser.number()
    }

    /// The number literal of an optional `-` and `digit_count` symbolic digits written into `buf`, with the 128-bit
    /// value of the digits and whether the first digit is a zero.
    fn any_integer_literal(buf: &mut [u8; 21], negative: bool, digit_count: usize) -> (&str, u128, bool) {
        let mut len = 0;
        if negative {
            buf[0] = b'-';
            len = 1;
        }
        let mut value: u128 = 0;
        let mut leading_zero = false;
        let mut i = 0;
        while i < digit_count {
            let digit: u8 = kani::any();
            kani::assume(digit < 10);
            if i == 0 {
                leading_zero = digit == 0;
            }
            value = value * 10 + u128::from(digit);
            buf[len] = b'0' + digit;
            len += 1;
            i += 1;
        }
        // SAFETY: `-` and ASCII digits
        (unsafe { std::str::from_utf8_unchecked(&buf[..len]) }, value, leading_zero)
    }

    /// What `number` must make of an integer literal of `value` (its digits' value) by the `i64`, then `u64`, then
    /// `f64` rule: `Int` when the value fits an i64, `UInt` when it fits a u64, a `Float` beyond (`-0` is `Int(0)`).
    fn check_integer_literal(result: Result<Json>, negative: bool, value: u128) {
        // matched by variant, never compared or dropped as a `Json` (its `PartialEq` and drop glue recurse through
        // the tree variants, which CBMC inlines once per call site per unwinding)
        let parsed = std::mem::ManuallyDrop::new(result);
        let (int, uint, float) = match &*parsed {
            Ok(Json::Int(i)) => (Some(*i), None, false),
            Ok(Json::UInt(u)) => (None, Some(*u), false),
            Ok(Json::Float(_)) => (None, None, true),
            _ => (None, None, false),
        };
        if negative {
            if value <= 1u128 << 63 {
                assert_eq!(int, Some((value as i64).wrapping_neg()));
            } else {
                assert!(float);
            }
        } else if value <= i64::MAX as u128 {
            assert_eq!(int, Some(value as i64));
        } else if value <= u64::MAX as u128 {
            assert_eq!(uint, Some(value as u64));
        } else {
            assert!(float);
        }
    }

    /// Integer literals of an optional `-` and 1..=4 digits (every length and digit): a leading zero before more
    /// digits ends the literal after the zero (`Json::parse` then rejects the trailing digits, as JSON forbids them),
    /// otherwise the whole literal is consumed and is `Int` of the exact value against a 128-bit reference.
    #[kani::proof]
    #[kani::unwind(6)]
    #[kani::stub(parse_float, any_float)]
    fn short_integer_literals_match_a_128_bit_reference() {
        let negative: bool = kani::any();
        let digit_count: usize = kani::any();
        kani::assume((1..=4).contains(&digit_count));
        let mut buf = [0u8; 21];
        let (text, value, leading_zero) = any_integer_literal(&mut buf, negative, digit_count);
        let mut parser = Parser { text, s: text.as_bytes(), pos: 0 };
        let result = parser.number();
        if leading_zero && digit_count > 1 {
            assert!(matches!(result, Ok(Json::Int(0))));
            assert_eq!(parser.pos, 1 + usize::from(negative));
            std::mem::forget(result);
        } else {
            assert_eq!(parser.pos, text.len());
            check_integer_literal(result, negative, value);
        }
    }

    /// Integer literals of an optional `-` and exactly 19 digits (every digit, no leading zero): across `i64::MAX`,
    /// the literal is `Int` when the value fits an i64 and `UInt` beyond (the negative ones all fit); no overflow
    /// panics.
    #[kani::proof]
    #[kani::unwind(21)]
    #[kani::stub(parse_float, any_float)]
    fn nineteen_digit_literals_cross_the_i64_boundary() {
        let negative: bool = kani::any();
        let mut buf = [0u8; 21];
        let (text, value, leading_zero) = any_integer_literal(&mut buf, negative, 19);
        kani::assume(!leading_zero);
        check_integer_literal(number(text), negative, value);
    }

    /// Integer literals of an optional `-` and exactly 20 digits (every digit, no leading zero): across `u64::MAX`
    /// and `i64::MIN`, the literal is `Int`, `UInt` or `Float` by the value; no overflow panics.
    #[kani::proof]
    #[kani::unwind(22)]
    #[kani::stub(parse_float, any_float)]
    fn twenty_digit_literals_cross_the_u64_boundary() {
        let negative: bool = kani::any();
        let mut buf = [0u8; 21];
        let (text, value, leading_zero) = any_integer_literal(&mut buf, negative, 20);
        kani::assume(!leading_zero);
        check_integer_literal(number(text), negative, value);
    }

    /// The JSON number grammar `-? (0 | [1-9][0-9]*) (. [0-9]+)? ([eE] [+-]? [0-9]+)?` as a hand-written matcher:
    /// whether a literal starts `text` and how many bytes it spans.
    fn reference_number(text: &[u8]) -> (bool, usize) {
        let mut i = 0;
        if text.first() == Some(&b'-') {
            i += 1;
        }
        match text.get(i) {
            Some(b'0') => i += 1,
            Some(d) if d.is_ascii_digit() => {
                while text.get(i).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                }
            }
            _ => return (false, 0),
        }
        if text.get(i) == Some(&b'.') {
            i += 1;
            if !text.get(i).is_some_and(u8::is_ascii_digit) {
                return (false, 0);
            }
            while text.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        if matches!(text.get(i), Some(b'e' | b'E')) {
            i += 1;
            if matches!(text.get(i), Some(b'+' | b'-')) {
                i += 1;
            }
            if !text.get(i).is_some_and(u8::is_ascii_digit) {
                return (false, 0);
            }
            while text.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        (true, i)
    }

    /// `number` on every string of 1..=6 bytes over `0-9 - + . e E x` that starts with `-` or a digit: never panics,
    /// accepts exactly the literals of the JSON number grammar (`reference_number`) and then stops exactly where the
    /// literal ends.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(parse_float, any_float)]
    fn number_accepts_exactly_the_json_grammar() {
        let mut buf = [0u8; 6];
        let len: usize = kani::any();
        kani::assume((1..=6).contains(&len));
        let mut i = 0;
        while i < len {
            let class: u8 = kani::any();
            buf[i] = match class {
                0 => b'-',
                1 => b'+',
                2 => b'.',
                3 => b'e',
                4 => b'E',
                5 => b'x',
                d => b'0' + d % 10,
            };
            i += 1;
        }
        kani::assume(buf[0] == b'-' || buf[0].is_ascii_digit());
        // SAFETY: ASCII
        let text = unsafe { std::str::from_utf8_unchecked(&buf[..len]) };
        let (accepted, consumed) = reference_number(text.as_bytes());
        let mut parser = Parser { text, s: text.as_bytes(), pos: 0 };
        let result = parser.number();
        assert!(parser.pos <= len);
        assert_eq!(result.is_ok(), accepted);
        if accepted {
            assert_eq!(parser.pos, consumed);
        }
        std::mem::forget(result);
    }

    /// `write_integer` -> `parse_integer` round trip for every value of magnitude below 10^4 (both signs): the digits
    /// written are read back as the same `Int`, 1..=4 digits with no leading zero, `-` exactly for a negative value.
    /// Bound: 4 digits (the full i64 range pits a 19-step division chain against a 19-step multiplication chain, an
    /// arithmetic equivalence beyond a SAT solver). About three minutes of CBMC: behind the `kani-heavy` feature, which
    /// `make kani` enables and the CI job does not.
    #[cfg(feature = "kani-heavy")]
    #[kani::proof]
    #[kani::unwind(6)]
    fn written_integers_parse_back() {
        let i: i64 = kani::any();
        kani::assume((-9999..=9999).contains(&i));
        let mut out = String::with_capacity(21);
        write_integer(i < 0, i.unsigned_abs(), &mut out);
        let parsed = std::mem::ManuallyDrop::new(parse_integer(&out));
        assert!(matches!(&*parsed, Some(Json::Int(back)) if *back == i));
        let bytes = out.as_bytes();
        assert_eq!(bytes[0] == b'-', i < 0);
        let digits = if bytes[0] == b'-' { &bytes[1..] } else { bytes };
        assert!(!digits.is_empty() && digits.len() <= 4);
        assert!(digits.len() == 1 || digits[0] != b'0');
    }
}
