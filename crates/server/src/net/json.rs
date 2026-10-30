//! A small JSON reader and writer for admin bodies and results (D21, D26). The server keeps
//! off `serde_json` (CLAUDE.md, invariant 1), and these are small flat objects.

const MAX_DEPTH: usize = 16;

#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// The number's text, so that a u64 keeps every digit.
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub fn parse(bytes: &[u8]) -> Option<Json> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut parser = Parser {
            bytes: text.as_bytes(),
            at: 0,
        };
        let value = parser.value(0)?;
        parser.space();
        (parser.at == parser.bytes.len()).then_some(value)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(text) => Some(text),
            _ => None,
        }
    }

    /// A non-negative integer, as a JSON number or a decimal string (D26: ids are strings).
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Number(text) | Json::String(text) => text.parse().ok(),
            _ => None,
        }
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn space(&mut self) {
        while matches!(self.bytes.get(self.at), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> Option<()> {
        (self.bytes.get(self.at) == Some(&byte)).then(|| self.at += 1)
    }

    fn word(&mut self, word: &[u8]) -> Option<()> {
        self.bytes[self.at..]
            .starts_with(word)
            .then(|| self.at += word.len())
    }

    fn value(&mut self, depth: usize) -> Option<Json> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.space();
        match self.bytes.get(self.at)? {
            b'n' => self.word(b"null").map(|()| Json::Null),
            b't' => self.word(b"true").map(|()| Json::Bool(true)),
            b'f' => self.word(b"false").map(|()| Json::Bool(false)),
            b'"' => self.string().map(Json::String),
            b'[' => {
                self.at += 1;
                let mut items = Vec::new();
                self.space();
                if self.eat(b']').is_some() {
                    return Some(Json::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.space();
                    if self.eat(b']').is_some() {
                        return Some(Json::Array(items));
                    }
                    self.eat(b',')?;
                }
            }
            b'{' => {
                self.at += 1;
                let mut fields = Vec::new();
                self.space();
                if self.eat(b'}').is_some() {
                    return Some(Json::Object(fields));
                }
                loop {
                    self.space();
                    let key = self.string()?;
                    self.space();
                    self.eat(b':')?;
                    fields.push((key, self.value(depth + 1)?));
                    self.space();
                    if self.eat(b'}').is_some() {
                        return Some(Json::Object(fields));
                    }
                    self.eat(b',')?;
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = self.at;
                self.at += 1;
                while matches!(
                    self.bytes.get(self.at),
                    Some(b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                ) {
                    self.at += 1;
                }
                let text = std::str::from_utf8(&self.bytes[start..self.at]).ok()?;
                text.parse::<f64>().ok()?;
                Some(Json::Number(text.to_owned()))
            }
            _ => None,
        }
    }

    fn string(&mut self) -> Option<String> {
        self.eat(b'"')?;
        let mut out = Vec::new();
        loop {
            let byte = *self.bytes.get(self.at)?;
            self.at += 1;
            match byte {
                b'"' => return String::from_utf8(out).ok(),
                b'\\' => {
                    let escape = *self.bytes.get(self.at)?;
                    self.at += 1;
                    let plain = match escape {
                        b'"' | b'\\' | b'/' => escape,
                        b'b' => 8,
                        b'f' => 12,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'u' => {
                            let mut code = self.hex4()?;
                            if (0xD800..0xDC00).contains(&code) {
                                self.word(b"\\u")?;
                                let low = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&low) {
                                    return None;
                                }
                                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                            }
                            let character = char::from_u32(code)?;
                            let mut buffer = [0u8; 4];
                            out.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
                            continue;
                        }
                        _ => return None,
                    };
                    out.push(plain);
                }
                0..=0x1F => return None,
                _ => out.push(byte),
            }
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        let digits = self.bytes.get(self.at..self.at + 4)?;
        self.at += 4;
        u32::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()
    }
}

/// `text` as a JSON string, with its quotes.
pub fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn it_reads_what_admin_bodies_hold() {
        let body = br#" {"retention":"7d","min_ack":2,"on":true,"x":null,"list":[1,"a"],
                         "big":18446744073709551615,"text":"a\"\n\u00e9\ud83d\ude00"} "#;
        let Some(Json::Object(fields)) = Json::parse(body) else {
            panic!("should parse");
        };
        assert_eq!(fields[0].1.as_str(), Some("7d"));
        assert_eq!(fields[1].1.as_u64(), Some(2));
        assert_eq!(fields[2].1, Json::Bool(true));
        assert_eq!(fields[3].1, Json::Null);
        assert_eq!(fields[5].1.as_u64(), Some(u64::MAX));
        assert_eq!(fields[6].1.as_str(), Some("a\"\n\u{e9}\u{1F600}"));
    }

    #[test]
    fn it_refuses_what_is_not_json() {
        for bad in [
            &b"{"[..],
            b"{\"a\"}",
            b"[1,]",
            b"nul",
            b"\"\x01\"",
            b"1 2",
            b"\xFF",
            b"\"\\ud83d\"",
            b"--",
        ] {
            assert_eq!(Json::parse(bad), None, "{bad:?}");
        }
        let deep = "[".repeat(1000);
        assert_eq!(Json::parse(deep.as_bytes()), None);
    }

    proptest! {
        #[test]
        fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let _ = Json::parse(&bytes);
        }

        #[test]
        fn a_quoted_string_reads_back(text in ".*") {
            let parsed = Json::parse(quoted(&text).as_bytes());
            prop_assert_eq!(parsed, Some(Json::String(text)));
        }
    }
}
