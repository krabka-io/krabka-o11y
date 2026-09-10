use super::{
    BT_BINARY, BT_BOOL, BT_BYTE, BT_DOUBLE, BT_I16, BT_I32, BT_I64, BT_LIST, BT_MAP, BT_SET,
    BT_STOP, BT_STRUCT, WireError,
};

pub(crate) struct BinaryInput<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> BinaryInput<'a> {
    /// The deepest struct or collection nesting that `skip` descends through.
    ///
    /// Each level costs bytes on the wire and one stack frame, so an
    /// unbounded skip turns a request body into a stack overflow. Apache
    /// Thrift's own protocols stop at the same depth, and a Jaeger batch
    /// nests four deep.
    const MAX_SKIP_DEPTH: u32 = 64;

    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(crate) fn read_field(&mut self) -> Result<Option<(u8, i16)>, WireError> {
        let field_type = self.read_u8()?;
        if field_type == BT_STOP {
            return Ok(None);
        }
        Ok(Some((field_type, self.read_i16()?)))
    }

    pub(crate) fn read_struct_list<T>(
        &mut self,
        read_one: fn(&mut BinaryInput<'_>) -> Result<T, WireError>,
    ) -> Result<Vec<T>, WireError> {
        let (element_type, len) = self.read_list_header()?;
        if element_type != BT_STRUCT {
            return Err(WireError::Decode("expected struct list".into()));
        }
        (0..len).map(|_| read_one(self)).collect()
    }

    pub(crate) fn read_list_header(&mut self) -> Result<(u8, usize), WireError> {
        let element_type = self.read_u8()?;
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("list length out of range".into()))?;
        self.check_collection_header(element_type, len)?;
        Ok((element_type, len))
    }

    /// Refuse a collection header that no encoder could have produced.
    ///
    /// Every element of a binary collection occupies at least one byte: a
    /// struct at least its stop byte, and a boolean the byte holding zero or
    /// one. A length past the bytes that remain is therefore malformed by
    /// construction, whatever the element type, and needs no tunable ceiling
    /// to reject. The stop type is not a value type at all -- it terminates a
    /// struct -- and is the one element type whose skip would consume
    /// nothing.
    fn check_collection_header(&self, element_type: u8, len: usize) -> Result<(), WireError> {
        if len == 0 {
            return Ok(());
        }
        if element_type == BT_STOP {
            return Err(WireError::Decode(
                "stop is not a collection element type".into(),
            ));
        }
        let remaining = self.bytes.len().saturating_sub(self.pos);
        if len > remaining {
            return Err(WireError::Decode(format!(
                "collection of {len} elements exceeds the {remaining} bytes left"
            )));
        }
        Ok(())
    }

    pub(crate) fn read_map_header(&mut self) -> Result<(u8, u8, usize), WireError> {
        let key_type = self.read_u8()?;
        let value_type = self.read_u8()?;
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("map length out of range".into()))?;
        self.check_collection_header(key_type, len)?;
        self.check_collection_header(value_type, len)?;
        Ok((key_type, value_type, len))
    }

    pub(crate) fn read_string(&mut self) -> Result<String, WireError> {
        String::from_utf8(self.read_binary()?).map_err(|err| WireError::Decode(err.to_string()))
    }

    pub(crate) fn read_binary(&mut self) -> Result<Vec<u8>, WireError> {
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("binary length out of range".into()))?;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| WireError::Decode("binary length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(WireError::Decode("truncated binary".into()));
        }
        let out = self.bytes[self.pos..end].to_vec();
        self.pos = end;
        Ok(out)
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool, WireError> {
        Ok(self.read_u8()? != 0)
    }

    pub(crate) fn read_i16(&mut self) -> Result<i16, WireError> {
        let mut bytes = [0; 2];
        self.read_exact(&mut bytes)?;
        Ok(i16::from_be_bytes(bytes))
    }

    pub(crate) fn read_i32(&mut self) -> Result<i32, WireError> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(i32::from_be_bytes(bytes))
    }

    pub(crate) fn read_i64(&mut self) -> Result<i64, WireError> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(i64::from_be_bytes(bytes))
    }

    pub(crate) fn read_double(&mut self) -> Result<f64, WireError> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(f64::from_bits(u64::from_be_bytes(bytes)))
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8, WireError> {
        let Some(byte) = self.bytes.get(self.pos).copied() else {
            return Err(WireError::Decode("unexpected end of thrift payload".into()));
        };
        self.pos += 1;
        Ok(byte)
    }

    pub(crate) fn read_exact(&mut self, out: &mut [u8]) -> Result<(), WireError> {
        let end = self
            .pos
            .checked_add(out.len())
            .ok_or_else(|| WireError::Decode("read length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(WireError::Decode("unexpected end of thrift payload".into()));
        }
        out.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(())
    }

    pub(crate) fn skip(&mut self, field_type: u8) -> Result<(), WireError> {
        self.skip_value(field_type, 0)
    }

    /// Skip one field or element value, `depth` levels inside the outermost
    /// struct.
    fn skip_value(&mut self, field_type: u8, depth: u32) -> Result<(), WireError> {
        match field_type {
            BT_STOP => Ok(()),
            BT_BOOL | BT_BYTE => self.read_u8().map(|_| ()),
            BT_I16 => self.read_i16().map(|_| ()),
            BT_I32 => self.read_i32().map(|_| ()),
            BT_I64 => self.read_i64().map(|_| ()),
            BT_DOUBLE => self.read_double().map(|_| ()),
            BT_BINARY => self.read_binary().map(|_| ()),
            BT_STRUCT => {
                let depth = Self::descend(depth)?;
                while let Some((inner_type, _)) = self.read_field()? {
                    self.skip_value(inner_type, depth)?;
                }
                Ok(())
            }
            BT_LIST | BT_SET => {
                let depth = Self::descend(depth)?;
                let (element_type, len) = self.read_list_header()?;
                for _ in 0..len {
                    self.skip_value(element_type, depth)?;
                }
                Ok(())
            }
            BT_MAP => {
                let depth = Self::descend(depth)?;
                let (key_type, value_type, len) = self.read_map_header()?;
                for _ in 0..len {
                    self.skip_value(key_type, depth)?;
                    self.skip_value(value_type, depth)?;
                }
                Ok(())
            }
            other => Err(WireError::Decode(format!("unknown thrift type {other}"))),
        }
    }

    /// Step one level deeper, refusing nesting past [`Self::MAX_SKIP_DEPTH`].
    fn descend(depth: u32) -> Result<u32, WireError> {
        let depth = depth.saturating_add(1);
        if depth > Self::MAX_SKIP_DEPTH {
            return Err(WireError::Decode(format!(
                "thrift nesting deeper than {}",
                Self::MAX_SKIP_DEPTH
            )));
        }
        Ok(depth)
    }
}

#[cfg(test)]
mod tests {

    /// A collection header declares how many elements follow, and every
    /// binary element occupies at least one byte -- a struct its stop byte, a
    /// boolean the byte holding zero or one. A length past the bytes that
    /// remain is therefore unsatisfiable whatever the elements are, and the
    /// header is refused where it stands rather than one element at a time.
    #[test]
    fn a_collection_longer_than_the_bytes_left_is_refused_at_the_header() {
        // A struct element type, then a four-byte length of 2^31 - 1.
        let mut input = BinaryInput::new(&[BT_STRUCT, 0x7F, 0xFF, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_list_header());

        // The bound is the bytes left, not merely the end of the buffer.
        let mut input = BinaryInput::new(&[BT_STRUCT, 0, 0, 0, 2, 0x00]);
        check!(let Err(WireError::Decode(_)) = input.read_list_header());

        // One element in one byte is satisfiable, and still decodes.
        let mut input = BinaryInput::new(&[BT_STRUCT, 0, 0, 0, 1, 0x00]);
        check!(input.read_list_header().expect("reads") == (BT_STRUCT, 1));

        // A map header is a key type, a value type, and then the length.
        let mut input = BinaryInput::new(&[BT_BINARY, BT_I32, 0x7F, 0xFF, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_map_header());

        // An empty collection declares nothing, so nothing has to fit.
        let mut input = BinaryInput::new(&[BT_STRUCT, 0, 0, 0, 0]);
        check!(input.read_list_header().expect("reads") == (BT_STRUCT, 0));
    }

    /// Stop terminates a struct; it is not a value type, and it is the one
    /// element type whose skip would read nothing at all. A collection that
    /// names it is refused rather than walked.
    #[test]
    fn the_stop_type_is_not_a_collection_element_type() {
        let mut input = BinaryInput::new(&[BT_STOP, 0, 0, 0, 1, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_list_header());

        // Either half of a map's type pair is enough to refuse it.
        let mut input = BinaryInput::new(&[BT_STOP, BT_I32, 0, 0, 0, 1, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_map_header());
        let mut input = BinaryInput::new(&[BT_I32, BT_STOP, 0, 0, 0, 1, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_map_header());
    }

    /// Unlike the compact protocol, a binary boolean is a byte wherever it
    /// appears, so a collection of them is walked a byte at a time. The
    /// length bound leans on that, and this pins it.
    #[test]
    fn a_boolean_element_spends_a_byte_of_its_own() {
        let mut input = BinaryInput::new(&[BT_BOOL, 0, 0, 0, 3, 0x01, 0x00, 0x01, 0x2A]);
        check!(input.skip(BT_LIST).is_ok());
        check!(
            input.pos == 8,
            "five header bytes, then one byte for each element"
        );
        check!(input.read_u8().expect("the sentinel") == 0x2A);
    }

    /// `skip` recurses through nesting, and a list of lists costs five bytes
    /// per level, so an unbounded skip turns a request body into thousands of
    /// stack frames. Nesting stops at a fixed depth, well past anything a
    /// Jaeger batch reaches.
    #[test]
    fn nesting_past_the_skip_depth_is_refused_rather_than_recursed() {
        /// A list header: one element, itself a list.
        const NESTED: [u8; 5] = [BT_LIST, 0, 0, 0, 1];

        let depth = usize::try_from(BinaryInput::MAX_SKIP_DEPTH).expect("fits");

        let bomb = NESTED.repeat(depth * 4);
        let mut input = BinaryInput::new(&bomb);
        check!(let Err(WireError::Decode(_)) = input.skip(BT_LIST));
        check!(
            input.pos == depth * NESTED.len(),
            "one header is read per level, and no level past the limit"
        );

        // Nesting up to the limit is well-formed, and still walked: the
        // innermost list is empty, which ends the descent.
        let mut nested = NESTED.repeat(depth - 1);
        nested.extend_from_slice(&[BT_LIST, 0, 0, 0, 0]);
        nested.push(0x2A);
        let mut input = BinaryInput::new(&nested);
        check!(input.skip(BT_LIST).is_ok());
        check!(input.read_u8().expect("the sentinel") == 0x2A);
    }

    /// The header checks sit on the path every batch takes, so a well-formed
    /// binary batch is decoded here to show they refuse nothing legitimate:
    /// a process, a one-element span list, and a tag list inside it.
    #[test]
    fn a_well_formed_binary_batch_still_decodes() {
        fn field(out: &mut Vec<u8>, field_type: u8, id: i16) {
            out.push(field_type);
            out.extend_from_slice(&id.to_be_bytes());
        }
        fn string_field(out: &mut Vec<u8>, id: i16, value: &str) {
            field(out, BT_BINARY, id);
            out.extend_from_slice(&i32::try_from(value.len()).expect("fits").to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) {
            field(out, BT_I64, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn list_header(out: &mut Vec<u8>, element_type: u8, len: i32) {
            out.push(element_type);
            out.extend_from_slice(&len.to_be_bytes());
        }

        let mut body = Vec::new();
        field(&mut body, BT_STRUCT, 1);
        string_field(&mut body, 1, "checkout");
        body.push(BT_STOP);
        field(&mut body, BT_LIST, 2);
        list_header(&mut body, BT_STRUCT, 1);
        i64_field(&mut body, 1, 2);
        i64_field(&mut body, 2, 1);
        i64_field(&mut body, 3, 3);
        string_field(&mut body, 5, "GET /binary");
        i64_field(&mut body, 8, 1_000);
        i64_field(&mut body, 9, 25);
        field(&mut body, BT_LIST, 10);
        list_header(&mut body, BT_STRUCT, 1);
        string_field(&mut body, 1, "http.method");
        field(&mut body, BT_I32, 2);
        body.extend_from_slice(&0_i32.to_be_bytes());
        string_field(&mut body, 3, "GET");
        body.push(BT_STOP);
        body.push(BT_STOP);
        body.push(BT_STOP);

        let spans = decode_jaeger_binary_thrift(&body).expect("decodes");

        check!(spans.len() == 1);
        check!(spans[0].name == "GET /binary");
        check!(spans[0].start_ns == 1_000_000);
        check!(
            spans[0].span_attrs
                == vec![KeyValue {
                    key: "http.method".into(),
                    value: AttrValue::Str("GET".into()),
                }]
        );
    }

    use assert2::check;

    use super::*;
    use crate::{
        span::{AttrValue, KeyValue},
        wire::jaeger::decode_jaeger_binary_thrift,
    };
}
