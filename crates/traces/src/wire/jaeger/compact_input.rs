use super::{
    T_BINARY, T_BOOL_FALSE, T_BOOL_TRUE, T_BYTE, T_DOUBLE, T_I16, T_I32, T_I64, T_LIST, T_MAP,
    T_SET, T_STOP, T_STRUCT, WireError,
};

pub(crate) struct CompactInput<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> CompactInput<'a> {
    /// The deepest struct or collection nesting that `skip` descends through.
    ///
    /// Each level costs one byte on the wire and one stack frame, so an
    /// unbounded skip turns a 64 KiB datagram into a stack overflow. Apache
    /// Thrift's own protocols stop at the same depth, and a Jaeger batch
    /// nests four deep.
    const MAX_SKIP_DEPTH: u32 = 64;

    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(crate) fn read_field(
        &mut self,
        last_field_id: &mut i16,
    ) -> Result<Option<(u8, i16)>, WireError> {
        let header = self.read_u8()?;
        let field_type = header & 0x0F;
        if field_type == T_STOP {
            return Ok(None);
        }
        let delta = i16::from(header >> 4);
        let field_id = if delta == 0 {
            i16::try_from(self.read_i32()?)
                .map_err(|_| WireError::Decode("field id out of range".into()))?
        } else {
            last_field_id.saturating_add(delta)
        };
        *last_field_id = field_id;
        Ok(Some((field_type, field_id)))
    }

    pub(crate) fn read_struct_list<T>(
        &mut self,
        read_one: fn(&mut CompactInput<'_>) -> Result<T, WireError>,
    ) -> Result<Vec<T>, WireError> {
        let (element_type, len) = self.read_list_header()?;
        if element_type != T_STRUCT {
            return Err(WireError::Decode("expected struct list".into()));
        }
        (0..len).map(|_| read_one(self)).collect()
    }

    pub(crate) fn read_list_header(&mut self) -> Result<(u8, usize), WireError> {
        let header = self.read_u8()?;
        let element_type = header & 0x0F;
        let short_len = usize::from(header >> 4);
        let len = if short_len == 15 {
            usize::try_from(self.read_varint()?)
                .map_err(|_| WireError::Decode("list too large".into()))?
        } else {
            short_len
        };
        self.check_collection_header(element_type, len)?;
        Ok((element_type, len))
    }

    /// Refuse a collection header that no encoder could have produced.
    ///
    /// Every element of a compact collection occupies at least one byte: a
    /// struct at least its stop byte, and a boolean the byte that carries its
    /// value once it is an element rather than a field. A length past the
    /// bytes that remain is therefore malformed by construction, whatever the
    /// element type, and needs no tunable ceiling to reject. The stop type is
    /// not a value type at all -- it terminates a struct -- and is the one
    /// element type whose skip would consume nothing.
    fn check_collection_header(&self, element_type: u8, len: usize) -> Result<(), WireError> {
        if len == 0 {
            return Ok(());
        }
        if element_type == T_STOP {
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
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| WireError::Decode("map too large".into()))?;
        if len == 0 {
            return Ok((T_STOP, T_STOP, 0));
        }
        let types = self.read_u8()?;
        let (key_type, value_type) = (types >> 4, types & 0x0F);
        self.check_collection_header(key_type, len)?;
        self.check_collection_header(value_type, len)?;
        Ok((key_type, value_type, len))
    }

    pub(crate) fn read_string(&mut self) -> Result<String, WireError> {
        String::from_utf8(self.read_binary()?).map_err(|err| WireError::Decode(err.to_string()))
    }

    pub(crate) fn read_binary(&mut self) -> Result<Vec<u8>, WireError> {
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| WireError::Decode("binary too large".into()))?;
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

    pub(crate) fn read_i32(&mut self) -> Result<i32, WireError> {
        let value = self.read_varint()?;
        let value = u32::try_from(value)
            .map_err(|_| WireError::Decode("i32 varint out of range".into()))?;
        Ok((value >> 1).cast_signed() ^ -((value & 1).cast_signed()))
    }

    pub(crate) fn read_i64(&mut self) -> Result<i64, WireError> {
        let value = self.read_varint()?;
        Ok((value >> 1).cast_signed() ^ -((value & 1).cast_signed()))
    }

    pub(crate) fn read_double(&mut self) -> Result<f64, WireError> {
        let mut bytes = [0; 8];
        for byte in &mut bytes {
            *byte = self.read_u8()?;
        }
        Ok(f64::from_le_bytes(bytes))
    }

    pub(crate) fn read_varint(&mut self) -> Result<u64, WireError> {
        let mut shift = 0;
        let mut out = 0_u64;
        loop {
            let byte = self.read_u8()?;
            out |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
            // `shift` only ever takes 7, 14, ... 63, 70, so every threshold in
            // 64..=70 refuses on exactly the same byte. A sweep reporting a
            // survivor here has found that equivalence, not a missing test.
            if shift >= 64 {
                return Err(WireError::Decode("varint too long".into()));
            }
        }
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8, WireError> {
        let Some(byte) = self.bytes.get(self.pos).copied() else {
            return Err(WireError::Decode("unexpected end of thrift payload".into()));
        };
        self.pos += 1;
        Ok(byte)
    }

    pub(crate) fn skip(&mut self, field_type: u8) -> Result<(), WireError> {
        self.skip_value(field_type, 0)
    }

    /// Skip one field value, `depth` levels inside the outermost struct.
    fn skip_value(&mut self, field_type: u8, depth: u32) -> Result<(), WireError> {
        match field_type {
            // A boolean field keeps its value in the type nibble of its own
            // header, so there is nothing after the header to skip.
            T_STOP | T_BOOL_TRUE | T_BOOL_FALSE => Ok(()),
            T_BYTE => self.read_u8().map(|_| ()),
            T_I16 | T_I32 => self.read_i32().map(|_| ()),
            T_I64 => self.read_i64().map(|_| ()),
            T_DOUBLE => self.read_double().map(|_| ()),
            T_BINARY => self.read_binary().map(|_| ()),
            T_STRUCT => {
                let depth = Self::descend(depth)?;
                let mut last = 0;
                while let Some((inner_type, _)) = self.read_field(&mut last)? {
                    self.skip_value(inner_type, depth)?;
                }
                Ok(())
            }
            T_LIST | T_SET => {
                let depth = Self::descend(depth)?;
                let (element_type, len) = self.read_list_header()?;
                for _ in 0..len {
                    self.skip_element(element_type, depth)?;
                }
                Ok(())
            }
            T_MAP => {
                let depth = Self::descend(depth)?;
                let (key_type, value_type, len) = self.read_map_header()?;
                for _ in 0..len {
                    self.skip_element(key_type, depth)?;
                    self.skip_element(value_type, depth)?;
                }
                Ok(())
            }
            other => Err(WireError::Decode(format!("unknown thrift type {other}"))),
        }
    }

    /// Skip one collection element of `element_type`.
    ///
    /// A boolean is the one type an element does not encode the way a field
    /// does: outside a field header there is no type nibble to hold the
    /// value, so each element spends a byte of its own, which is also what
    /// Apache Thrift's compact writer emits.
    fn skip_element(&mut self, element_type: u8, depth: u32) -> Result<(), WireError> {
        if element_type == T_BOOL_TRUE || element_type == T_BOOL_FALSE {
            return self.read_u8().map(|_| ());
        }
        self.skip_value(element_type, depth)
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
    /// compact element occupies at least one byte. A length past the bytes
    /// that remain is therefore unsatisfiable whatever the elements are, and
    /// the header is refused where it stands rather than one element at a
    /// time.
    #[test]
    fn a_collection_longer_than_the_bytes_left_is_refused_at_the_header() {
        // Fifteen in the size nibble means the length follows as a varint.
        // Element type twelve is a struct; nothing follows the length.
        let mut input = CompactInput::new(&[0xFC, 0x7F]);
        check!(let Err(WireError::Decode(_)) = input.read_list_header());

        // The bound is the bytes left, not merely the end of the buffer: two
        // struct elements cannot fit in the single byte after the header.
        let mut input = CompactInput::new(&[0x2C, 0x00]);
        check!(let Err(WireError::Decode(_)) = input.read_list_header());

        // One element in one byte is satisfiable, and still decodes.
        let mut input = CompactInput::new(&[0x1C, 0x00]);
        check!(input.read_list_header().expect("reads") == (T_STRUCT, 1));

        // A compact map header is a varint length and then a types byte.
        let mut input = CompactInput::new(&[0x7F, 0x88]);
        check!(let Err(WireError::Decode(_)) = input.read_map_header());

        // An empty collection declares nothing, so nothing has to fit.
        let mut input = CompactInput::new(&[0x0C]);
        check!(input.read_list_header().expect("reads") == (T_STRUCT, 0));
        let mut input = CompactInput::new(&[0x00]);
        check!(input.read_map_header().expect("reads") == (T_STOP, T_STOP, 0));
    }

    /// Stop terminates a struct; it is not a value type, and it is the one
    /// element type whose skip would read nothing at all. A collection that
    /// names it is refused rather than walked.
    #[test]
    fn the_stop_type_is_not_a_collection_element_type() {
        // One element, of type stop.
        let mut input = CompactInput::new(&[0x10, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_list_header());

        // Either half of a map's types byte is enough to refuse it.
        let mut input = CompactInput::new(&[0x01, 0x08, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_map_header());
        let mut input = CompactInput::new(&[0x01, 0x80, 0xFF, 0xFF]);
        check!(let Err(WireError::Decode(_)) = input.read_map_header());
    }

    /// A boolean is the one type whose encoding differs between a field and a
    /// collection element. A field header carries the value in its type
    /// nibble and is followed by nothing; an element has no header of its
    /// own, so Apache Thrift's compact writer spends a byte on each one --
    /// and a skip that reads neither byte nor header is both a desync and,
    /// inside a long list, an endless loop.
    #[test]
    fn a_boolean_element_spends_a_byte_where_a_boolean_field_spends_none() {
        // Three booleans, then a sentinel that belongs to whatever follows.
        let mut input = CompactInput::new(&[0x31, 0x01, 0x00, 0x01, 0x2A]);
        check!(input.skip(T_LIST).is_ok());
        check!(
            input.pos == 4,
            "one header byte, then one byte for each element"
        );
        check!(input.read_u8().expect("the sentinel") == 0x2A);

        // The same three, as a set of the false type: the element type names
        // the type, not the values, so the bytes are still read.
        let mut input = CompactInput::new(&[0x32, 0x01, 0x00, 0x01, 0x2A]);
        check!(input.skip(T_SET).is_ok());
        check!(input.read_u8().expect("the sentinel") == 0x2A);

        // As a field the value is in the header, so the sentinel is the next
        // byte after the header alone.
        let mut input = CompactInput::new(&[0x11, 0x2A]);
        let mut last = 0;
        let (field_type, _) = input
            .read_field(&mut last)
            .expect("reads")
            .expect("a field, not a stop");
        check!(field_type == T_BOOL_TRUE);
        check!(input.skip(field_type).is_ok());
        check!(input.read_u8().expect("the sentinel") == 0x2A);
    }

    /// `skip` recurses through nesting, and a compact list of lists costs one
    /// byte per level, so an unbounded skip turns a datagram into as many
    /// stack frames as it has bytes. Nesting stops at a fixed depth, well
    /// past anything a Jaeger batch reaches.
    #[test]
    fn nesting_past_the_skip_depth_is_refused_rather_than_recursed() {
        let depth = usize::try_from(CompactInput::MAX_SKIP_DEPTH).expect("fits");

        // Each byte is a one-element list whose element is another list.
        let bomb = vec![0x19; depth * 4];
        let mut input = CompactInput::new(&bomb);
        check!(let Err(WireError::Decode(_)) = input.skip(T_LIST));
        check!(
            input.pos == depth,
            "one header is read per level, and no level past the limit"
        );

        // Nesting up to the limit is well-formed, and still walked: the
        // innermost list is empty, which ends the descent.
        let mut nested = vec![0x19; depth - 1];
        nested.push(0x09);
        nested.push(0x2A);
        let mut input = CompactInput::new(&nested);
        check!(input.skip(T_LIST).is_ok());
        check!(input.read_u8().expect("the sentinel") == 0x2A);
    }

    /// The header checks sit on the path every batch takes, so the sample
    /// batch is decoded here as well to show they refuse nothing legitimate.
    /// `decodes_jaeger_thrift_batch` asserts its full shape.
    #[test]
    fn a_well_formed_batch_still_decodes() {
        let spans = decode_jaeger_thrift(&encode_sample_batch()).expect("decodes");

        check!(spans.len() == 1);
        check!(spans[0].name == "GET /");
    }

    use assert2::check;

    use super::*;
    use crate::wire::jaeger::{decode_jaeger_thrift, test_support::encode_sample_batch};
}
