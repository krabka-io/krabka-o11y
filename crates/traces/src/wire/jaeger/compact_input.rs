use super::{WireError, T_STOP, T_STRUCT, T_BOOL_TRUE, T_BOOL_FALSE, T_BYTE, T_I16, T_I32, T_I64, T_DOUBLE, T_BINARY, T_LIST, T_SET, T_MAP};

pub(crate) struct CompactInput<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> CompactInput<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(crate) fn read_field(&mut self, last_field_id: &mut i16) -> Result<Option<(u8, i16)>, WireError> {
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
        Ok((element_type, len))
    }

    pub(crate) fn read_map_header(&mut self) -> Result<(u8, u8, usize), WireError> {
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| WireError::Decode("map too large".into()))?;
        if len == 0 {
            return Ok((T_STOP, T_STOP, 0));
        }
        let types = self.read_u8()?;
        Ok((types >> 4, types & 0x0F, len))
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
        match field_type {
            T_STOP | T_BOOL_TRUE | T_BOOL_FALSE => Ok(()),
            T_BYTE => self.read_u8().map(|_| ()),
            T_I16 | T_I32 => self.read_i32().map(|_| ()),
            T_I64 => self.read_i64().map(|_| ()),
            T_DOUBLE => self.read_double().map(|_| ()),
            T_BINARY => self.read_binary().map(|_| ()),
            T_STRUCT => {
                let mut last = 0;
                while let Some((inner_type, _)) = self.read_field(&mut last)? {
                    self.skip(inner_type)?;
                }
                Ok(())
            }
            T_LIST | T_SET => {
                let (element_type, len) = self.read_list_header()?;
                for _ in 0..len {
                    self.skip(element_type)?;
                }
                Ok(())
            }
            T_MAP => {
                let (key_type, value_type, len) = self.read_map_header()?;
                for _ in 0..len {
                    self.skip(key_type)?;
                    self.skip(value_type)?;
                }
                Ok(())
            }
            other => Err(WireError::Decode(format!("unknown thrift type {other}"))),
        }
    }
}
