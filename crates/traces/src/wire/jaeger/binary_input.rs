use super::{
    BT_BINARY, BT_BOOL, BT_BYTE, BT_DOUBLE, BT_I16, BT_I32, BT_I64, BT_LIST, BT_MAP, BT_SET,
    BT_STOP, BT_STRUCT, WireError,
};

pub(crate) struct BinaryInput<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> BinaryInput<'a> {
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
        Ok((element_type, len))
    }

    pub(crate) fn read_map_header(&mut self) -> Result<(u8, u8, usize), WireError> {
        let key_type = self.read_u8()?;
        let value_type = self.read_u8()?;
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("map length out of range".into()))?;
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
        match field_type {
            BT_STOP => Ok(()),
            BT_BOOL | BT_BYTE => self.read_u8().map(|_| ()),
            BT_I16 => self.read_i16().map(|_| ()),
            BT_I32 => self.read_i32().map(|_| ()),
            BT_I64 => self.read_i64().map(|_| ()),
            BT_DOUBLE => self.read_double().map(|_| ()),
            BT_BINARY => self.read_binary().map(|_| ()),
            BT_STRUCT => {
                while let Some((inner_type, _)) = self.read_field()? {
                    self.skip(inner_type)?;
                }
                Ok(())
            }
            BT_LIST | BT_SET => {
                let (element_type, len) = self.read_list_header()?;
                for _ in 0..len {
                    self.skip(element_type)?;
                }
                Ok(())
            }
            BT_MAP => {
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
