use bitflags::bitflags;
use bitvec::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt};
use derivative::Derivative;
use log::{error, trace};

#[derive(Debug)]
enum RecordType {
    Primary,
    Forwarded,
    Forwarding,
    Index,
    Blob,
    GhostIndex,
    GhostData,
    GhostVersion,
}

impl RecordType {
    fn parse(num: u8) -> Self {
        match num {
            0 => RecordType::Primary,
            1 => RecordType::Forwarded,
            2 => RecordType::Forwarding,
            3 => RecordType::Index,
            4 => RecordType::Blob,
            5 => RecordType::GhostIndex,
            6 => RecordType::GhostData,
            7 => RecordType::GhostVersion,
            _ => unreachable!(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Record<'a> {
    ty: RecordType,
    tag_a: RecordTagA,
    tag_b: RecordTagB,
    pub column_count: u16,
    #[derivative(Debug = "ignore")]
    pub fixed_data: &'a [u8],
    null_bitmap: Option<&'a BitSlice<Lsb0, u8>>,
    pub var_length_columns: Option<VarLengthColumns<'a>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct VarLengthColumns<'a> {
    // Starts at the `variable length column offset array`
    #[derivative(Debug = "ignore")]
    data: &'a [u8],
    pub count: u16,
    base_offset: usize,
}

pub struct VarLengthColumnOffset {
    end: u16,
    complex: bool,
}

impl VarLengthColumnOffset {
    fn parse(bytes: &[u8]) -> Self {
        let value = (&bytes[0..2]).read_u16::<LittleEndian>().unwrap();
        let end = value & 0x7fff;
        let complex = (value & 0x8000) != 0;
        Self { end, complex }
    }
}

impl<'a> VarLengthColumns<'a> {
    // Get data of the `idx`th column
    pub fn get(&self, idx: u16) -> (bool, &'a [u8]) {
        // If we want a bigger index than we support the value is null by definition
        // assert!(idx < self.count);
        if idx >= self.count {
            // We don't really know if its complex or not, lets hope this works
            (false, &[])
        } else {
            let start = if idx == 0 {
                // There are two bytes for each var length column in offsets,
                // after that the values start
                2 * self.count as usize
            } else {
                let prev_idx = idx as usize - 1;
                VarLengthColumnOffset::parse(&self.data[2 * prev_idx..2 * (prev_idx + 1)]).end
                    as usize
                    - self.base_offset
            };
            let idx = idx as usize;
            let end = VarLengthColumnOffset::parse(&self.data[2 * idx..2 * (idx + 1)]);
            let end_offs = end.end as usize - self.base_offset;

            (end.complex, &self.data[start..end_offs])
        }
    }
}

bitflags! {
    pub struct RecordTagA: u8 {
        const HAS_NULL_BITMAP        = 1 << 0;
        const HAS_VAR_LENGTH_COLUMNS = 1 << 1;
        const HAS_VERSIONING_TAG     = 1 << 2;
        const HAS_VALID_TAG_B        = 1 << 3;
    }
}

bitflags! {
    pub struct RecordTagB: u8 {
        const IS_GHOST_FORWARDED     = 1 << 0;
    }
}

impl<'a> Record<'a> {
    pub fn has_null_bitmap(&self) -> bool {
        self.tag_a.contains(RecordTagA::HAS_NULL_BITMAP)
    }

    pub fn has_var_length_columns(&self) -> bool {
        self.tag_a.contains(RecordTagA::HAS_VAR_LENGTH_COLUMNS)
    }

    pub fn is_column_null(&self, idx: u16) -> bool {
        self.null_bitmap.map(|v| v[idx as usize]).unwrap_or(false)
    }

    pub fn parse(data: &'a [u8], is_index: bool, p_min_len: u16) -> Option<Self> {
        let tag_a = RecordTagA::from_bits(data[0] >> 4).unwrap();

        let tag_b = if is_index {
            RecordTagB::empty()
        } else {
            // Seems there are some unknown bits
            RecordTagB::from_bits_truncate(data[1])
        };

        let ty = RecordType::parse((data[0] & 0xf) >> 1);

        // Other record types are currently not supported
        assert!(matches!(
            ty,
            RecordType::Primary | RecordType::Index | RecordType::Blob
        ));

        let fixed_data_length = if is_index {
            p_min_len - 1
        } else {
            let offs = (&data[2..4]).read_u16::<LittleEndian>().unwrap();
            if offs < 4 {
                error!("something is fucked, the fixed data len is smaller than < 4: {}, {:?}, {:?}, {:?}", offs, ty, tag_a, tag_b);
                return None;
            }
            offs - 4
        };
        let mut offset = if is_index {
            p_min_len as usize
        } else {
            4 + fixed_data_length as usize
        };

        if offset > data.len() {
            error!(
                "something is fucked, we got a fixed data offset of {} > {}",
                offset,
                data.len()
            );
            return None;
        }

        let column_count = (&data[offset..]).read_u16::<LittleEndian>().unwrap();
        offset += 2;

        let null_bitmap = if tag_a.contains(RecordTagA::HAS_NULL_BITMAP) {
            let null_bitmap_bytes = (column_count as usize + 7) / 8;
            let bitslice = BitSlice::from_slice(&data[offset..offset + null_bitmap_bytes]).unwrap();
            offset += null_bitmap_bytes;
            Some(bitslice)
        } else {
            None
        };

        let var_length_columns_count = if tag_a.contains(RecordTagA::HAS_VAR_LENGTH_COLUMNS) {
            Some((&data[offset..]).read_u16::<LittleEndian>().unwrap())
        } else {
            None
        };

        let fixed_data = &data[4..fixed_data_length as usize + 4];
        trace!("record has {} bytes of fixed_data", fixed_data_length);

        Some(Record {
            ty,
            tag_a,
            tag_b,
            fixed_data,
            column_count,
            null_bitmap,
            var_length_columns: var_length_columns_count.map(|count| VarLengthColumns {
                count,
                data: &data[offset + 2..],
                base_offset: offset + 2,
            }),
        })
    }
}
