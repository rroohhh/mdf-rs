use bitflags::bitflags;
use derivative::Derivative;
use byteorder::{ReadBytesExt, LittleEndian};
use bitvec::prelude::*;

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
            _ => unreachable!()
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Record<'a> {
    ty: RecordType,
    tag_a: RecordTagA,
    tag_b: RecordTagB,
    column_count: u16,
    #[derivative(Debug="ignore")]
    pub fixed_data: &'a [u8],
    null_bitmap: Option<&'a BitSlice<Lsb0, u8>>,
    var_length_columns: Option<VarLengthColumns<'a>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct VarLengthColumns<'a> {
    // Starts at the `variable length column offset array`
    #[derivative(Debug="ignore")]
    data: &'a [u8],
    count: u16
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
    pub fn parse(data: &'a [u8]) -> Self {
        let tag_a = RecordTagA::from_bits(data[0] >> 4).unwrap();
        let tag_b = RecordTagB::from_bits(data[1]).unwrap();
        let ty = RecordType::parse((data[0] & 0xf) >> 1);
        let fixed_data_length = (&data[2..4]).read_u16::<LittleEndian>().unwrap() - 4;
        let mut offset = 4 + fixed_data_length as usize;

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

        Record {
            ty,
            tag_a,
            tag_b,
            fixed_data,
            column_count,
            null_bitmap,
            var_length_columns: var_length_columns_count.map(|count| {
                VarLengthColumns {
                    count,
                    data: &data[offset + 2..],
                }
            })
        }
    }
}