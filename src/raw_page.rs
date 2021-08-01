use crate::Record;
use byteorder::{LittleEndian, ReadBytesExt};
use derivative::Derivative;
use log::{error, trace};
use pretty_hex::{config_hex, HexConfig};
use serde::{Deserialize, Serialize};

pub const PAGE_SIZE: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PagePointer {
    pub page_id: u32,
    pub file_id: u16,
}

impl PagePointer {
    pub(crate) fn parse(data: &[u8]) -> Option<Self> {
        let file_id = (&data[4..6]).read_u16::<LittleEndian>().unwrap();
        if file_id == 0 {
            None
        } else {
            Some(Self {
                page_id: (&data[0..4]).read_u32::<LittleEndian>().unwrap(),
                file_id,
            })
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RecordPointer {
    pub page_ptr: PagePointer,
    pub slot_id: u16,
}

impl RecordPointer {
    pub(crate) fn parse(data: &[u8]) -> Option<Self> {
        let file_id = (&data[4..6]).read_u16::<LittleEndian>().unwrap();
        if file_id == 0 {
            None
        } else {
            Some(Self {
                page_ptr: PagePointer::parse(&data[0..6]).unwrap(),
                slot_id: (&data[6..8]).read_u16::<LittleEndian>().unwrap(),
            })
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PageType {
    UnAlloc,
    Data,
    Index,
    TextMix,
    TextTree,
    Sort,
    GAM,
    SGAM,
    IAM,
    PFS,
    Boot,
    FileHeader,
    DiffMap,
    MLMap,
    CheckDBTemp,
    AlterIndexTemp,
    PreAlloc,
    Unknown(u8),
}

impl PageType {
    fn parse(ty: u8) -> Self {
        match ty {
            0 => PageType::UnAlloc,
            1 => PageType::Data,
            2 => PageType::Index,
            3 => PageType::TextMix,
            4 => PageType::TextTree,
            7 => PageType::Sort,
            8 => PageType::GAM,
            9 => PageType::SGAM,
            10 => PageType::IAM,
            11 => PageType::PFS,
            13 => PageType::Boot,
            15 => PageType::FileHeader,
            16 => PageType::DiffMap,
            17 => PageType::MLMap,
            18 => PageType::CheckDBTemp,
            19 => PageType::AlterIndexTemp,
            20 => PageType::PreAlloc,
            unk => PageType::Unknown(unk),
            //            _ => panic!("unknown page type {}", ty)
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageHeader {
    pub ptr: PagePointer,
    // Number of records in this page
    slot_count: u16,
    // level in the btree, numbered up from 0 at the leaf-level
    // zero for non-index pages
    level: u8,
    // length of the fixed data section in index type records
    pub p_min_len: u16,
    pub ty: PageType,
    pub object_id: u32,
    pub index_id: u16,
    prev_page_ptr: Option<PagePointer>,
    next_page_ptr: Option<PagePointer>,
}

impl PageHeader {
    fn parse(data: &[u8]) -> Self {
        let ptr = Self::parse_ptr(data).unwrap();
        let ty = PageType::parse(data[1]);
        let level = data[3];
        let index_id = (&data[6..8]).read_u16::<LittleEndian>().unwrap();
        let p_min_len = (&data[14..16]).read_u16::<LittleEndian>().unwrap();
        let slot_count = (&data[22..24]).read_u16::<LittleEndian>().unwrap();
        let object_id = (&data[24..28]).read_u32::<LittleEndian>().unwrap();
        let prev_page_ptr = PagePointer::parse(&data[8..14]);
        let next_page_ptr = PagePointer::parse(&data[16..22]);

        Self {
            ptr,
            ty,
            level,
            p_min_len,
            slot_count,
            index_id,
            object_id,
            next_page_ptr,
            prev_page_ptr,
        }
    }

    pub fn parse_ptr(data: &[u8]) -> Option<PagePointer> {
        PagePointer::parse(&data[32..])
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RawPage<'a, T: ?Sized> {
    pub header: PageHeader,
    #[derivative(Debug = "ignore")]
    pub data: &'a [u8],
    #[derivative(Debug = "ignore")]
    pub page_provider: &'a T,
}

impl<'a, T> Clone for RawPage<'a, T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header.clone(),
            data: self.data,
            page_provider: self.page_provider,
        }
    }
}

impl<'a, T: PageProvider> RawPage<'a, T> {
    pub fn parse(data: &'a [u8], page_provider: &'a T) -> Self {
        Self {
            header: PageHeader::parse(data),
            // All the offsets include the 96 byte header, so just use the whole data array
            data: &data[..8192],
            page_provider,
        }
    }

    // number of records on *this* page
    pub fn record_count(&self) -> u16 {
        self.header.slot_count
    }

    // idx is relative to *this* page
    pub fn record(&self, idx: u16) -> Option<Record<'a>> {
        // assert!(idx < self.record_count());
        if idx >= self.record_count() {
            error!(
                "requested a slot idx bigger than our count: {}, {:?}",
                idx, self
            );
            return None;
        }

        let slot_array_position = PAGE_SIZE - 2 * (idx as usize) - 2;
        let offset = (&self.data[slot_array_position..])
            .read_u16::<LittleEndian>()
            .unwrap() as usize;
        trace!("reading record {} at {:x}", idx, offset);
        let cfg = HexConfig {
            width: 32,
            group: 0,
            ..HexConfig::default()
        };
        trace!("{}", config_hex(&(&self.data[offset..]), cfg));
        Record::parse(
            &self.data[offset..],
            self.header.ty == PageType::Index,
            self.header.p_min_len,
        )
    }

    pub fn records(&self) -> impl Iterator<Item = Record<'a>> {
        RecordIterator::new((*self).clone(), false)
    }

    pub fn local_records(&self) -> impl Iterator<Item = Record<'a>> {
        RecordIterator::new((*self).clone(), true)
    }

    pub fn into_records(self) -> impl Iterator<Item = Record<'a>> {
        RecordIterator::new(self, false)
    }
}

struct RecordIterator<'a, T> {
    current_page: RawPage<'a, T>,
    // idx (on this page) of the record we will present next
    idx: u16,
    local: bool,
}

impl<'a, T> RecordIterator<'a, T> {
    fn new(start_page: RawPage<'a, T>, local: bool) -> Self {
        Self {
            current_page: start_page,
            idx: 0,
            local,
        }
    }
}

impl<'a, T: PageProvider> Iterator for RecordIterator<'a, T> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.current_page.record_count() {
            match self.current_page.header.next_page_ptr {
                Some(ptr) if !self.local => match self.current_page.page_provider.get(ptr) {
                    Some(next_page) => {
                        self.current_page = next_page;
                        self.idx = 0;
                    }
                    None => return None,
                },
                _ => return None,
            }
        }

        trace!("reading record {} from {:#?}", self.idx, self.current_page);
        let record = self.current_page.record(self.idx);
        self.idx += 1;
        record
    }
}

pub trait PageProvider: Sized {
    fn file_ids(&self) -> Vec<u16>;

    fn num_pages(&self, file_id: u16) -> u32;

    fn get(&self, ptr: PagePointer) -> Option<RawPage<Self>>;

    fn get_record(&self, ptr: RecordPointer) -> Option<Record> {
        self.get(ptr.page_ptr)
            .and_then(|page| page.record(ptr.slot_id))
    }
}
