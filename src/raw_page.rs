use byteorder::{LittleEndian, ReadBytesExt};
use crate::Record;
use std::hint::unreachable_unchecked;

pub const PAGE_SIZE: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PagePointer {
    pub page_id: u32,
    pub file_id: u16,
}

impl PagePointer {
    pub(crate) fn parse(data: &[u8]) -> Self {
        Self {
            page_id: (&data[0..4]).read_u32::<LittleEndian>().unwrap(),
            file_id: (&data[4..6]).read_u16::<LittleEndian>().unwrap(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PageType {
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
    PreAlloc
}

impl PageType {
    fn parse(ty: u8) -> Self {
        match ty {
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
            _ => panic!("unknown page type {}", ty)
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
    pub ty: PageType,
    prev_page_ptr: Option<PagePointer>,
    next_page_ptr: Option<PagePointer>,
}

impl PageHeader {
    fn parse(data: &[u8]) -> Self {
        let ptr = Self::parse_ptr(data);
        let ty = PageType::parse(data[1]);
        let level = data[3];
        let slot_count = (&data[22..24]).read_u16::<LittleEndian>().unwrap();
        let prev_page_ptr= match PagePointer::parse(&data[8..14]) {
            PagePointer { page_id: 0, .. } => None,
            ptr @ _ => Some(ptr)
        };
        let next_page_ptr= match PagePointer::parse(&data[16..22]) {
            PagePointer { page_id: 0, .. } => None,
            ptr @ _ => Some(ptr)
        };

        Self {
            ptr,
            ty,
            level,
            slot_count,
            next_page_ptr,
            prev_page_ptr,
        }
    }

    pub fn parse_ptr(data: &[u8]) -> PagePointer {
        PagePointer::parse(&data[32..])
    }
}

#[derive(Debug)]
pub struct RawPage<'a, T: ?Sized> {
    pub header: PageHeader,
    pub data: &'a [u8],
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
            page_provider
        }
    }

    // number of records on *this* page
    pub fn record_count(&self) -> u16 {
        self.header.slot_count
    }

    // idx is relative to *this* page
    pub fn record(&self, idx: u16) -> Record<'a> {
        assert!(idx < self.record_count());

        let slot_array_position = PAGE_SIZE - 2 * (idx as usize) - 2;
        let offset = (&self.data[slot_array_position..]).read_u16::<LittleEndian>().unwrap() as usize;
        Record::parse(&self.data[offset..])
    }

    pub fn records(&'a self) -> impl Iterator<Item = Record<'a>> {
        RecordIterator::new((*self).clone())
    }
}

struct RecordIterator<'a, T> {
    current_page: RawPage<'a, T>,
    // idx (on this page) of the record we will present next
    idx: u16
}

impl<'a, T> RecordIterator<'a, T> {
    fn new(start_page: RawPage<'a, T>) -> Self {
        Self {
            current_page: start_page,
            idx: 0
        }
    }
}

impl<'a, T: PageProvider> Iterator for RecordIterator<'a, T> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.current_page.record_count() {
            match dbg!(self.current_page.header.next_page_ptr) {
                Some(ptr) => {
                    self.current_page = self.current_page.page_provider.get(ptr);
                    dbg!(&self.current_page.header);
                    self.idx = 0 ;
                },
                None => return None
            }
        }

        let record = self.current_page.record(self.idx);
        self.idx += 1;
        Some(record)
    }
}

pub trait PageProvider {
    fn get(&self, ptr: PagePointer) -> RawPage<Self>;
}
