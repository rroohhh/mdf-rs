use crate::{PageProvider, Record, RecordPointer};
use byteorder::{LittleEndian, ReadBytesExt};
use derivative::Derivative;
use log::{error, warn};
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(Debug)]
pub struct LobDataBlocks<'a> {
    pub data_blocks: Vec<(u64, &'a [u8])>,
}

impl<'a> LobDataBlocks<'a> {
    pub fn write_to_file(&self, filename: &str) -> Result<(), std::io::Error> {
        let mut file = std::fs::File::create(filename)?;
        let mut last_offs = 0;
        for data_block in &self.data_blocks {
            if data_block.0 > last_offs {
                // rather allow out of order block than not dumping anything
                // let length = data_block.0 - last_offs;
                // assert_eq!(data_block.1.len(), length as usize);
                warn!("potentially out of order data blocks for {}", filename);
            } else {
                // it seems that this can happen for very big files
                warn!("potentially out of order data blocks for {}", filename);
            }

            file.write_all(data_block.1)?;

            last_offs = data_block.0;
        }

        Ok(())
    }

    pub fn length(&self) -> u32 {
        let mut len = 0;
        for (_, data) in &self.data_blocks {
            len += data.len()
        }
        len as u32
    }
}

#[derive(Debug)]
pub struct LobPointer {
    timestamp: u32,
    ptr: RecordPointer,
}

impl LobPointer {
    pub fn parse(data: &[u8]) -> Self {
        Self {
            timestamp: (&data[0..4]).read_u32::<LittleEndian>().unwrap(),
            ptr: RecordPointer::parse(&data[8..16]).unwrap(),
        }
    }

    // TODO(robin): refactor!!!
    pub fn read<'a, T: PageProvider>(&self, page_provider: &'a T) -> Option<LobDataBlocks<'a>> {
        let record = page_provider.get_record(self.ptr)?;
        let mut entries = vec![LobEntry::parse(record)?];
        let mut data_blocks = vec![];

        while !entries.is_empty() {
            let mut new_entries = vec![];
            for entry in entries {
                match &entry {
                    LobEntry::SmallRoot(LobSmallRoot { data, .. })
                    | LobEntry::Data(LobData { data, .. }) => {
                        // this can basically only happen at the first entry
                        data_blocks.push((data.len() as u64, *data));
                    }
                    _ => {
                        for (offs, entry) in entry.sub_entries(page_provider) {
                            let entry = entry?;
                            match &entry {
                                LobEntry::SmallRoot(LobSmallRoot { data, .. })
                                | LobEntry::Data(LobData { data, .. }) => {
                                    data_blocks.push((offs, *data));
                                }
                                _ => new_entries.push(entry),
                            }
                        }
                    }
                }
            }
            entries = new_entries;
        }

        Some(LobDataBlocks { data_blocks })
    }
}

#[derive(Debug)]
pub enum LobEntry<'a> {
    SmallRoot(LobSmallRoot<'a>),
    LargeRootYukon(LobLargeRootYukon<'a>),
    Data(LobData<'a>),
    Internal(LobInternal<'a>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum LobType {
    SmallRoot,
    LargeRootYukon,
    Data,
    Internal,
    Null,
}

impl LobType {
    fn parse(record: &Record) -> Option<Self> {
        let ty = (&record.fixed_data[8..10])
            .read_u16::<LittleEndian>()
            .unwrap();
        match ty {
            0 => Some(Self::SmallRoot),
            // 1 => Self::LargeRoot,
            2 => Some(Self::Internal),
            3 => Some(Self::Data),
            // 4 => Self::LargeRootShiloh,
            5 => Some(Self::LargeRootYukon),
            // 6 => Self::SuperLargeRoot,
            8 => Some(Self::Null),
            _ => {
                error!("unknown lob type {}", ty);
                None
            }
        }
    }
}

impl<'a> LobEntry<'a> {
    pub fn parse(record: Record<'a>) -> Option<Self> {
        LobType::parse(&record).and_then(|ty| match ty {
            LobType::SmallRoot => Some(Self::SmallRoot(LobSmallRoot::parse(record)?)),
            LobType::LargeRootYukon => {
                Some(Self::LargeRootYukon(LobLargeRootYukon::parse(record)?))
            }
            LobType::Data => Some(Self::Data(LobData::parse(record)?)),
            LobType::Internal => Some(Self::Internal(LobInternal::parse(record)?)),
            LobType::Null => None,
        })
    }

    pub fn sub_entries<'b, T: PageProvider>(
        &'b self,
        page_provider: &'a T,
    ) -> LobEntrySubEntryIterator<'a, 'b, T> {
        LobEntrySubEntryIterator::new(self, page_provider)
    }
}

pub struct LobEntrySubEntryIterator<'a, 'b, T> {
    page_provider: &'a T,
    lob_entry: &'b LobEntry<'a>,
    idx: u16,
}

impl<'a, 'b, T> LobEntrySubEntryIterator<'a, 'b, T> {
    fn new(lob_entry: &'b LobEntry<'a>, page_provider: &'a T) -> Self {
        Self {
            page_provider,
            lob_entry,
            idx: 0,
        }
    }
}

impl<'a, 'b, T: PageProvider> Iterator for LobEntrySubEntryIterator<'a, 'b, T> {
    type Item = (u64, Option<LobEntry<'a>>);

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.lob_entry {
            LobEntry::LargeRootYukon(root) => root.read(self.page_provider, self.idx),
            LobEntry::Internal(internal) => internal.read(self.page_provider, self.idx),
            _ => panic!("cannot read subentries of {:?}", self.lob_entry),
        };

        self.idx += 1;

        item
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct LobSmallRoot<'a> {
    blob_id: u64,
    ty: LobType,
    length: u16,
    #[derivative(Debug = "ignore")]
    pub data: &'a [u8],
}

impl<'a> LobSmallRoot<'a> {
    fn parse(record: Record<'a>) -> Option<Self> {
        let blob_id = (&record.fixed_data[..8])
            .read_u64::<LittleEndian>()
            .unwrap();
        let ty = LobType::parse(&record)?;
        assert_eq!(ty, LobType::SmallRoot);

        let length = (&record.fixed_data[10..12])
            .read_u16::<LittleEndian>()
            .unwrap();

        Some(Self {
            blob_id,
            ty,
            length,
            data: &record.fixed_data[16..16 + length as usize],
        })
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct SizedRecordPointer {
    size: u32,
    ptr: RecordPointer,
}

impl SizedRecordPointer {
    fn parse(data: &[u8]) -> Self {
        Self {
            size: (&data[0..4]).read_u32::<LittleEndian>().unwrap(),
            ptr: RecordPointer::parse(&data[4..]).unwrap(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RecordPointerWithOffset {
    offset: u64,
    ptr: RecordPointer,
}

impl RecordPointerWithOffset {
    fn parse(data: &[u8]) -> Self {
        Self {
            offset: (&data[0..8]).read_u64::<LittleEndian>().unwrap(),
            ptr: RecordPointer::parse(&data[8..]).unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct LobLargeRootYukon<'a> {
    pub blob_id: u64,
    ty: LobType,
    pub max_links: u16,
    pub cur_links: u16,
    pub level: u16,
    record: Record<'a>,
}

impl<'a> LobLargeRootYukon<'a> {
    fn parse(record: Record<'a>) -> Option<Self> {
        let blob_id = (&record.fixed_data[..8])
            .read_u64::<LittleEndian>()
            .unwrap();
        let ty = LobType::parse(&record)?;
        assert_eq!(ty, LobType::LargeRootYukon);

        let max_links = (&record.fixed_data[10..12])
            .read_u16::<LittleEndian>()
            .unwrap();
        let cur_links = (&record.fixed_data[12..14])
            .read_u16::<LittleEndian>()
            .unwrap();
        let level = (&record.fixed_data[14..16])
            .read_u16::<LittleEndian>()
            .unwrap();

        Some(Self {
            blob_id,
            ty,
            max_links,
            cur_links,
            level,
            record,
        })
    }

    pub fn read_idx(&self, idx: u16) -> Option<RecordPointer> {
        if idx >= self.cur_links {
            None
        } else {
            let idx = idx as usize;
            Some(
                SizedRecordPointer::parse(
                    &self.record.fixed_data[20 + 12 * idx..20 + 12 * (idx + 1)],
                )
                .ptr,
            )
        }
    }

    fn read<T: PageProvider>(
        &self,
        page_provider: &'a T,
        idx: u16,
    ) -> Option<(u64, Option<LobEntry<'a>>)> {
        if idx >= self.cur_links {
            None
        } else {
            let idx = idx as usize;
            let ptr = SizedRecordPointer::parse(
                &self.record.fixed_data[20 + 12 * idx..20 + 12 * (idx + 1)],
            );
            Some((
                ptr.size as u64,
                Some(LobEntry::parse(page_provider.get_record(ptr.ptr)?)?),
            ))
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct LobData<'a> {
    blob_id: u64,
    ty: LobType,
    #[derivative(Debug = "ignore")]
    pub data: &'a [u8],
}

impl<'a> LobData<'a> {
    fn parse(record: Record<'a>) -> Option<Self> {
        let blob_id = (&record.fixed_data[..8])
            .read_u64::<LittleEndian>()
            .unwrap();
        let ty = LobType::parse(&record)?;
        assert_eq!(ty, LobType::Data);

        Some(Self {
            blob_id,
            ty,
            data: &record.fixed_data[10..],
        })
    }
}

#[derive(Debug)]
pub struct LobInternal<'a> {
    pub blob_id: u64,
    ty: LobType,
    pub max_links: u16,
    pub cur_links: u16,
    pub level: u16,
    record: Record<'a>,
}

impl<'a> LobInternal<'a> {
    fn parse(record: Record<'a>) -> Option<Self> {
        let blob_id = (&record.fixed_data[..8])
            .read_u64::<LittleEndian>()
            .unwrap();
        let ty = LobType::parse(&record)?;
        assert_eq!(ty, LobType::Internal);

        let max_links = (&record.fixed_data[10..12])
            .read_u16::<LittleEndian>()
            .unwrap();
        let cur_links = (&record.fixed_data[12..14])
            .read_u16::<LittleEndian>()
            .unwrap();
        let level = (&record.fixed_data[14..16])
            .read_u16::<LittleEndian>()
            .unwrap();

        Some(Self {
            blob_id,
            ty,
            max_links,
            cur_links,
            level,
            record,
        })
    }

    pub fn read_idx(&self, idx: u16) -> Option<RecordPointer> {
        if idx >= self.cur_links {
            None
        } else {
            let idx = idx as usize;
            Some(
                RecordPointerWithOffset::parse(
                    &self.record.fixed_data[16 * (idx + 1)..16 * (idx + 2)],
                )
                .ptr,
            )
        }
    }

    fn read<T: PageProvider>(
        &self,
        page_provider: &'a T,
        idx: u16,
    ) -> Option<(u64, Option<LobEntry<'a>>)> {
        if idx >= self.cur_links {
            None
        } else {
            let idx = idx as usize;
            let ptr = RecordPointerWithOffset::parse(
                &self.record.fixed_data[16 * (idx + 1)..16 * (idx + 2)],
            );
            Some((
                ptr.offset,
                Some(LobEntry::parse(page_provider.get_record(ptr.ptr)?)?),
            ))
        }
    }
}
