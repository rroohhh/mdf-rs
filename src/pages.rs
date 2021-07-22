use crate::{PagePointer, RawPage, PageProvider, PageType};
use byteorder::{LittleEndian, ReadBytesExt};
use crate::util::parse_utf16_string;

#[derive(Debug)]
pub struct BootPage {
    version: u16,
    create_version: u16,
    status: u32,
    next_id: u32,
    database_name: String,
    db_id: u16,
    max_db_timestamp: u64,
    pub first_sys_indices: PagePointer,
}

impl BootPage {
    pub fn parse<T: PageProvider>(page: RawPage<T>) -> Self {
        assert_eq!(page.header.ty, PageType::Boot);

        let data = page.record(0).fixed_data;
        let version = (&data[..2]).read_u16::<LittleEndian>().unwrap();
        let create_version = (&data[2..4]).read_u16::<LittleEndian>().unwrap();
        let status = (&data[32..36]).read_u32::<LittleEndian>().unwrap();
        let next_id = (&data[36..40]).read_u32::<LittleEndian>().unwrap();
        let database_name = parse_utf16_string(&data[48..304]);
        let db_id = (&data[308..310]).read_u16::<LittleEndian>().unwrap();
        let max_db_timestamp = (&data[312..320]).read_u64::<LittleEndian>().unwrap();
        let first_sys_indices = PagePointer::parse(&data[512..518]);

        Self {
            version, create_version, status, next_id, database_name,
            db_id, max_db_timestamp, first_sys_indices
        }
    }
}