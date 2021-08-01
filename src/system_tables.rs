use crate::{create_row_parser, PagePointer, ValueOrLob};
use bitflags::bitflags;

// All the system tables are made to copy data, as they are stored in the database, which
// is very hard to do because it requires a self-referential struct
// They should be very small anyways

pub const SYS_ROW_SET_AUID: i64 = 327680;
pub const SYS_SCH_OBJS_IDMAJOR: i32 = 34;
pub const SYS_COL_PARS_IDMAJOR: i32 = 41;
pub const SYS_SCALAR_TYPES_IDMAJOR: i32 = 50;
// OrcaMDF uses this
// pub const SYS_RS_COLS_IDMAJOR: i32 = 3;
// but we only have a sysrowsetcolumns with a IDMAJOR = 4
pub const SYS_RS_COLS_IDMAJOR: i32 = 4;
pub const SYS_SINGLE_OBJECT_REFS_IDMAJOR: i32 = 74;

#[derive(Debug, PartialEq, Eq)]
pub enum AllocUnitType {
    Dropped,
    InRowData,
    LobData,
    RowOverflowData,
}

impl AllocUnitType {
    fn parse(value: i8) -> Self {
        match value {
            0 => Self::Dropped,
            1 => Self::InRowData,
            2 => Self::LobData,
            3 => Self::RowOverflowData,
            _ => panic!("unknown SysAllocUnitType {}", value),
        }
    }
}

create_row_parser!(
    struct SysAllocUnit {
        au_id: i64,
        ty: AllocUnitType = [TinyInt] TinyInt(v) => AllocUnitType::parse(v),
        owner_id: i64,
        status: i32,
        fgid: i16,
        pg_first: Option<PagePointer> = [Binary(6)] Binary(v) => PagePointer::parse(v),
        pg_root: Option<PagePointer> = [Binary(6)] Binary(v) => PagePointer::parse(v),
        pg_firstiam: Option<PagePointer> = [Binary(6)] Binary(v) => PagePointer::parse(v),
        pc_used: i64,
        pc_data: i64,
        pc_reserved: i64,
        db_frag_id: i32[?],
    }
);

create_row_parser!(
    struct SysRowSet {
        row_set_id: i64,
        owner_type: i8,
        id_major: i32,
        id_minor: i32,
        num_part: i32,
        status: i32,
        fgidfs: i16,
        rcrows: i64,
        cmpr_level: i8[?],
        fill_fact: i8[?],
        max_leaf: i32[?],
        max_int: i16[?],
        min_leaf: i16[?],
        min_int: i16[?],
        rs_guid: ValueOrLob<Vec<u8>>[?] = [VarBinary(None)] VarBinary(data) => data.map(|bytes| bytes.to_vec()),
        lock_res: ValueOrLob<Vec<u8>>[?] = [VarBinary(None)] VarBinary(data) => data.map(|bytes| bytes.to_vec()),
        db_frag_id: i32[?],
    }
);

#[derive(Debug, Eq, PartialEq)]
pub enum SchType {
    SystemTable,
    SqlScalarFunction,
    UserTable,
    ServiceQueue,
    InternalTable,
    DefaultConstraint,
    // Just guessing with these two
    PrimaryKey,
    StoredProcedure,
    Unique,
    SqlTableFunction,
    View,
    Trigger,
}

impl SchType {
    fn parse(ty: &str) -> Self {
        match ty {
            "S " => Self::SystemTable,
            "FN" => Self::SqlScalarFunction,
            "U " => Self::UserTable,
            "SQ" => Self::ServiceQueue,
            "IT" => Self::InternalTable,
            "D " => Self::DefaultConstraint,
            "PK" => Self::PrimaryKey,
            "P " => Self::StoredProcedure,
            "UQ" => Self::Unique,
            "IF" => Self::SqlTableFunction,
            "V " => Self::View,
            "TR" => Self::Trigger,
            _ => panic!("unknown SchType {}", ty),
        }
    }
}

create_row_parser!(
    struct SysSchObj {
        id: i32,
        name: String = [SysName] SysName(v) => v,
        ns_id: i32,
        ns_class: i8,
        status: i32,
        ty: SchType = [Char(2)] Char(v) => SchType::parse(v),
        pid: i32,
        pcall: i8,
        int_prop: i32,
        created: chrono::NaiveDateTime = [DateTime] DateTime(v) => v,
        modified: chrono::NaiveDateTime = [DateTime] DateTime(v) => v
    }
);

bitflags! {
    pub struct ColParStatus: i32 {
        const NULLABLE           = 1 << 0;
        const ANSI_PADDED        = 1 << 1;
        const IDENTIYT           = 1 << 2;
        const ROW_GUID_COL       = 1 << 3;
        const COMPUTED           = 1 << 4;
        const FILESTREAM         = 1 << 5;
        const XML_DOCUMENT       = 1 << 11;
        const REPLICATED         = 1 << 17;
        const NON_SQL_SUBSCRIBED = 1 << 18;
        const MERGE_PUBLISHED    = 1 << 19;
        const DTS_REPLICATED     = 1 << 21;
        const SPARSE             = 1 << 24;
        const COLUMN_SET         = 1 << 25;
    }
}

create_row_parser!(
    struct SysColPar {
        id: i32,
        number: i16,
        col_id: i32,
        name: String[?] = [SysName] SysName(v) => v,
        xtype: i8,
        utype: i32,
        length: i16,
        prec: i8,
        scale: i8,
        collation_id: i32,
        status: ColParStatus = [Int] Int(i) => ColParStatus::from_bits_truncate(i),
        max_in_row: i16,
        xml_ns: i32,
        dflt: i32,
        chk: i32,
        idt_val: ValueOrLob<Vec<u8>>[?] = [VarBinary(None)] VarBinary(v) => v.map(|bytes| bytes.to_vec()),
    }
);

create_row_parser!(
    struct SysScalarType {
        id: i32,
        sch_id: i32,
        name: String = [SysName] SysName(v) => v,
        xtype: i8,
        length: i16,
        prec: i8,
        scale: i8,
        collation_id: i32,
        status: i32,
        created: chrono::NaiveDateTime = [DateTime] DateTime(v) => v,
        modified: chrono::NaiveDateTime = [DateTime] DateTime(v) => v,
        dflt: i32,
        chk: i32,
    }
);

create_row_parser!(
    struct SysRsCol {
        row_set_id: i64,
        row_set_col_id: i32,
        hobt_col_id: i32,
        status: i32,
        rc_modified: i64,
        max_in_row_len: i16,
    }
);
/*

       ti: i32,
       c_id: i32,
       ord_key: i16,
       max_in_row_len: i16,
       status: i32,
       offset: i32,
       null_bit: i32,
       bit_pos: i16,
       col_guid: Vec<u8>[?] = [VarBinary(Some(16))] VarBinary(v) => v.to_vec(),
       db_frag_id: i32[?]
*/

create_row_parser!(
    struct SysSingleObjRef {
        class: i8,
        dep_id: i32,
        dep_sub_id: i32,
        in_dep_id: i32,
        in_dep_sub_id: i32,
        status: i32,
    }
);
