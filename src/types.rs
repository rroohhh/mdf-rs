use crate::util::parse_utf16_string;
use crate::{ColParStatus, LobPointer, Record, SysColPar, SysScalarType};
use byteorder::{LittleEndian, ReadBytesExt};
use log::trace;
use std::io::Cursor;

#[derive(Debug)]
pub enum SqlType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Binary(usize),
    Char(usize),
    NChar(usize),
    VarBinary(Option<usize>),
    VarChar(Option<usize>),
    Bit,
    SqlVariant,
    NVarChar,
    SysName,
    DateTime,
    SmallDateTime,
    UniqueIdentifier,
    Image,
    NText,
    Float,
}

impl SqlType {
    pub fn from_col(col: &SysColPar, ty: &SysScalarType) -> Self {
        match ty.name.as_str() {
            "tinyint" => Self::TinyInt,
            "smallint" => Self::SmallInt,
            "int" => Self::Int,
            "bigint" => Self::BigInt,
            "binary" => Self::Binary(col.length as usize),
            "char" => Self::Char(col.length as usize),
            "nchar" => Self::NChar(col.length as usize),
            "varbinary" => Self::VarBinary(Some(col.length as usize)),
            "varchar" => Self::VarChar(Some(col.length as usize)),
            "bit" => Self::Bit,
            "nvarchar" => Self::NVarChar,
            "sysname" => Self::SysName,
            "uniqueidentifier" => Self::UniqueIdentifier,
            "datetime" => Self::DateTime,
            "sql_variant" => Self::SqlVariant,
            "image" => Self::Image,
            "ntext" => Self::NText,
            "float" => Self::Float,
            "smalldatetime" => Self::SmallDateTime,
            _ => panic!("unknown column type\n{:?}\n{:?}", col, ty),
        }
    }

    pub fn is_var_length(&self) -> bool {
        use SqlType::*;
        match self {
            TinyInt | SmallInt | Int | BigInt | Binary(_) | Char(_) | NChar(_) | DateTime
            | UniqueIdentifier | Bit | Float | SmallDateTime => false,
            VarBinary(_) | VarChar(_) | SysName | NVarChar | SqlVariant | Image | NText => true,
        }
    }

    // TODO(robin): think of way to consolidate these two
    pub fn parse_var_length<'a>(&self, complex: bool, data: &'a [u8]) -> SqlValue<'a> {
        match self {
            Self::VarBinary(max_size) => {
                SqlValue::VarBinary(if complex {
                    ValueOrLob::Lob(LobPointer::parse(data))
                } else {
                    if let Some(max_size) = max_size {
                        if data.len() > *max_size {
                            println!(
                                "got a value longer than we wanted {} > {}",
                                data.len(),
                                *max_size
                            );
                        }
                        // Maybe this is actually allowed?
                        // what is the meaning of it then?
                        // assert!(data.len() <= *max_size);
                    }
                    ValueOrLob::Value(data)
                })
            }
            Self::VarChar(max_size) => {
                assert!(!complex);

                if let Some(max_size) = max_size {
                    assert!(data.len() <= *max_size);
                }
                SqlValue::VarChar(data)
            }
            Self::Image => SqlValue::Image(if !data.is_empty() {
                assert!(complex);
                assert_eq!(data.len(), 16);
                Some(LobPointer::parse(data))
            } else {
                None
            }),
            Self::NText => SqlValue::Image(if !data.is_empty() {
                assert!(complex);
                assert_eq!(data.len(), 16);
                Some(LobPointer::parse(data))
            } else {
                None
            }),
            Self::SysName => {
                assert!(!complex);
                SqlValue::SysName(parse_utf16_string(data))
            }
            Self::NVarChar => SqlValue::NVarChar(if complex {
                ValueOrLob::Lob(LobPointer::parse(data))
            } else {
                ValueOrLob::Value(parse_utf16_string(data))
            }),
            // TODO(robin): proper parsing
            Self::SqlVariant => {
                assert!(!complex);
                SqlValue::SqlVariant(data)
            }
            _ => panic!(
                "cannot parse fixed length type using `parse_var_length`: {:?}",
                self
            ),
        }
    }

    pub fn parse<'a>(
        &self,
        bit_parser: &mut BitParser,
        cursor: &mut Cursor<&'a [u8]>,
    ) -> SqlValue<'a> {
        match self {
            Self::TinyInt => SqlValue::TinyInt(cursor.read_i8().unwrap()),
            Self::SmallInt => SqlValue::SmallInt(cursor.read_i16::<LittleEndian>().unwrap()),
            Self::Int => SqlValue::Int(cursor.read_i32::<LittleEndian>().unwrap()),
            Self::BigInt => SqlValue::BigInt(cursor.read_i64::<LittleEndian>().unwrap()),
            Self::Bit => SqlValue::Bit(bit_parser.read_bit(cursor)),
            Self::Float => SqlValue::Float(cursor.read_f64::<LittleEndian>().unwrap()),
            Self::UniqueIdentifier => {
                SqlValue::UniqueIdentifier(cursor.read_u128::<LittleEndian>().unwrap())
            }
            Self::DateTime => {
                let time = cursor.read_i32::<LittleEndian>().unwrap();
                let date = cursor.read_i32::<LittleEndian>().unwrap();
                let mut dt = chrono::NaiveDate::from_ymd(1900, 1, 1).and_hms(0, 0, 0);
                // TODO(robin): wtf is happening here??
                if date < 1_000_000 && date > 0 {
                    dt += chrono::Duration::days(date as i64);
                }
                dt += chrono::Duration::milliseconds((time as i64) * 1000 / 300);

                SqlValue::DateTime(dt)
            }
            Self::SmallDateTime => {
                let time = cursor.read_u16::<LittleEndian>().unwrap();
                let date = cursor.read_u16::<LittleEndian>().unwrap();
                let mut dt = chrono::NaiveDate::from_ymd(1900, 1, 1).and_hms(0, 0, 0);
                dt += chrono::Duration::days(date as i64);
                dt += chrono::Duration::minutes(time as i64);

                SqlValue::DateTime(dt)
            }
            Self::Binary(size) => {
                let pos = cursor.position() as usize;
                let ret = SqlValue::Binary(&cursor.get_ref()[pos..pos + size]);
                cursor.set_position((pos + size) as u64);
                ret
            }
            Self::Char(size) => {
                let pos = cursor.position() as usize;
                let ret = SqlValue::Char(
                    std::str::from_utf8(&cursor.get_ref()[pos..pos + size]).unwrap(),
                );
                cursor.set_position((pos + size) as u64);
                ret
            }
            Self::NChar(size) => {
                let pos = cursor.position() as usize;
                let ret = SqlValue::NChar(parse_utf16_string(&cursor.get_ref()[pos..pos + size]));
                cursor.set_position((pos + size) as u64);
                ret
            }
            _ => panic!("cannot parse var length type using `parse`"),
        }
    }
}

pub trait ToSqlType {
    fn to_sql_type() -> SqlType;
}

pub trait FromSqlValue<'a> {
    fn from_sql_value(sql_value: SqlValue<'a>) -> Self;
}

macro_rules! impl_to_from_sql_for_literal {
    ($($literal:ty = $sql_type:ident),* $(,)?) => {
        $(
            impl ToSqlType for $literal {
                fn to_sql_type() -> SqlType {
                    SqlType::$sql_type
                }
            }

            impl<'a> FromSqlValue<'a> for $literal {
                fn from_sql_value(sql_value: SqlValue<'a>) -> Self {
                    match sql_value {
                        SqlValue::$sql_type(v) => v,
                        _ => unreachable!()
                    }
                }
            }
        )*
    }
}

impl_to_from_sql_for_literal!(i8 = TinyInt, i16 = SmallInt, i32 = Int, i64 = BigInt);

impl ToSqlType for ValueOrLob<&[u8]> {
    fn to_sql_type() -> SqlType {
        SqlType::VarBinary(None)
    }
}

impl<'a> FromSqlValue<'a> for ValueOrLob<&'a [u8]> {
    fn from_sql_value(sql_value: SqlValue<'a>) -> Self {
        match sql_value {
            SqlValue::VarBinary(v) => v,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub enum ValueOrLob<T> {
    Value(T),
    Lob(LobPointer),
}

impl<T> ValueOrLob<T> {
    pub fn map<V, FN: Fn(T) -> V>(self, fun: FN) -> ValueOrLob<V> {
        match self {
            Self::Value(v) => ValueOrLob::Value(fun(v)),
            Self::Lob(l) => ValueOrLob::Lob(l),
        }
    }
}

#[derive(Debug)]
pub enum SqlValue<'a> {
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Bit(bool),
    Binary(&'a [u8]),
    Char(&'a str),
    NChar(String),
    // always in a seperate database page
    NText(&'a [u8]),
    VarBinary(ValueOrLob<&'a [u8]>),
    VarChar(&'a [u8]),
    SysName(String),
    NVarChar(ValueOrLob<String>),
    SqlVariant(&'a [u8]),
    UniqueIdentifier(u128),
    DateTime(chrono::NaiveDateTime),
    SmallDateTime(chrono::NaiveDateTime),
    Image(Option<LobPointer>),
    Float(f64),
}

impl<'a> SqlValue<'a> {
    pub fn unwrap_unique_identifier(self) -> u128 {
        match self {
            Self::UniqueIdentifier(uuid) => uuid,
            _ => panic!("{:?} is not a unique identifier", self),
        }
    }

    pub fn unwrap_int(self) -> i32 {
        match self {
            Self::Int(i) => i,
            _ => panic!("{:?} is not a int", self),
        }
    }

    pub fn unwrap_nvar_char_in_row(self) -> String {
        match self {
            Self::NVarChar(ValueOrLob::Value(s)) => s,
            _ => panic!("{:?} is not a in row stored nvarchar", self),
        }
    }
}

pub fn value_for_display(this: &Option<SqlValue>) -> String {
    match this {
        Some(v) => match v {
            SqlValue::TinyInt(i) => format!("{}", i),
            SqlValue::SmallInt(i) => format!("{}", i),
            SqlValue::Int(i) => format!("{}", i),
            SqlValue::BigInt(i) => format!("{}", i),
            SqlValue::Bit(b) => format!("{}", b),
            SqlValue::Binary(bytes) | SqlValue::VarChar(bytes) => {
                format!("{:x?}", bytes)
            }
            SqlValue::VarBinary(b) => match b {
                ValueOrLob::Value(s) => format!("{:x?}", s),
                ValueOrLob::Lob(l) => format!("{:?}", l),
            },
            SqlValue::Char(s) => s.to_string(),
            SqlValue::NChar(s) => s.to_string(),
            SqlValue::SysName(s) => s.to_string(),
            SqlValue::NVarChar(s) => match s {
                ValueOrLob::Value(s) => s.to_string(),
                ValueOrLob::Lob(l) => format!("{:?}", l),
            },
            SqlValue::DateTime(d) | SqlValue::SmallDateTime(d) => format!("{}", d),
            SqlValue::SqlVariant(bytes) => format!("{:?}", bytes),
            SqlValue::UniqueIdentifier(uuid) => format!("{}", uuid),
            SqlValue::Image(bytes) => format!("{:?}", bytes),
            SqlValue::NText(bytes) => format!("{:?}", bytes),
            SqlValue::Float(f) => format!("{}", f),
        },
        None => "NULL".to_string(),
    }
}

#[derive(Debug)]
pub struct ColumnType {
    pub idx: i32,
    pub data_type: SqlType,
    pub name: String,
    pub nullable: bool,
    pub computed: bool,
}

#[derive(Debug)]
pub struct Schema {
    // Each column has a name and a type
    // the ordering of the columns is also significant, so we don't use a hashmap ore something like that
    pub columns: Vec<ColumnType>,
}

pub struct BitParser {
    current_byte: u8,
    read_bits: u8,
}

impl BitParser {
    fn new() -> Self {
        Self {
            current_byte: 0,
            read_bits: 8,
        }
    }

    fn read_bit(&mut self, cursor: &mut Cursor<&[u8]>) -> bool {
        if self.read_bits == 8 {
            self.current_byte = cursor.read_u8().unwrap();
            self.read_bits = 0;
        }

        let ret = (self.current_byte & 1) == 1;
        self.current_byte >>= 1;
        self.read_bits += 1;
        ret
    }
}

impl Schema {
    pub fn from_col_par<'a>(
        column_info: impl Iterator<Item = (&'a SysColPar, &'a SysScalarType)>,
    ) -> Self {
        let mut columns = column_info
            .map(|(col, ty)| {
                assert!(!col.status.contains(ColParStatus::SPARSE));
                assert!(!col.status.contains(ColParStatus::FILESTREAM));
                assert!(!col.status.contains(ColParStatus::XML_DOCUMENT));

                ColumnType {
                    idx: col.col_id,
                    data_type: SqlType::from_col(col, ty),
                    name: col.name.clone().unwrap(),
                    nullable: !col.status.contains(ColParStatus::NULLABLE),
                    computed: col.status.contains(ColParStatus::COMPUTED),
                }
            })
            .collect::<Vec<_>>();

        columns.sort_by(|a, b| a.idx.partial_cmp(&b.idx).unwrap());

        Self { columns }
    }

    // TODO(robin): we probably want to return something more like Option<Row>, because
    //              of forwarded / forwarding records and the like
    pub fn parse<'a>(&self, record: Record<'a>) -> Row<'a> {
        let mut values: Vec<_> = std::iter::repeat_with(|| None)
            .take(self.columns.len())
            .collect();
        let mut fixed_data_cursor = Cursor::new(record.fixed_data);
        let mut bit_parser = BitParser::new();
        let mut var_column_idx = 0;
        let mut null_bit_idx = 0;

        trace!("{:#?}, {:#?}", self, record);

        for (
            i,
            ColumnType {
                data_type,
                nullable,
                computed,
                name,
                ..
            },
        ) in self.columns.iter().enumerate()
        {
            trace!(
                "parsing column [{}] with data_type = {:?}, nullable = {}, name = {}",
                i,
                data_type,
                nullable,
                name
            );

            if *computed {
                trace!("column is computed, doing nothing for now");
                continue;
            }

            // nullable columns can be added after the fact
            if null_bit_idx >= record.column_count as usize {
                trace!("we are past the record.column_count, so we must be null");
                // assert!(nullable);
            } else if !record.is_column_null(null_bit_idx as u16) {
                trace!("the column is not null");
                if data_type.is_var_length() {
                    trace!("the column is var length");
                    match record.var_length_columns {
                        Some(ref columns) => {
                            trace!("the record has var length columns, so we parse it, current idx: {}, total: {}", var_column_idx, columns.count);
                            let (complex, data) = columns.get(var_column_idx);
                            values[i] = Some(data_type.parse_var_length(complex, data));
                            var_column_idx += 1;
                        }
                        None => {
                            trace!("the record does not have var length columns, so we parse a zero byte value");
                            // We are guessing with false here, lets hope it won't break
                            values[i] = Some(data_type.parse_var_length(false, &[]));
                        }
                    }
                } else {
                    trace!("the column is fixed length, we parse");
                    values[i] = Some(data_type.parse(&mut bit_parser, &mut fixed_data_cursor));
                }
            } else {
                trace!("the column is null");
            }

            null_bit_idx += 1;
            trace!("we got the value {:?}", values[i]);
        }

        Row { values }
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    // TODO(robin): Is there a better way to do nullability handling?
    //              maybe type level nullability?
    pub values: Vec<Option<SqlValue<'a>>>,
}

impl<'a> Row<'a> {
    pub fn format_row(&self) -> String {
        let mut res = "".to_owned();
        for value in &self.values {
            res += &format!("{:<16 }, ", value_for_display(value))
        }
        res
    }
}

// TODO(robin): use real columns idx's instead of dummy ones
#[macro_export]
macro_rules! create_row_parser {
    (struct $name:ident $(<$l:lifetime>)? { $($field_name:ident : $struct_ty:ty $([$optional:tt])? $(= [$input_ty:expr] $input_pat:pat => $conv_expr:expr)?),* $(,)? }) => {
        #[derive(Debug)]
        pub struct $name$(<$l>)? {
            $(pub $field_name: create_row_parser!(@actual_type $($optional,)? $struct_ty)),*
        }

        impl$(<$l>)? $name$(<$l>)? {
            pub fn schema() -> crate::Schema {
                #[allow(unused)]
                use crate::SqlType::*;

                crate::Schema {
                    columns: vec![$(create_row_parser!(@column_type $field_name, $($optional,)? $struct_ty $(as $input_ty)?),)*]
                }
            }

            #[allow(unused_assignments)]
            pub fn parse(record: crate::Record<$($l)?>) -> Self {
                let schema = $name::schema();
                let mut row = schema.parse(record);
                let mut idx = 0;
                $(
                    let $field_name = create_row_parser!(@unpack_column row.values[idx].take(), $($optional,)? $struct_ty $(= [$input_ty] $input_pat => $conv_expr)?);
                    idx += 1;
                )*

                Self {
                    $($field_name,)*
                }
            }
        }
    };
    (@actual_type ?, $struct_ty:ty) => {
        Option<$struct_ty>
    };
    (@actual_type $struct_ty:ty) => {
        $struct_ty
    };
    (@unpack_column $value:expr, ?, $struct_ty:ty = [$input_ty:expr] $input_pat:pat => $conv_expr:expr) => {
        {
            use crate::SqlValue::*;
            $value.map(|v| {
                match v {
                    $input_pat => $conv_expr,
                    _ => unreachable!()
                }
            })
        }
    };
    (@unpack_column $value:expr, ?, $struct_ty:ty) => {
        $value.map(<$struct_ty as crate::FromSqlValue>::from_sql_value)
    };
    (@unpack_column $value:expr, $struct_ty:ty = [$input_ty:expr] $input_pat:pat => $conv_expr:expr) => {
        {
            use crate::SqlValue::*;
            match $value.unwrap() {
                $input_pat => $conv_expr,
                _ => unreachable!()
            }
        }
    };
    (@unpack_column $value:expr, $struct_ty:ty) => {
        <$struct_ty as crate::FromSqlValue>::from_sql_value($value.unwrap())
    };
    (@column_type $name:ident, ?, $struct_ty:ty as $input_ty:expr) => {
        crate::ColumnType {
            idx: 0,
            computed: false,
            data_type: $input_ty,
            nullable: true,
            name: stringify!($name).to_string()
        }
    };
    (@column_type $name:ident, ?, $struct_ty:ty) => {
        crate::ColumnType {
            idx: 0,
            computed: false,
            data_type: <$struct_ty as crate::ToSqlType>::to_sql_type(),
            nullable: true,
            name: stringify!($name).to_string()
        }
    };
    (@column_type $name:ident, $struct_ty:ty) => {
        crate::ColumnType {
            idx: 0,
            computed: false,
            data_type: <$struct_ty as crate::ToSqlType>::to_sql_type(),
            nullable: false,
            name: stringify!($name).to_string()
        }
    };
    (@column_type $name:ident, $struct_ty:ty as $input_ty:expr) => {
        crate:: ColumnType {
            idx: 0,
            computed: false,
            data_type: $input_ty,
            nullable: false,
            name: stringify!($name).to_string()
        }
    };
}
