use crate::pages::BootPage;
use crate::raw_page::{PagePointer, PageProvider};
use crate::{
    AllocUnitType, SchType, Schema, SysAllocUnit, SysColPar, SysRowSet, SysRsCol, SysScalarType,
    SysSchObj, SysSingleObjRef, Table, SYS_COL_PARS_IDMAJOR, SYS_ROW_SET_AUID,
    SYS_SCALAR_TYPES_IDMAJOR, SYS_SCH_OBJS_IDMAJOR, SYS_SINGLE_OBJECT_REFS_IDMAJOR,
};
use log::trace;

#[derive(Debug)]
pub struct DB<T> {
    pub page_provider: T,
    boot_page: BootPage,
    pub system_tables: SystemTables,
}

impl<T: PageProvider> DB<T> {
    pub fn new(page_provider: T) -> Self {
        // The location of the boot page is always the same
        let boot_page = BootPage::parse(
            page_provider
                .get(PagePointer {
                    file_id: 1,
                    page_id: 9,
                })
                .unwrap(),
        );

        let system_tables = SystemTables::parse(&page_provider, &boot_page);

        Self {
            page_provider,
            boot_page,
            system_tables,
        }
    }
    pub fn table(&self, name: &str) -> Option<Table<T>> {
        let tbl = self.system_tables.tables().find(|tbl| tbl.name == name);

        tbl.map(|tbl| Table {
            name: tbl.name.clone(),
            page_provider: &self.page_provider,
            schema: Schema::from_col_par(self.system_tables.columns_for_table(tbl).map(|col| {
                trace!("col = {:?}", col);
                (col, self.system_tables.type_for_column(col))
            })),
            partition_pointer: self
                .system_tables
                .partitions_for_table(tbl)
                .map(|part| {
                    self.system_tables
                        .allocation_unit_for_partition(part)
                        .pg_first
                })
                .filter(|pg| pg.is_some())
                .map(|pg| pg.unwrap())
                .collect(),
        })
    }

    pub fn tables(&self) -> impl Iterator<Item = Table<T>> {
        self.system_tables.tables().map(move |tbl| Table {
            name: tbl.name.clone(),
            page_provider: &self.page_provider,
            schema: Schema::from_col_par(
                self.system_tables
                    .columns_for_table(tbl)
                    .map(|col| (col, self.system_tables.type_for_column(col))),
            ),
            partition_pointer: self
                .system_tables
                .partitions_for_table(tbl)
                .map(|part| {
                    self.system_tables
                        .allocation_unit_for_partition(part)
                        .pg_first
                })
                .filter(|pg| pg.is_some())
                .map(|pg| pg.unwrap())
                .collect(),
        })
    }
}

#[derive(Debug)]
pub struct SystemTables {
    alloc_units: Vec<SysAllocUnit>,
    row_sets: Vec<SysRowSet>,
    sch_objs: Vec<SysSchObj>,
    col_pars: Vec<SysColPar>,
    scalar_types: Vec<SysScalarType>,
    rs_cols: Vec<SysRsCol>,
    single_object_refs: Vec<SysSingleObjRef>,
}

impl SystemTables {
    pub fn tables(&self) -> impl Iterator<Item = &SysSchObj> {
        self.sch_objs
            .iter()
            .filter(|obj| obj.ty == SchType::UserTable || obj.ty == SchType::SystemTable)
    }

    pub fn partitions_for_table<'a>(
        &'a self,
        table: &'a SysSchObj,
    ) -> impl Iterator<Item = &'a SysRowSet> {
        // TODO(robin): wtf ever does minor_id mean?
        self.row_sets
            .iter()
            .filter(move |row_set| row_set.id_major == table.id && row_set.id_minor <= 1)
    }

    pub fn columns_for_table<'a>(
        &'a self,
        table: &'a SysSchObj,
    ) -> impl Iterator<Item = &'a SysColPar> {
        self.col_pars.iter().filter(move |col| col.id == table.id)
    }

    pub fn type_for_column(&self, col: &SysColPar) -> &SysScalarType {
        self.scalar_types
            .iter()
            .find(|ty| ty.xtype == col.xtype && ty.id <= 255)
            .unwrap()
    }

    pub fn allocation_unit_for_partition(&self, partition: &SysRowSet) -> &SysAllocUnit {
        self.alloc_units
            .iter()
            .find(|au| au.owner_id == partition.row_set_id && au.ty == AllocUnitType::InRowData)
            .unwrap()
    }

    fn parse<T: PageProvider>(page_provider: &T, boot_page: &BootPage) -> Self {
        let alloc_units: Vec<_> = page_provider
            .get(boot_page.first_sys_indices)
            .unwrap()
            .into_records()
            .map(SysAllocUnit::parse)
            .collect();
        let row_sets: Vec<_> = page_provider
            .get(
                Self::find_alloc_unit_by_id(
                    &alloc_units[..],
                    SYS_ROW_SET_AUID,
                    AllocUnitType::InRowData,
                )
                .unwrap()
                .pg_first
                .unwrap(),
            )
            .unwrap()
            .into_records()
            .map(SysRowSet::parse)
            .collect();

        // TODO(robin): figure out what the id_minor stands for,
        let sch_objs = page_provider
            .get(
                Self::find_alloc_unit_by_rowset_ids(
                    &alloc_units,
                    &row_sets,
                    SYS_SCH_OBJS_IDMAJOR,
                    1,
                )
                .unwrap()
                .pg_first
                .unwrap(),
            )
            .unwrap()
            .records()
            .map(SysSchObj::parse)
            .collect();

        let col_pars = page_provider
            .get(
                Self::find_alloc_unit_by_rowset_ids(
                    &alloc_units,
                    &row_sets,
                    SYS_COL_PARS_IDMAJOR,
                    1,
                )
                .unwrap()
                .pg_first
                .unwrap(),
            )
            .unwrap()
            .records()
            .map(SysColPar::parse)
            .collect();

        let scalar_types = page_provider
            .get(
                Self::find_alloc_unit_by_rowset_ids(
                    &alloc_units,
                    &row_sets,
                    SYS_SCALAR_TYPES_IDMAJOR,
                    1,
                )
                .unwrap()
                .pg_first
                .unwrap(),
            )
            .unwrap()
            .records()
            .map(SysScalarType::parse)
            .collect();

        /*
        let rs_cols = page_provider.get(
            Self::find_alloc_unit_by_rowset_ids(
                &alloc_units, &row_sets, SYS_RS_COLS_IDMAJOR, 1
            ).unwrap().pg_first.unwrap()
        ).records().take(530).map(SysRsCol::parse).collect();
        */

        let single_object_refs = page_provider
            .get(
                Self::find_alloc_unit_by_rowset_ids(
                    &alloc_units,
                    &row_sets,
                    SYS_SINGLE_OBJECT_REFS_IDMAJOR,
                    1,
                )
                .unwrap()
                .pg_first
                .unwrap(),
            )
            .unwrap()
            .records()
            .map(SysSingleObjRef::parse)
            .collect();

        Self {
            alloc_units,
            row_sets,
            sch_objs,
            col_pars,
            scalar_types,
            rs_cols: vec![],
            single_object_refs,
        }
    }

    fn find_alloc_unit_by_id(
        alloc_units: &[SysAllocUnit],
        au_id: i64,
        ty: AllocUnitType,
    ) -> Option<&SysAllocUnit> {
        alloc_units
            .iter()
            .find(move |alloc_unit| alloc_unit.au_id == au_id && alloc_unit.ty == ty)
    }

    fn find_alloc_unit_by_rowset_ids<'au>(
        alloc_units: &'au [SysAllocUnit],
        row_sets: &[SysRowSet],
        id_major: i32,
        id_minor: i32,
    ) -> Option<&'au SysAllocUnit> {
        row_sets
            .iter()
            .find(move |row_set| row_set.id_major == id_major && row_set.id_minor == id_minor)
            .and_then(|row_set| {
                Self::find_alloc_unit_by_id(
                    alloc_units,
                    row_set.row_set_id,
                    AllocUnitType::InRowData,
                )
            })
    }
}
