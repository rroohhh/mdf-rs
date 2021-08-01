use crate::{PagePointer, PageProvider, PageType, Row, Schema};
use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Table<'a, T> {
    pub name: String,
    #[derivative(Debug = "ignore")]
    pub page_provider: &'a T,
    pub schema: Schema,
    pub partition_pointer: Vec<PagePointer>,
}

impl<'a, T: PageProvider> Table<'a, T> {
    pub fn rows(&self) -> impl Iterator<Item = Row> {
        self.partition_pointer.iter().flat_map(move |part| {
            let start_page = self.page_provider.get(*part).unwrap();
            start_page
                .into_records()
                .map(move |rec| self.schema.parse(rec))
        })
    }

    // This is used to recover data from broken db's
    // instead of following the page links, this looks up the p_min_len from the
    // first page linked to from the allocation units and then scans the whole database
    // for tables with this p_min_len
    // For this to work the p_min_len has to be unique enough and the first page must be accessible
    pub fn scan_db(&'a self) -> impl Iterator<Item = Row> {
        let first_page = self.partition_pointer[0];
        let first_page = self.page_provider.get(first_page).unwrap();
        let p_min_len = first_page.header.p_min_len;

        self.page_provider
            .file_ids()
            .into_iter()
            .flat_map(move |j| {
                (0..self.page_provider.num_pages(j))
                    .filter_map(move |i| {
                        if let Some(page) = self.page_provider.get(PagePointer {
                            page_id: i,
                            file_id: j,
                        }) {
                            println!("{:?}", page.header);
                            if (page.header.p_min_len == p_min_len)
                                && (page.header.ty == PageType::Data)
                            {
                                println!("{} {}", j, i);
                                return Some(page);
                            }
                        }
                        None
                    })
                    .flat_map(move |page| {
                        page.local_records()
                            .map(move |record| self.schema.parse(record))
                    })
            })
    }

    pub fn scan_db_from(&'a self, start: PagePointer) -> impl Iterator<Item = Row> {
        let first_page = self.partition_pointer[0];
        let first_page = self.page_provider.get(first_page).unwrap();
        let p_min_len = first_page.header.p_min_len;
        let j = start.file_id;

        (start.page_id..self.page_provider.num_pages(j))
            .filter_map(move |i| {
                if let Some(page) = self.page_provider.get(PagePointer {
                    page_id: i,
                    file_id: j,
                }) {
                    if (page.header.p_min_len == p_min_len) && (page.header.ty == PageType::Data) {
                        return Some(page);
                    }
                }
                None
            })
            .flat_map(move |page| {
                page.local_records()
                    .map(move |record| self.schema.parse(record))
            })
    }
}
